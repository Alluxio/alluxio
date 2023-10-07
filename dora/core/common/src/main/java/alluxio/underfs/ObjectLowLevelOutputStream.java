/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs;

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * [Experimental] A stream for writing a file into object storage using streaming upload.
 * The data transfer is done using object storage low-level multipart upload.
 * <p>
 * We upload data in partitions. When write(), the data will be persisted to
 * a temporary file {@link #mFile} on the local disk. When the data {@link #mPartitionOffset}
 * in this temporary file reaches the {@link #mPartitionSize}, the file will be submitted
 * to the {@link MultipartUploader} and we do not wait for uploads to finish.
 * A new temp file will be created for the future write and the {@link #mPartitionOffset}
 * will be reset to zero. The process goes until all the data has been written to temp files.
 * <p>
 * In flush(), we upload the buffered data if they are bigger than 5MB
 * and wait for all uploads to finish. The temp files will be deleted after uploading successfully.
 * <p>
 * In close(), we upload the last part of data (if exists), wait for all uploads to finish,
 * and complete the multipart upload.
 * <p>
 * close() will not be retried, but all the multipart upload
 * related operations(init, upload, complete, and abort) will be retried.
 * <p>
 * If an error occurs and we have no way to recover, we abort the multipart uploads.
 * Some multipart uploads may not be completed/aborted in normal ways and need periodical cleanup
 * by enabling the {@link PropertyKey#UNDERFS_CLEANUP_ENABLED}.
 * When a leader master starts or a cleanup interval is reached, all the multipart uploads
 * older than clean age will be cleaned.
 */
@NotThreadSafe
public class ObjectLowLevelOutputStream extends OutputStream
    implements ContentHashable {
  protected static final Logger LOG = LoggerFactory.getLogger(ObjectLowLevelOutputStream.class);

  protected final List<String> mTmpDirs;

  /**
   * Only parts bigger than 5MB could be uploaded through multipart upload,
   * except the last part.
   */
  protected static final long UPLOAD_THRESHOLD = 5L * Constants.MB;

  /** Bucket name of the object storage bucket. */
  protected final String mBucketName;

  /** Key of the file when it is uploaded to object storage. */
  protected final String mKey;

  /** The retry policy of this multipart upload. */
  protected final Supplier<RetryPolicy> mRetryPolicy = () -> new CountingRetry(5);

  /** Pre-allocated byte buffer for writing single characters. */
  protected final byte[] mSingleCharWrite = new byte[1];

  /** Flag to indicate this stream has been closed, to ensure close is only done once. */
  protected boolean mClosed = false;

  /** When the offset reaches the partition size, we upload the temp file. */
  protected long mPartitionOffset;
  /** The maximum allowed size of a partition. */
  protected final long mPartitionSize;

  /**
   * The local temp file that will be uploaded when reaches the partition size
   * or when flush() is called and this file is bigger than {@link #UPLOAD_THRESHOLD}.
   */
  @Nullable
  protected File mFile;
  /** The output stream to the local temp file. */
  @Nullable
  protected OutputStream mLocalOutputStream;

  /**
   * Give each upload request a unique and continuous id
   * so that object storage knows the part sequence to concatenate the parts to a single object.
   */
  private final AtomicInteger mPartNumber;

  private final ObjectMultipartUploader mMultipartUploader;

  /**
   * Constructs a new stream for writing a file.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param multipartUploader the uploader to complete the multipart upload task
   * @param ufsConf the object store under file system configuration
   */
  public ObjectLowLevelOutputStream(
      String bucketName,
      String key,
      ObjectMultipartUploader multipartUploader,
      AlluxioConfiguration ufsConf) {
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(),
        "Bucket name must not be null or empty.");
    mBucketName = bucketName;
    mTmpDirs = ufsConf.getList(PropertyKey.TMP_DIRS);
    Preconditions.checkArgument(!mTmpDirs.isEmpty(), "No temporary directories available");
    mKey = key;
    long streamingUploadPartitionSize =
        ufsConf.getBytes(PropertyKey.UNDERFS_S3_STREAMING_UPLOAD_PARTITION_SIZE);
    mPartitionSize = Math.max(UPLOAD_THRESHOLD, streamingUploadPartitionSize);
    mPartNumber = new AtomicInteger(1);
    mMultipartUploader = multipartUploader;
    try {
      mMultipartUploader.startUpload();
    } catch (IOException e) {
      throw AlluxioRuntimeException.from(e);
    }
  }

  @Override
  public void write(int b) throws IOException {
    mSingleCharWrite[0] = (byte) b;
    write(mSingleCharWrite);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null || len == 0) {
      return;
    }
    Preconditions.checkNotNull(b);
    Preconditions.checkArgument(off >= 0 && off <= b.length && len >= 0 && off + len <= b.length);
    if (mFile == null) {
      initNewFile();
    }
    if (mPartitionOffset + len <= mPartitionSize) {
      mLocalOutputStream.write(b, off, len);
      mPartitionOffset += len;
    } else {
      int firstLen = (int) (mPartitionSize - mPartitionOffset);
      mLocalOutputStream.write(b, off, firstLen);
      mPartitionOffset += firstLen;
      uploadPart();
      write(b, off + firstLen, len - firstLen);
    }
  }

  @Override
  public void flush() throws IOException {
    // We try to minimize the time use to close()
    // because Fuse release() method which calls close() is async.
    // In flush(), we upload the current writing file if it is bigger than 5 MB,
    // and wait for all current upload to complete.
    if (mLocalOutputStream != null) {
      mLocalOutputStream.flush();
    }
    if (mPartitionOffset > UPLOAD_THRESHOLD) {
      uploadPart();
    }
    waitForAllPartsUpload();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }

    // Set the closed flag, we never retry close() even if exception occurs
    mClosed = true;

    try {

      if (mFile == null) {
        LOG.debug("Streaming upload output stream closed without uploading any data.");
        RetryUtils.retry("put empty object for key" + mKey, mMultipartUploader::complete,
            mRetryPolicy.get());
        return;
      } else {
        mLocalOutputStream.close();
        int partNumber = mPartNumber.getAndIncrement();
        File partFile = new File(mFile.getPath());
        InputStream in = new BufferedInputStream(Files.newInputStream(partFile.toPath()));
        mMultipartUploader.putPart(in, partNumber, partFile.length());
      }

      waitForAllPartsUpload();
      RetryUtils.retry("complete multipart upload", mMultipartUploader::complete,
          mRetryPolicy.get());
    } catch (Exception e) {
      LOG.error("Failed to upload {}", mKey, e);
      throw new IOException(e);
    }
  }

  /**
   * Creates a new temp file to write to.
   */
  private void initNewFile() throws IOException {
    mFile = new File(PathUtils.concatPath(CommonUtils.getTmpDir(mTmpDirs), UUID.randomUUID()));
    mLocalOutputStream = new BufferedOutputStream(new FileOutputStream(mFile));
    mPartitionOffset = 0;
    LOG.debug("Init new temp file @ {}", mFile.getPath());
  }

  /**
   * Uploads part async.
   */
  protected void uploadPart() throws IOException {
    if (mFile == null) {
      return;
    }
    mLocalOutputStream.close();
    int partNumber = mPartNumber.getAndIncrement();
    File partFile = new File(mFile.getPath());
    InputStream in = new BufferedInputStream(Files.newInputStream(partFile.toPath()));
    mMultipartUploader.putPart(in, partNumber, partFile.length());
    mFile = null;
    mLocalOutputStream = null;
  }

  protected void abortMultiPartUpload() {
    try {
      RetryUtils.retry("abort multipart upload for key " + mKey, () -> mMultipartUploader.abort(),
          mRetryPolicy.get());
    } catch (IOException e) {
      LOG.warn("Unable to abort multipart upload for key '{}' and id '{}' to bucket {}. "
              + "You may need to enable the periodical cleanup by setting property {}"
              + "to be true.", mKey, mBucketName,
          PropertyKey.UNDERFS_CLEANUP_ENABLED.getName(), e);
    }
  }

  protected void waitForAllPartsUpload() throws IOException {
    mMultipartUploader.flush();
  }

  /**
   * Get the part number.
   * @return the part number
   */
  @VisibleForTesting
  public int getPartNumber() {
    return mPartNumber.get();
  }

  @Override
  public Optional<String> getContentHash() throws IOException {
    return Optional.ofNullable(mMultipartUploader.getContentHash());
  }
}
