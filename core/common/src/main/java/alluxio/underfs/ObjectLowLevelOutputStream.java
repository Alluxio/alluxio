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
import alluxio.retry.CountingRetry;
import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * [Experimental] A stream for writing a file into object storage using streaming upload.
 * The data transfer is done using object storage low-level multipart upload.
 * <p>
 * We upload data in partitions. When write(), the data will be persisted to
 * a temporary file {@link #mFile} on the local disk. When the data {@link #mPartitionOffset}
 * in this temporary file reaches the {@link #mPartitionSize}, the file will be submitted
 * to the upload executor {@link #mExecutor} and we do not wait for uploads to finish.
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
public abstract class ObjectLowLevelOutputStream extends OutputStream {
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
  protected final RetryPolicy mRetryPolicy = new CountingRetry(5);

  /** Pre-allocated byte buffer for writing single characters. */
  protected final byte[] mSingleCharWrite = new byte[1];

  /** The MD5 hash of the file. */
  protected MessageDigest mHash;

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
  protected File mFile;
  /** The output stream to the local temp file. */
  protected OutputStream mLocalOutputStream;

  /**
   * Give each upload request a unique and continuous id
   * so that object storage knows the part sequence to concatenate the parts to a single object.
   */
  private final AtomicInteger mPartNumber;

  /** Executing the upload tasks. */
  private final ListeningExecutorService mExecutor;

  /** Store the future of tags. */
  private final List<ListenableFuture<?>> mFutures = new ArrayList<>();

  /** upload part timeout, null means no timeout. */
  @Nullable
  private Long mUploadPartTimeoutMills;

  /** Whether the multi upload has been initialized. */
  private boolean mMultiPartUploadInitialized = false;

  /**
   * Constructs a new stream for writing a file.
   *
   * @param bucketName the name of the bucket
   * @param key the key of the file
   * @param streamingUploadPartitionSize the size in bytes for partitions of streaming uploads
   * @param executor executor
   * @param ufsConf the object store under file system configuration
   */
  public ObjectLowLevelOutputStream(
      String bucketName,
      String key,
      ListeningExecutorService executor,
      long streamingUploadPartitionSize,
      AlluxioConfiguration ufsConf) {
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(),
        "Bucket name must not be null or empty.");
    mBucketName = bucketName;
    mTmpDirs = ufsConf.getList(PropertyKey.TMP_DIRS);
    Preconditions.checkArgument(!mTmpDirs.isEmpty(), "No temporary directories available");
    mExecutor = executor;
    mKey = key;
    initHash();
    mPartitionSize = Math.max(UPLOAD_THRESHOLD, streamingUploadPartitionSize);
    mPartNumber = new AtomicInteger(1);
    if (ufsConf.isSet(PropertyKey.UNDERFS_OBJECT_STORE_STREAMING_UPLOAD_PART_TIMEOUT)) {
      mUploadPartTimeoutMills =
          ufsConf.getDuration(PropertyKey.UNDERFS_OBJECT_STORE_STREAMING_UPLOAD_PART_TIMEOUT)
              .toMillis();
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
    if (!mMultiPartUploadInitialized) {
      return;
    }
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

    // Multi-part upload has not been initialized
    if (!mMultiPartUploadInitialized) {
      if (mFile == null) {
        LOG.debug("Streaming upload output stream closed without uploading any data.");
        RetryUtils.retry("put empty object for key" + mKey, () -> createEmptyObject(mKey),
            mRetryPolicy);
      } else {
        try {
          mLocalOutputStream.close();
          final String md5 = mHash != null ? Base64.encodeBase64String(mHash.digest()) : null;
          RetryUtils.retry("put object for key" + mKey, () -> putObject(mKey, mFile, md5),
              mRetryPolicy);
        } finally {
          if (!mFile.delete()) {
            LOG.error("Failed to delete temporary file @ {}", mFile.getPath());
          }
        }
      }
      return;
    }

    try {
      if (mFile != null) {
        mLocalOutputStream.close();
        int partNumber = mPartNumber.getAndIncrement();
        uploadPart(mFile, partNumber, true);
      }

      waitForAllPartsUpload();
      RetryUtils.retry("complete multipart upload",
          this::completeMultiPartUploadInternal, mRetryPolicy);
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
    initHash();
    if (mHash != null) {
      mLocalOutputStream =
          new BufferedOutputStream(new DigestOutputStream(new FileOutputStream(mFile), mHash));
    } else {
      mLocalOutputStream = new BufferedOutputStream(new FileOutputStream(mFile));
    }
    mPartitionOffset = 0;
    LOG.debug("Init new temp file @ {}", mFile.getPath());
  }

  private void initHash() {
    try {
      mHash = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      LOG.warn("Algorithm not available for MD5 hash.", e);
      mHash = null;
    }
  }

  /**
   * Uploads part async.
   */
  protected void uploadPart() throws IOException {
    if (mFile == null) {
      return;
    }
    if (!mMultiPartUploadInitialized) {
      RetryUtils.retry("init multipart upload", this::initMultiPartUploadInternal, mRetryPolicy);
      mMultiPartUploadInitialized = true;
    }
    mLocalOutputStream.close();
    int partNumber = mPartNumber.getAndIncrement();
    uploadPart(new File(mFile.getPath()), partNumber, false);
    mFile = null;
    mLocalOutputStream = null;
  }

  protected void uploadPart(File file, int partNumber, boolean lastPart) {
    final String md5 = mHash != null ? Base64.encodeBase64String(mHash.digest()) : null;
    Callable<?> callable = () -> {
      try {
        RetryUtils.retry("upload part for key " + mKey + " and part number " + partNumber,
            () -> uploadPartInternal(file, partNumber, lastPart, md5), mRetryPolicy);
        return null;
      } finally {
        // Delete the uploaded or failed to upload file
        if (!file.delete()) {
          LOG.error("Failed to delete temporary file @ {}", file.getPath());
        }
      }
    };
    ListenableFuture<?> futureTag = mExecutor.submit(callable);
    mFutures.add(futureTag);
    LOG.debug(
        "Submit upload part request. key={}, partNum={}, file={}, fileSize={}, lastPart={}.",
        mKey, partNumber, file.getPath(), file.length(), lastPart);
  }

  protected void abortMultiPartUpload() throws IOException {
    RetryUtils.retry("abort multipart upload for key " + mKey, this::abortMultiPartUploadInternal,
        mRetryPolicy);
  }

  protected void waitForAllPartsUpload() throws IOException {
    try {
      for (ListenableFuture<?> future : mFutures) {
        if (mUploadPartTimeoutMills == null) {
          future.get();
        } else {
          future.get(mUploadPartTimeoutMills, TimeUnit.MILLISECONDS);
        }
      }
    } catch (ExecutionException e) {
      // No recover ways so that we need to cancel all the upload tasks
      // and abort the multipart upload
      Futures.allAsList(mFutures).cancel(true);
      abortMultiPartUpload();
      throw new IOException(
          "Part upload failed in multipart upload with to " + mKey, e);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted object upload.", e);
      Futures.allAsList(mFutures).cancel(true);
      abortMultiPartUpload();
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      LOG.error("timeout when upload part");
      Futures.allAsList(mFutures).cancel(true);
      abortMultiPartUpload();
      throw new IOException("timeout when upload part " + mKey, e);
    }
    mFutures.clear();
  }

  /**
   * Get the part number.
   * @return the part number
   */
  @VisibleForTesting
  public int getPartNumber() {
    return mPartNumber.get();
  }

  protected abstract void uploadPartInternal(
      File file,
      int partNumber,
      boolean isLastPart,
      @Nullable String md5)
      throws IOException;

  protected abstract void initMultiPartUploadInternal() throws IOException;

  protected abstract void completeMultiPartUploadInternal() throws IOException;

  protected abstract void abortMultiPartUploadInternal() throws IOException;

  protected abstract void createEmptyObject(String key) throws IOException;

  protected abstract void putObject(String key, File file, String md5) throws IOException;
}
