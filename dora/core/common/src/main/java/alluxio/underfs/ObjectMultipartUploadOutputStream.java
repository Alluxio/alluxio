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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Upload File to object storage in multiple parts.
 * <p>
 * The file is partitioned into multiple parts, each part is uploaded in a separate thread.
 * The number of threads is determined by the configuration.
 * The minimum part size is 5MB (s3) or 100KB (oss, obs) or 1MB(cos), except the last part.
 * We choose 5MB to be the minimum part size for all object storage systems.
 * The maximum part size is 5GB (s3, oss, cos, obs).
 * The partition size is determined by the configuration.
 * <p>
 * In flush(), we wait for all uploads to finish.
 * <p>
 * In close() we complete the multipart upload.
 * <p>
 * close() will not be retried, but all the multipart upload
 * related operations(init, upload, complete, and abort) will be retried.
 * <p>
 * If an error occurs, and we have no way to recover, we abort the multipart uploads.
 * Some multipart uploads may not be completed/aborted in normal ways and need periodical cleanup
 * by enabling the {@link PropertyKey#UNDERFS_CLEANUP_ENABLED}.
 * When a leader master starts or a cleanup interval is reached, all the multipart uploads
 * older than clean age will be cleaned.
 */
@NotThreadSafe
public abstract class ObjectMultipartUploadOutputStream extends OutputStream
    implements ContentHashable {
  protected static final Logger LOG =
      LoggerFactory.getLogger(ObjectMultipartUploadOutputStream.class);
  /**
   * Only parts bigger than 5MB could be uploaded through multipart upload,
   * except the last part.
   */
  protected static final long MINIMUM_PART_SIZE = 5L * Constants.MB;

  /**
   * The maximum size of a single part is 5GB.
   */
  protected static final long MAXIMUM_PART_SIZE = 5L * Constants.GB;

  /**
   * Bucket name of the object storage bucket.
   */
  protected final String mBucketName;
  /**
   * Key of the file when it is uploaded to object storage.
   */
  protected final String mKey;
  /**
   * The retry policy of this multipart upload.
   */
  protected final Supplier<RetryPolicy> mRetryPolicy = () -> new CountingRetry(5);
  /**
   * Pre-allocated byte buffer for writing single characters.
   */
  protected final byte[] mSingleCharWrite = new byte[1];
  /**
   * The maximum allowed size of a partition.
   */
  protected final long mPartitionSize;
  /**
   * Give each upload request a unique and continuous id
   * so that object storage knows the part sequence to concatenate the parts to a single object.
   */
  private final AtomicInteger mPartNumber;
  /**
   * Executing the upload tasks.
   */
  private final ListeningExecutorService mExecutor;
  /**
   * Store the future of tags.
   */
  private final List<ListenableFuture<?>> mFutures = new ArrayList<>();
  /**
   * Flag to indicate this stream has been closed, to ensure close is only done once.
   */
  protected boolean mClosed = false;
  /**
   * When the offset reaches the partition size, we upload mStream.
   */
  protected long mPartitionOffset;
  /**
   * upload part timeout, null means no timeout.
   */
  @Nullable
  private Long mUploadPartTimeoutMills;
  /**
   * Whether the multi upload has been initialized.
   */
  private boolean mMultiPartUploadInitialized = false;

  @Nullable
  private byte[] mUploadPartArray;

  /**
   * Constructs a new stream for writing a file.
   *
   * @param bucketName                   the name of the bucket
   * @param key                          the key of the file
   * @param executor                     executor
   * @param multipartUploadPartitionSize the size in bytes for partitions of multipart uploads
   * @param ufsConf                      the object store under file system configuration
   */
  public ObjectMultipartUploadOutputStream(
      String bucketName,
      String key,
      ListeningExecutorService executor,
      long multipartUploadPartitionSize,
      AlluxioConfiguration ufsConf) {
    Preconditions.checkArgument(bucketName != null && !bucketName.isEmpty(),
        "Bucket name must not be null or empty.");
    mBucketName = bucketName;
    mExecutor = executor;
    mKey = key;
    mPartNumber = new AtomicInteger(1);
    mPartitionSize = Math.min(Math.max(
        MINIMUM_PART_SIZE, multipartUploadPartitionSize), MAXIMUM_PART_SIZE);
    if (ufsConf.isSet(PropertyKey.UNDERFS_OBJECT_STORE_MULTIPART_UPLOAD_TIMEOUT)) {
      mUploadPartTimeoutMills =
          ufsConf.getDuration(PropertyKey.UNDERFS_OBJECT_STORE_MULTIPART_UPLOAD_TIMEOUT)
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
    Preconditions.checkArgument(off >= 0 && off <= b.length
        && len >= 0 && off + len <= b.length);

    if (mUploadPartArray == null) {
      initNewUploadPartArray();
    }
    // If the current partition is not full, we write the data to the current partition.
    if (mPartitionOffset + len <= mPartitionSize) {
      // Since the original b array will be overwritten in other functions
      // We can't just keep the reference of b array, but should keep a copy of b array.
      System.arraycopy(b, off, mUploadPartArray, (int) mPartitionOffset, len);
      mPartitionOffset += len;
    } else {
      // If the current partition cannot write all the data,
      // write the excess data to the next partition after filling the current partition
      int firstLen = (int) (mPartitionSize - mPartitionOffset);

      // As described before, we keep a copy of b array.
      System.arraycopy(b, off, mUploadPartArray, (int) mPartitionOffset, firstLen);
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
    if (mPartitionOffset > MINIMUM_PART_SIZE) {
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

    // Multipart upload has not been initialized, use putObject to upload the file.
    if (!mMultiPartUploadInitialized) {
      if (mUploadPartArray == null) {
        LOG.debug("Multipart upload output stream closed without uploading any data.");
        RetryUtils.retry("put empty object for key" + mKey, () -> createEmptyObject(mKey),
            mRetryPolicy.get());
      } else {
        byte[] buf = mUploadPartArray;
        mUploadPartArray = null;
        try {
          RetryUtils.retry("put object for key" + mKey,
              () -> putObject(mKey, buf, mPartitionOffset), mRetryPolicy.get());
        } catch (Exception e) {
          LOG.error("Failed to upload {}", mKey, e);
          throw new IOException(e);
        }
      }
      return;
    }

    // Multipart upload has been initialized, upload the last part and complete the multipart.
    try {
      if (mUploadPartArray != null) {
        int partNumber = mPartNumber.getAndIncrement();
        byte[] buf = mUploadPartArray;
        uploadPart(buf, partNumber, true, mPartitionOffset);
        mUploadPartArray = null;
      }

      // Wait for all parts to be uploaded.
      waitForAllPartsUpload();
      RetryUtils.retry("complete multipart upload",
          this::completeMultipartUploadInternal, mRetryPolicy.get());
    } catch (Exception e) {
      LOG.error("Failed to upload {}", mKey, e);
      throw new IOException(e);
    }
  }

  /**
   * Creates a new temp file to write to.
   */
  private void initNewUploadPartArray() {
    mUploadPartArray = new byte[(int) mPartitionSize];
    mPartitionOffset = 0;
    LOG.debug("Init new mUploadPartArray @ {}", mUploadPartArray);
  }

  /**
   * Uploads part async.
   */
  protected void uploadPart() throws IOException {
    if (mUploadPartArray == null) {
      return;
    }

    if (!mMultiPartUploadInitialized) {
      RetryUtils.retry("init multipart upload", this::initMultipartUploadInternal,
          mRetryPolicy.get());
      mMultiPartUploadInitialized = true;
    }

    int partNumber = mPartNumber.getAndIncrement();
    byte[] buf = mUploadPartArray;
    uploadPart(buf, partNumber, false, mPartitionOffset);
    mUploadPartArray = null;
  }

  protected void uploadPart(byte[] buf, int partNumber,
                            boolean isLastPart, long length) throws IOException {
    Callable<?> callable = () -> {
      try {
        RetryUtils.retry("upload part for key " + mKey + " and part number " + partNumber,
            () -> uploadPartInternal(buf, partNumber, isLastPart, length), mRetryPolicy.get());
        return null;
      } catch (Exception e) {
        LOG.error("Failed to upload part {} for key {}", partNumber, mKey, e);
        throw new IOException(e);
      }
    };
    ListenableFuture<?> futureTag = mExecutor.submit(callable);
    mFutures.add(futureTag);
  }

  protected void abortMultiPartUpload() throws IOException {
    RetryUtils.retry("abort multipart upload for key " + mKey, this::abortMultipartUploadInternal,
        mRetryPolicy.get());
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
   * @param buf              the byte buf
   * @param partNumber       the part number
   * @param isLastPart       whether this is the last part
   * @param length           the length of the part to be uploaded
   * @throws IOException
   */
  protected abstract void uploadPartInternal(
      byte[] buf,
      int partNumber,
      boolean isLastPart,
      long length)
      throws IOException;

  /**
   * @throws IOException
   */
  protected abstract void initMultipartUploadInternal() throws IOException;

  /**
   * @throws IOException
   */
  protected abstract void completeMultipartUploadInternal() throws IOException;

  /**
   * @throws IOException
   */
  protected abstract void abortMultipartUploadInternal() throws IOException;

  /**
   * @param key the key
   * @throws IOException
   */
  protected abstract void createEmptyObject(String key) throws IOException;

  /**
   * @param key     the key
   * @param buf     the byte buf
   * @param length  the length of the file to be uploaded
   * @throws IOException
   */
  protected abstract void putObject(String key, byte[] buf, long length) throws IOException;
}
