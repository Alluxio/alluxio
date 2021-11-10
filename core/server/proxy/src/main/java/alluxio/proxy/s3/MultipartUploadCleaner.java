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

package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.DeletePOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A lazy method (not scan the whole fileSystem to find tmp directory) to
 * support automatic abortion s3 multipartUpload after a timeout.
 */
public class MultipartUploadCleaner {
  private static final Logger LOG = LoggerFactory.getLogger(MultipartUploadCleaner.class);

  private final long mTimeout;
  private final int mRetry;
  private final long mRetryDelay;
  private final FileSystem mFs;
  private final ScheduledThreadPoolExecutor mExecutor;
  private final ConcurrentHashMap<AbortTask, ScheduledFuture<?>> mTasks =
      new ConcurrentHashMap<>();

  /**
   * Creates a new instance of {@link MultipartUploadCleaner}.
   *
   * @param fs the {@link FileSystem} instance
   */
  public MultipartUploadCleaner(FileSystem fs) {
    mTimeout =
        ServerConfiguration.getMs(PropertyKey.PROXY_S3_MULTIPART_UPLOAD_TIMEOUT);
    mRetry =
        ServerConfiguration.getInt(PropertyKey.PROXY_S3_MULTIPART_UPLOAD_CLEANER_RETRY_COUNT);
    mRetryDelay =
        ServerConfiguration.getMs(PropertyKey.PROXY_S3_MULTIPART_UPLOAD_CLEANER_RETRY_DELAY);
    mExecutor = new ScheduledThreadPoolExecutor(
        ServerConfiguration.getInt(PropertyKey.PROXY_S3_MULTIPART_UPLOAD_CLEANER_POOL_SIZE));

    mFs = fs;
  }

  /**
   * Schedule a task to clean multipart upload.
   *
   * @param bucket bucket name
   * @param object object name
   * @return true if add a abort task
   */
  public boolean apply(String bucket, String object)
      throws IOException, AlluxioException {
    // Use schedule pool do everyThing.
    long delay = tryAbortMultipartUpload(bucket, object, null);
    if (delay > 0) {
      Long uploadId = getMultipartUploadId(bucket, object);
      if (uploadId == null) {
        LOG.warn("Can not add abort task, because of object {} in bucket {} does not exist.",
            object, bucket);
        return false;
      }
      return apply(new AbortTask(bucket, object, uploadId), 0);
    }
    return false;
  }

  /**
   * Schedule a task to clean multipart upload.
   *
   * @param bucket bucket name
   * @param object object name
   * @param uploadId multipart upload tmp directory fileId
   * @return true if add a abort task
   */
  public boolean apply(String bucket, String object, Long uploadId)
      throws IOException, AlluxioException {
    // Use schedule pool do everyThing.
    if (uploadId == null) {
      uploadId = getMultipartUploadId(bucket, object);
      if (uploadId == null) {
        LOG.warn("Can not add abort task, because of object {} in bucket {} does not exist.",
            object, bucket);
        return false;
      }
    }
    return apply(new AbortTask(bucket, object, uploadId), 0);
  }

  /**
   * Schedule a task to clean multipart upload.
   *
   * @param task abort multipart upload task
   * @param delay delay time
   * @return true if add a abort task
   */
  private boolean apply(AbortTask task, long delay) {
    final ScheduledFuture<?> f = mExecutor.schedule(task, delay, TimeUnit.MILLISECONDS);
    mTasks.put(task, f);
    return true;
  }

  /**
   * Cancel schedule task.
   *
   * @param bucket bucket name
   * @param object object name
   * @param uploadId multipart upload tmp directory fileId
   */
  public void cancelAbort(String bucket, String object, Long uploadId) {
    AbortTask task = new AbortTask(bucket, object, uploadId);
    if (mTasks.containsKey(task)) {
      ScheduledFuture<?> f = mTasks.remove(task);
      if (f != null) {
        f.cancel(false);
      }
    }
  }

  /**
   * Remove finished task.
   */
  private void removeTaskRecord(AbortTask task) {
    mTasks.remove(task);
  }

  /**
   * Detect if abort task can retry.
   *
   * @param task abort task
   * @return if task can retry
   */
  private boolean canRetry(AbortTask task) {
    return task.mRetryCount < mRetry;
  }

  /**
   * Try to abort a multipart upload if it was timeout.
   *
   * @param bucket the bucket name
   * @param object the object name
   * @param uploadId multipart upload tmp directory fileId
   * @return delay time
   */
  public long tryAbortMultipartUpload(String bucket, String object, Long uploadId)
      throws IOException, AlluxioException {
    long delay = 0;
    final String bucketPath = AlluxioURI.SEPARATOR  + bucket;
    final String multipartTemporaryDir =
        S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object);
    final String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
    AlluxioURI tmpUri = new AlluxioURI(multipartTemporaryDir);
    try {
      URIStatus status = mFs.getStatus(tmpUri);
      if ((uploadId == null || uploadId == status.getFileId()) && status.isFolder()) {
        final long curTime = System.currentTimeMillis();
        long lastModificationTimeMs = status.getLastModificationTimeMs();
        delay = lastModificationTimeMs + mTimeout - curTime;
        if (delay <= 0) {
          // check object, when merge multipart upload, it may be timeout
          try {
            AlluxioURI uri = new AlluxioURI(objectPath);
            status = mFs.getStatus(uri);
            lastModificationTimeMs = status.getLastModificationTimeMs();
            delay = lastModificationTimeMs + mTimeout - curTime;
            if (delay <= 0) {
              mFs.delete(tmpUri, DeletePOptions.newBuilder().setRecursive(true).build());
              LOG.info("Abort multipart upload {} in bucket {} with uploadId {}.",
                  object, bucket, uploadId);
            }
          } catch (FileDoesNotExistException e) {
            mFs.delete(tmpUri, DeletePOptions.newBuilder().setRecursive(true).build());
            LOG.info("Abort multipart upload {} in bucket {} with uploadId {}.",
                object, bucket, uploadId);
          }
        }
      }
    } catch (FileDoesNotExistException ignored) {
      return delay;
    }
    return delay;
  }

  /**
   * Get multipart uploadId.
   *
   * @param bucket the bucket name
   * @param object the object name
   */
  @Nullable
  private Long getMultipartUploadId(String bucket, String object)
      throws IOException, AlluxioException {
    final String bucketPath = AlluxioURI.SEPARATOR  + bucket;
    String multipartTemporaryDir =
        S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object);
    try {
      URIStatus status = mFs.getStatus(new AlluxioURI(multipartTemporaryDir));
      return status.getFileId();
    } catch (FileDoesNotExistException e) {
      return null;
    }
  }

  /**
   * Abort Multipart upload task.
   */
  public class AbortTask implements Runnable {
    private final String mBucket;
    private final String mObject;
    private final Long mUploadId;
    private int mRetryCount;

    /**
     * Creates a new instance of {@link AbortTask}.
     *
     * @param bucket the bucket name
     * @param object the object name
     * @param uploadId multipart upload tmp directory fileId
     */
    public AbortTask(String bucket, String object,
                     Long uploadId) {
      mBucket = bucket;
      mObject = object;
      mUploadId = uploadId;
      mRetryCount = 0;
    }

    @Override
    public void run() {
      try {
        long delay = tryAbortMultipartUpload(mBucket, mObject, mUploadId);
        if (delay > 0) {
          apply(this, delay);
        } else {
          removeTaskRecord(this);
        }
      } catch (IOException | AlluxioException e) {
        mRetryCount++;
        LOG.error("Failed abort multipart upload {} in bucket {} with uploadId {} "
                + "after {} retries with error {}.", mObject, mBucket, mUploadId, mRetryCount, e);
        e.printStackTrace();
        if (canRetry(this)) {
          apply(this, mRetryDelay);
        }
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      AbortTask abortTask = (AbortTask) o;
      return mBucket.equals(abortTask.mBucket)
          && mObject.equals(abortTask.mObject)
          && mUploadId.equals(abortTask.mUploadId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mBucket, mObject, mUploadId);
    }

    @Override
    public String toString() {
      return "AbortTask{"
          + "mBucket='" + mBucket + '\''
          + ", mObject='" + mObject + '\''
          + ", mUploadId=" + mUploadId
          + ", mRetryCount=" + mRetryCount
          + '}';
    }
  }
}
