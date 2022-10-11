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
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.DeletePOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private final ScheduledThreadPoolExecutor mExecutor;
  private final ConcurrentHashMap<AbortTask, ScheduledFuture<?>> mTasks;

  private static MultipartUploadCleaner sInstance = null;

  /**
   * Creates a new instance of {@link MultipartUploadCleaner}.
   */
  private MultipartUploadCleaner() {
    mTimeout =
        Configuration.getMs(PropertyKey.PROXY_S3_MULTIPART_UPLOAD_CLEANER_TIMEOUT);
    mRetry =
        Configuration.getInt(PropertyKey.PROXY_S3_MULTIPART_UPLOAD_CLEANER_RETRY_COUNT);
    mRetryDelay =
        Configuration.getMs(PropertyKey.PROXY_S3_MULTIPART_UPLOAD_CLEANER_RETRY_DELAY);
    mExecutor = new ScheduledThreadPoolExecutor(
        Configuration.getInt(PropertyKey.PROXY_S3_MULTIPART_UPLOAD_CLEANER_POOL_SIZE));
    mTasks = new ConcurrentHashMap<>();
  }

  /**
   *@return instance of {@link MultipartUploadCleaner}
   */
  public static MultipartUploadCleaner getInstance() {
    if (sInstance == null) {
      synchronized (MultipartUploadCleaner.class) {
        if (sInstance == null) {
          sInstance = new MultipartUploadCleaner();
        }
      }
    }
    return sInstance;
  }

  /**
   * Shutdown cleaner.
   */
  public static void shutdown() {
    if (sInstance != null) {
      synchronized (MultipartUploadCleaner.class) {
        if (sInstance != null) {
          sInstance.mExecutor.shutdownNow();
          sInstance = null;
        }
      }
    }
  }

  /**
   * Schedule a task to clean multipart upload.
   *
   * @param metaFs instance of {@link FileSystem} - used for metadata operations
   * @param userFs instance of {@link FileSystem} - under the scope of a user agent
   * @param bucket bucket name
   * @param object object name
   * @param uploadId multipart upload tmp directory fileId
   * @return true if add a abort task
   */
  public static boolean apply(final FileSystem metaFs, final FileSystem userFs,
                              final String bucket, final String object, String uploadId)
      throws IOException, AlluxioException {
    final MultipartUploadCleaner cleaner = getInstance();
    // Use schedule pool do everything.
    return cleaner.apply(new AbortTask(metaFs, userFs, bucket, object, uploadId), 0);
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
   * @param metaFs instance of {@link FileSystem} - used for metadata operations
   * @param userFs instance of {@link FileSystem} - under the scope of a user agent
   * @param bucket bucket name
   * @param object object name
   * @param uploadId multipart upload tmp directory fileId
   */
  public static void cancelAbort(final FileSystem metaFs, final FileSystem userFs,
                                 final String bucket, final String object, final String uploadId) {
    final MultipartUploadCleaner cleaner = getInstance();
    AbortTask task = new AbortTask(metaFs, userFs, bucket, object, uploadId);
    if (cleaner.containsTaskRecord(task)) {
      ScheduledFuture<?> f = cleaner.removeTaskRecord(task);
      if (f != null) {
        f.cancel(false);
      }
    }
  }

  /**
   * Remove finished task.
   */
  private ScheduledFuture<?> removeTaskRecord(AbortTask task) {
    return mTasks.remove(task);
  }

  /**
   * Check task if already exist.
   */
  private boolean containsTaskRecord(AbortTask task) {
    return mTasks.containsKey(task);
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
   * Get retry delay.
   *
   * @return delay time
   */
  public long getRetryDelay() {
    return mRetryDelay;
  }

  /**
   * Try to abort a multipart upload if it was timeout.
   *
   * @param metaFs instance of {@link FileSystem} - used for metadata operations
   * @param userFs instance of {@link FileSystem} - under the scope of a user agent
   * @param bucket the bucket name
   * @param object the object name
   * @param uploadId multipart upload tmp directory fileId
   * @return delay time, non-positive values indicate to not retry this method
   */
  public long tryAbortMultipartUpload(FileSystem metaFs, FileSystem userFs,
                                      String bucket, String object, String uploadId)
      throws IOException, AlluxioException {
    final String bucketPath = S3RestUtils.parsePath(AlluxioURI.SEPARATOR + bucket);
    final AlluxioURI multipartTempDirUri = new AlluxioURI(
        S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object, uploadId));
    try {
      URIStatus status = S3RestUtils.checkStatusesForUploadId(metaFs, userFs,
          multipartTempDirUri, uploadId).get(0);
      // Check if multipart upload has exceeded its timeout
      final long curTime = System.currentTimeMillis();
      long delay = status.getLastModificationTimeMs() + mTimeout - curTime;
      if (delay > 0) { return delay; }
      // Abort the multipart upload
      userFs.delete(multipartTempDirUri, DeletePOptions.newBuilder().setRecursive(true).build());
      metaFs.delete(new AlluxioURI(S3RestUtils.getMultipartMetaFilepathForUploadId(uploadId)),
          DeletePOptions.newBuilder().build());
      LOG.info("Timeout exceeded, aborting multipart upload (bucket {}: object: {}, uploadId: {}).",
          object, bucket, uploadId);
    } catch (FileDoesNotExistException e) {
      return 0; // do not retry, multipart upload has been completed/aborted already
    }
    return 0; // do not retry, multipart upload has been completed/aborted by this method
  }

  /**
   * Abort Multipart upload task.
   */
  public static class AbortTask implements Runnable {
    private final FileSystem mMetaFs;
    private final FileSystem mUserFs;
    private final String mBucket;
    private final String mObject;
    private final String mUploadId;
    private int mRetryCount;
    private final MultipartUploadCleaner mCleaner;

    /**
     * Creates a new instance of {@link AbortTask}.
     *
     * @param metaFs instance of {@link FileSystem} - used for metadata operations
     * @param userFs instance of {@link FileSystem} - under the scope of a user agent
     * @param bucket the bucket name
     * @param object the object name
     * @param uploadId multipart upload tmp directory fileId
     */
    public AbortTask(final FileSystem metaFs, final FileSystem userFs, final String bucket,
                     final String object, final String uploadId) {
      mMetaFs = metaFs;
      mUserFs = userFs;
      mBucket = bucket;
      mObject = object;
      mUploadId = uploadId;
      mRetryCount = 0;
      mCleaner = getInstance();
    }

    @Override
    public void run() {
      try {
        long delay = mCleaner.tryAbortMultipartUpload(mMetaFs, mUserFs,
            mBucket, mObject, mUploadId);
        if (delay > 0) {
          mCleaner.apply(this, delay);
        } else {
          mCleaner.removeTaskRecord(this);
        }
      } catch (IOException | AlluxioException e) {
        mRetryCount++;
        LOG.error("Failed to abort multipart upload (bucket: {}, object: {}, uploadId: {}) "
                + "after {} retries with error {}.", mObject, mBucket, mUploadId, mRetryCount, e);
        e.printStackTrace();
        if (mCleaner.canRetry(this)) {
          mCleaner.apply(this, mCleaner.getRetryDelay());
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
