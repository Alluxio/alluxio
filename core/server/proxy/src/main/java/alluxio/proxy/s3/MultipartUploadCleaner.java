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
import alluxio.conf.Configuration;
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
import javax.annotation.Nullable;

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
   * @param fs instance of {@link FileSystem}
   * @param bucket bucket name
   * @param object object name
   * @return true if add a abort task
   */
  public static boolean apply(final FileSystem fs, final String bucket, final String object)
      throws IOException, AlluxioException {
    final MultipartUploadCleaner cleaner = getInstance();
    // Use schedule pool do everyThing.
    long delay = cleaner.tryAbortMultipartUpload(fs, bucket, object, null);
    if (delay > 0) {
      long uploadId = cleaner.getMultipartUploadId(fs, bucket, object);
      return cleaner.apply(new AbortTask(fs, bucket, object, uploadId), 0);
    }
    return false;
  }

  /**
   * Schedule a task to clean multipart upload.
   *
   * @param fs instance of {@link FileSystem}
   * @param bucket bucket name
   * @param object object name
   * @param uploadId multipart upload tmp directory fileId
   * @return true if add a abort task
   */
  public static boolean apply(final FileSystem fs, final String bucket,
                              final String object, Long uploadId)
      throws IOException, AlluxioException {
    final MultipartUploadCleaner cleaner = getInstance();
    // Use schedule pool do everyThing.
    if (uploadId == null) {
      uploadId = cleaner.getMultipartUploadId(fs, bucket, object);
    }
    return cleaner.apply(new AbortTask(fs, bucket, object, uploadId), 0);
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
   * @param fs instance of {@link FileSystem}
   * @param bucket bucket name
   * @param object object name
   * @param uploadId multipart upload tmp directory fileId
   */
  public static void cancelAbort(final FileSystem fs, final String bucket,
                          final String object, final Long uploadId) {
    final MultipartUploadCleaner cleaner = getInstance();
    AbortTask task = new AbortTask(fs, bucket, object, uploadId);
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
   * @param fs instance of {@link FileSystem}
   * @param bucket the bucket name
   * @param object the object name
   * @param uploadId multipart upload tmp directory fileId
   * @return delay time
   */
  public long tryAbortMultipartUpload(FileSystem fs, String bucket, String object, Long uploadId)
      throws IOException, AlluxioException {
    long delay = 0;
    final String bucketPath = AlluxioURI.SEPARATOR  + bucket;
    final String multipartTemporaryDir =
        S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object);
    final String objectPath = bucketPath + AlluxioURI.SEPARATOR + object;
    AlluxioURI tmpUri = new AlluxioURI(multipartTemporaryDir);
    try {
      URIStatus status = fs.getStatus(tmpUri);
      if ((uploadId == null || uploadId == status.getFileId()) && status.isFolder()) {
        final long curTime = System.currentTimeMillis();
        long lastModificationTimeMs = status.getLastModificationTimeMs();
        delay = lastModificationTimeMs + mTimeout - curTime;
        if (delay <= 0) {
          // check object, when merge multipart upload, it may be timeout
          try {
            AlluxioURI uri = new AlluxioURI(objectPath);
            status = fs.getStatus(uri);
            lastModificationTimeMs = status.getLastModificationTimeMs();
            delay = lastModificationTimeMs + mTimeout - curTime;
            if (delay <= 0) {
              fs.delete(tmpUri, DeletePOptions.newBuilder().setRecursive(true).build());
              LOG.info("Abort multipart upload {} in bucket {} with uploadId {}.",
                  object, bucket, uploadId);
            }
          } catch (FileDoesNotExistException e) {
            fs.delete(tmpUri, DeletePOptions.newBuilder().setRecursive(true).build());
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
  private Long getMultipartUploadId(FileSystem fs, String bucket, String object)
      throws IOException, AlluxioException {
    final String bucketPath = AlluxioURI.SEPARATOR  + bucket;
    String multipartTemporaryDir =
        S3RestUtils.getMultipartTemporaryDirForObject(bucketPath, object);
    try {
      URIStatus status = fs.getStatus(new AlluxioURI(multipartTemporaryDir));
      return status.getFileId();
    } catch (FileDoesNotExistException e) {
      return null;
    }
  }

  /**
   * Abort Multipart upload task.
   */
  public static class AbortTask implements Runnable {
    private FileSystem mFileSystem;
    private final String mBucket;
    private final String mObject;
    private final Long mUploadId;
    private int mRetryCount;
    private MultipartUploadCleaner mCleaner;

    /**
     * Creates a new instance of {@link AbortTask}.
     *
     * @param fs instance of {@link FileSystem}
     * @param bucket the bucket name
     * @param object the object name
     * @param uploadId multipart upload tmp directory fileId
     */
    public AbortTask(final FileSystem fs, final String bucket,
                     final String object, final Long uploadId) {
      mFileSystem = fs;
      mBucket = bucket;
      mObject = object;
      mUploadId = uploadId;
      mRetryCount = 0;
      mCleaner = getInstance();
    }

    @Override
    public void run() {
      try {
        long delay = mCleaner.tryAbortMultipartUpload(mFileSystem, mBucket, mObject, mUploadId);
        if (delay > 0) {
          mCleaner.apply(this, delay);
        } else {
          mCleaner.removeTaskRecord(this);
        }
      } catch (IOException | AlluxioException e) {
        mRetryCount++;
        LOG.error("Failed abort multipart upload {} in bucket {} with uploadId {} "
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
