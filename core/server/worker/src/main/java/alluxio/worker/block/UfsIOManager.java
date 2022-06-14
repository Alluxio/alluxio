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

package alluxio.worker.block;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.ResourceExhaustedException;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;

import com.codahale.metrics.Meter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;

/**
 * Control UFS IO.
 */
public class UfsIOManager implements Closeable {
  private static final int READ_CAPACITY = 1024;
  private final UfsManager.UfsClient mUfsClient;
  private final ConcurrentMap<String, Integer> mThroughputQuota = new ConcurrentHashMap<>();
  private final UfsInputStreamCache mUfsInstreamCache = new UfsInputStreamCache();
  private final LinkedBlockingQueue<ReadTask> mReadQueue = new LinkedBlockingQueue<>(READ_CAPACITY);
  private final ConcurrentMap<AlluxioURI, Meter> mUfsBytesReadThroughputMetrics =
      new ConcurrentHashMap<>();
  private final ExecutorService mUfsIoExecutor =
      Executors.newFixedThreadPool(Configuration.getInt(PropertyKey.UNDERFS_IO_THREADS),
          ThreadFactoryUtils.build("UfsIOManager-IO-%d", false));
  private final ExecutorService mScheduleExecutor = Executors
      .newSingleThreadExecutor(ThreadFactoryUtils.build("UfsIOManager-Scheduler-%d", true));

  /**
   * @param ufsClient ufs client
   */
  public UfsIOManager(UfsManager.UfsClient ufsClient) {
    mUfsClient = ufsClient;
  }

  /**
   * Start schedule thread. Main reason is for test
   */
  public void start() {
    mScheduleExecutor.submit(this::schedule);
  }

  /**
   * Close.
   */
  @Override
  public void close() {
    mScheduleExecutor.shutdownNow();
    mUfsIoExecutor.shutdownNow();
  }

  /**
   * Set throughput quota for tag.
   * @param tag the client name or tag
   * @param throughput throughput limit, MB/s
   */
  public void setQuotaInMB(String tag, int throughput) {
    Preconditions.checkArgument(throughput > 0, "throughput should be positive");
    mThroughputQuota.put(tag, throughput);
  }

  private void schedule() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        ReadTask task = mReadQueue.take();
        if (mThroughputQuota.containsKey(task.mTag)
            && mThroughputQuota.get(task.mTag) * Constants.MB < getUsedThroughput(task.mMeter)) {
          // resubmit to queue
          mReadQueue.put(task);
        } else {
          try {
            mUfsIoExecutor.submit(task);
          } catch (RejectedExecutionException e) {
            // should not reach here since we use unbounded queue in thread pool and use a bounded
            // blocking queue upfront to control number of read tasks
            mReadQueue.put(task);
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * Get used throughput.
   * @param meter throughput meter
   * @return throughput
   */
  @VisibleForTesting
  public double getUsedThroughput(Meter meter) {
    return meter.getOneMinuteRate();
  }

  /**
   * Read from ufs.
   * @param blockId block id
   * @param offset offset in ufs file
   * @param length length to read
   * @param blockSize block size
   * @param ufsPath ufs path
   * @param positionShort is position short or not, used for HDFS performance optimization. When
   *                      the client buffer size is large ( > 2MB) and reads are guaranteed
   *                      to be somewhat sequential, the `pread` API to HDFS is not as efficient as
   *                      simple `read`. We introduce a heuristic to choose which API to use.
   * @param tag user/client name or specific identifier of the read task
   * @return content read
   * @throws ResourceExhaustedException when too many read task happens
   */
  public CompletableFuture<byte[]> read(long blockId, long offset, long length, long blockSize,
      String ufsPath, boolean positionShort, String tag) throws ResourceExhaustedException {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    if (mReadQueue.size() >= READ_CAPACITY) {
      throw new ResourceExhaustedException("UFS read at capacity");
    }
    Meter meter = mUfsBytesReadThroughputMetrics.computeIfAbsent(mUfsClient.getUfsMountPointUri(),
        uri -> MetricsSystem.meterWithTags(MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.getName(),
            MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.isClusterAggregated(), MetricInfo.TAG_UFS,
            MetricsSystem.escape(mUfsClient.getUfsMountPointUri()), MetricInfo.TAG_USER, tag));
    long bytesToRead = Math.min(length, blockSize - offset);
    if (bytesToRead <= 0) {
      future.complete(new byte[0]);
      return future;
    }
    mReadQueue.add(new ReadTask(ufsPath, IdUtils.fileIdFromBlockId(blockId), offset,
        bytesToRead, positionShort, tag, future, meter));
    return future;
  }

  private class ReadTask implements Runnable {
    private final long mOffset;
    private final long mBytesToRead;
    private final boolean mIsPositionShort;
    private final CompletableFuture<byte[]> mFuture;
    private final String mUfsPath;
    private final String mTag;
    private final Meter mMeter;
    private final long mFileId;

    private ReadTask(String ufsPath, long fileId, long offset, long bytesToRead,
        boolean positionShort, String tag, CompletableFuture<byte[]> future, Meter meter) {
      mTag = tag;
      mUfsPath = ufsPath;
      mFileId = fileId;
      mOffset = offset;
      mBytesToRead = bytesToRead;
      mIsPositionShort = positionShort;
      mFuture = future;
      mMeter = meter;
    }

    public void run() {
      try {
        byte[] buffer = readInternal();
        mFuture.complete(buffer);
      } catch (IOException e) {
        mFuture.completeExceptionally(e);
      }
    }

    private byte[] readInternal() throws IOException {
      byte[] data = new byte[(int) mBytesToRead];
      int bytesRead = 0;
      InputStream inStream = null;
      try (CloseableResource<UnderFileSystem> ufsResource = mUfsClient.acquireUfsResource()) {
        inStream = mUfsInstreamCache.acquire(ufsResource.get(), mUfsPath, mFileId,
            OpenOptions.defaults().setOffset(mOffset).setPositionShort(mIsPositionShort));
        while (bytesRead < mBytesToRead) {
          int read;
          read = inStream.read(data, bytesRead, (int) (mBytesToRead - bytesRead));
          if (read == -1) {
            break;
          }
          bytesRead += read;
        }
      } catch (IOException e) {
        throw AlluxioStatusException.fromIOException(e);
      } finally {
        if (inStream != null) {
          mUfsInstreamCache.release(inStream);
        }
      }
      mMeter.mark(bytesRead);
      return data;
    }
  }
}
