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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.master.block.BlockId;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
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
  private static final int IO_THREADS = ServerConfiguration.getInt(PropertyKey.UNDERFS_IO_THREADS);
  private static final int READ_CAPACITY =
      ServerConfiguration.getInt(PropertyKey.UNDERFS_IO_READ_QUEUE_CAPACITY);
  private final UfsManager.UfsClient mUfsClient;
  private final ConcurrentMap<BytesReadMetricKey, Counter> mUfsBytesReadMetrics =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<AlluxioURI, Meter> mUfsBytesReadThroughputMetrics =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Integer> mThroughputQuota = new ConcurrentHashMap<>();
  private final UfsInputStreamCache mUfsInstreamCache = new UfsInputStreamCache();
  private final LinkedBlockingQueue<ReadTask> mReadQueue = new LinkedBlockingQueue<>(READ_CAPACITY);
  private final ExecutorService mUfsIoExecutor = Executors.newFixedThreadPool(IO_THREADS,
      ThreadFactoryUtils.build("UfsIOManager-IO-%d", true));
  private final ExecutorService mScheduleExecutor = Executors
      .newSingleThreadExecutor(ThreadFactoryUtils.build("UfsIOManager-Scheduler-%d", true));

  /**
   * @param ufsClient ufs client
   */
  public UfsIOManager(UfsManager.UfsClient ufsClient) {
    mUfsClient = ufsClient;
  }

  /**
   * start ufs io manager.
   * 
   * @throws IOException
   */
  public void start() throws IOException {
    mScheduleExecutor.submit(this::schedule);
  }

  /**
   * close.
   */
  @Override
  public void close() {
    mScheduleExecutor.shutdownNow();
    mUfsIoExecutor.shutdownNow();
  }

  /**
   * set throughput quota for specific user.
   * 
   * @param user the client name or tag
   * @param throughput throughput limit
   */
  public void setQuota(String user, int throughput) {
    mThroughputQuota.put(user, throughput);
  }

  private void schedule() {
    while (!Thread.currentThread().isInterrupted()) {
      ReadTask task = null;
      try {
        task = mReadQueue.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      if (task != null) {
        int quota = mThroughputQuota.getOrDefault(task.getUser(), -1);
        Meter ufsBytesReadThroughput = mUfsBytesReadThroughputMetrics.computeIfAbsent(
            mUfsClient.getUfsMountPointUri(),
            uri -> MetricsSystem.meterWithTags(MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.getName(),
                MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.isClusterAggregated(),
                MetricInfo.TAG_UFS, MetricsSystem.escape(uri)));
        if (quota == -1 || quota > ufsBytesReadThroughput.getMeanRate()) {
          try {
            mUfsIoExecutor.submit(task);
            return;
          } catch (RejectedExecutionException e) {
            // or throw error?
            mReadQueue.add(task);
          }
        } else { // resubmit to queue
          mReadQueue.add(task);
        }
      }
    }
  }

  /**
   * read from ufs.
   * @param blockId block id
   * @param blockSize block size
   * @param ufsPath ufs path
   * @param offset offset to read
   * @param length length to read
   * @param positionShort is position short or not
   * @param user user tag
   * @return content read
   * @throws IOException any exception from ufs
   */
  public CompletableFuture<byte[]> read(long blockId, long blockSize, String ufsPath, long offset,
      long length, boolean positionShort, String user) throws IOException {

    CompletableFuture<byte[]> future = new CompletableFuture<>();
    mReadQueue.add(
        new ReadTask(blockId, blockSize, ufsPath, offset, length, positionShort, user, future));

    return future;
  }



  private class ReadTask implements Runnable {
    private final long mOffset;
    private final long mBytesToRead;
    private final boolean mIsPositionShort;
    private final CompletableFuture<byte[]> mFuture;
    private final long mBlockId;
    private final String mUfsPath;
    private final String mUser;
    private final long mBlockSize;

    private ReadTask(long blockId, long blockSize, String ufsPath, long offset, long length,
        boolean positionShort, String user, CompletableFuture<byte[]> future) {
      mUser = user;
      mBlockId = blockId;
      mBlockSize = blockSize;
      mUfsPath = ufsPath;
      mOffset = offset;
      mBytesToRead = length;
      mIsPositionShort = positionShort;
      mFuture = future;
    }

    public String getUser() {
      return mUser;
    }

    public void run() {
      try {
        byte[] buffer = readInternal();
        mFuture.complete(buffer);
      } catch (IOException e) {
        mFuture.completeExceptionally(e);
      }
    }

    public byte[] readInternal() throws IOException {
      Counter ufsBytesRead = mUfsBytesReadMetrics.computeIfAbsent(
          new BytesReadMetricKey(mUfsClient.getUfsMountPointUri(), mUser),
          key -> key.getUser() == null
              ? MetricsSystem.counterWithTags(MetricKey.WORKER_BYTES_READ_UFS.getName(),
                  MetricKey.WORKER_BYTES_READ_UFS.isClusterAggregated(), MetricInfo.TAG_UFS,
                  MetricsSystem.escape(key.getUri()))
              : MetricsSystem.counterWithTags(MetricKey.WORKER_BYTES_READ_UFS.getName(),
                  MetricKey.WORKER_BYTES_READ_UFS.isClusterAggregated(), MetricInfo.TAG_UFS,
                  MetricsSystem.escape(key.getUri()), MetricInfo.TAG_USER, key.getUser()));
      Meter ufsBytesReadThroughput = mUfsBytesReadThroughputMetrics.computeIfAbsent(
          mUfsClient.getUfsMountPointUri(),
          uri -> MetricsSystem.meterWithTags(MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.getName(),
              MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.isClusterAggregated(), MetricInfo.TAG_UFS,
              MetricsSystem.escape(uri)));

      UnderFileSystem ufs = mUfsClient.acquireUfsResource().get();
      long blockStart = BlockId.getSequenceNumber(mBlockId) * mBlockSize;
      InputStream inStream =
          mUfsInstreamCache.acquire(ufs, mUfsPath, IdUtils.fileIdFromBlockId(mBlockId), OpenOptions
              .defaults().setOffset(blockStart + mOffset).setPositionShort(mIsPositionShort));

      if (mBytesToRead <= 0) {
        return new byte[0];
      }
      byte[] data = new byte[(int) mBytesToRead];
      int bytesRead = 0;
      Preconditions.checkNotNull(inStream, "inStream");
      while (bytesRead < mBytesToRead) {
        int read;
        try {
          read = inStream.read(data, bytesRead, (int) (mBytesToRead - bytesRead));
        } catch (IOException e) {
          throw AlluxioStatusException.fromIOException(e);
        }
        if (read == -1) {
          break;
        }
        bytesRead += read;
      }
      mUfsInstreamCache.release(inStream);
      ufsBytesRead.inc(bytesRead);
      ufsBytesReadThroughput.mark(bytesRead);
      return data;
    }
  }
}
