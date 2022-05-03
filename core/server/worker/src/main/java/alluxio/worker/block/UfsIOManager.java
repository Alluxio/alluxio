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
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.worker.block.meta.UnderFileSystemBlockMeta;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
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
public class UfsIOManager {
  private static final int SYNC_THREADS =
      ServerConfiguration.getInt(PropertyKey.UNDERFS_IO_THREADS);

  private final ConcurrentMap<BytesReadMetricKey, Counter> mUfsBytesReadMetrics =
      new ConcurrentHashMap<>();

  private final ConcurrentMap<AlluxioURI, Meter> mUfsBytesReadThroughputMetrics =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Double> mThroughputQuota = new ConcurrentHashMap<>();
  private final UfsManager.UfsClient mUfsClient;
  private final UfsInputStreamCache mUfsInstreamCache;
  private final LinkedBlockingQueue<ReadTask> mReadQueue = new LinkedBlockingQueue<>(8192);
  private final LinkedBlockingQueue<Runnable> mWriteQueue = new LinkedBlockingQueue<>();
  private ExecutorService mUfsIoExecutor;
  private ExecutorService mScheduleExecutor;

  public UfsIOManager(UfsManager.UfsClient ufsClient) {
    mUfsClient = ufsClient;
    mUfsInstreamCache = new UfsInputStreamCache();
  }

  public void start() throws IOException {
    mUfsIoExecutor = Executors.newFixedThreadPool(SYNC_THREADS,
        ThreadFactoryUtils.build("UfsIOManager-IO-%d", true));
    mScheduleExecutor = Executors
        .newSingleThreadExecutor(ThreadFactoryUtils.build("UfsIOManager-Scheduler-%d", true));
    mScheduleExecutor.submit(this::schedule);
  }

  public void setQuota(String user, double throughput) {
    mThroughputQuota.put(user, throughput);
  }

  private void schedule() {
    while (!Thread.currentThread().isInterrupted()) {
      ReadTask task = mReadQueue.poll();
      double quota = mThroughputQuota.getOrDefault(task.getOptions().getUser(), -1d);
      Meter ufsBytesReadThroughput =
          mUfsBytesReadThroughputMetrics.computeIfAbsent(mUfsClient.getUfsMountPointUri(),
              uri -> MetricsSystem.meterWithTags(
                  MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.getName(),
                  MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.isClusterAggregated(),
                  MetricInfo.TAG_UFS, MetricsSystem.escape(uri)));
      if (quota == -1 || quota > ufsBytesReadThroughput.getMeanRate()) {
        // submit read
        try {
          mUfsIoExecutor.submit(task);
          return;
        } catch (RejectedExecutionException e) {
          // or throw error?
          mReadQueue.add(task);
        }
      } else {// resubmit to queue
        mReadQueue.add(task);
      }
    }
  }

  public CompletableFuture<ByteBuffer> read(UnderFileSystemBlockMeta blockMeta, long offset,
      long length, boolean positionShort, Protocol.OpenUfsBlockOptions options) throws IOException {

    CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
    mReadQueue.add(new ReadTask(blockMeta, offset, length, positionShort, options, future));

    return future;
  }

  private class ReadTask implements Runnable {
    private final Protocol.OpenUfsBlockOptions mOptions;
    private final UnderFileSystemBlockMeta mBlockMeta;
    private final long mOffset;
    private final long mBytesToRead;
    private final boolean mIsPositionShort;
    private final CompletableFuture<ByteBuffer> mFuture;

    private ReadTask(UnderFileSystemBlockMeta blockMeta, long offset, long length,
        boolean positionShort, Protocol.OpenUfsBlockOptions options,
        CompletableFuture<ByteBuffer> future) {
      mOptions = options;
      mBlockMeta = blockMeta;
      mOffset = offset;
      mBytesToRead = length;
      mIsPositionShort = positionShort;
      mFuture = future;
    }

    public Protocol.OpenUfsBlockOptions getOptions() {
      return mOptions;
    }

    public void run() {
      try {
        ByteBuffer buffer = readInternal();
        mFuture.complete(buffer);
      } catch (IOException e) {
        mFuture.completeExceptionally(e);
      }
    }

    public ByteBuffer readInternal() throws IOException {
      Counter ufsBytesRead = mUfsBytesReadMetrics.computeIfAbsent(
          new BytesReadMetricKey(mUfsClient.getUfsMountPointUri(), mOptions.getUser()),
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
      InputStream inStream = mUfsInstreamCache.acquire(ufs, mBlockMeta.getUnderFileSystemPath(),
          IdUtils.fileIdFromBlockId(mBlockMeta.getBlockId()), OpenOptions.defaults()
              .setOffset(mBlockMeta.getOffset() + mOffset).setPositionShort(mIsPositionShort));

      if (mBytesToRead <= 0) {
        return ByteBuffer.allocate(0);
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
      return ByteBuffer.wrap(data, 0, bytesRead);
    }
  }
}
