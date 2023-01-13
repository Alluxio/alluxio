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
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.OutOfRangeRuntimeException;
import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.grpc.UfsReadOptions;
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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Objects;
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
  private final ConcurrentMap<String, Long> mThroughputQuota = new ConcurrentHashMap<>();
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
   * @param throughput throughput limit in bytes
   */
  public void setQuota(String tag, long throughput) {
    Preconditions.checkArgument(throughput > 0, "throughput should be positive");
    mThroughputQuota.put(tag, throughput);
  }

  private void schedule() {
    while (!Thread.currentThread().isInterrupted()) {
      try {
        ReadTask task = mReadQueue.take();
        if (mThroughputQuota.containsKey(task.mOptions.getTag())
            && mThroughputQuota.get(task.mOptions.getTag()) < getUsedThroughput(task.mMeter)) {
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
   *
   * @param buf bytebuffer
   * @param offset  offset in ufs file
   * @param len  length to read
   * @param blockId block id
   * @param ufsPath ufs path
   * @param options read ufs options
   * @return content read
   * @throws ResourceExhaustedRuntimeException when too many read task happens
   * @throws OutOfRangeRuntimeException offset is negative, len is negative, or len > buf remaining
   * @throws AlluxioRuntimeException future complete exceptionally when having exception from ufs
   */
  public CompletableFuture<Integer> read(ByteBuffer buf, long offset, long len, long blockId,
      String ufsPath, UfsReadOptions options) {
    Objects.requireNonNull(buf);
    if (offset < 0 || len < 0 || len > buf.remaining()) {
      throw new OutOfRangeRuntimeException(String.format(
          "offset is negative, len is negative, or len is greater than buf remaining. "
              + "offset: %s, len: %s, buf remaining: %s", offset, len, buf.remaining()));
    }
    if (mReadQueue.size() >= READ_CAPACITY) {
      throw new ResourceExhaustedRuntimeException("UFS read at capacity", true);
    }
    CompletableFuture<Integer> future = new CompletableFuture<>();
    if (len == 0) {
      future.complete(0);
      return future;
    }
    Meter meter = mUfsBytesReadThroughputMetrics.computeIfAbsent(mUfsClient.getUfsMountPointUri(),
        uri -> MetricsSystem.meterWithTags(MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.getName(),
            MetricKey.WORKER_BYTES_READ_UFS_THROUGHPUT.isClusterAggregated(), MetricInfo.TAG_UFS,
            MetricsSystem.escape(mUfsClient.getUfsMountPointUri()), MetricInfo.TAG_USER,
            options.getTag()));

    mReadQueue.add(new ReadTask(buf, ufsPath, IdUtils.fileIdFromBlockId(blockId), offset,
        len, options, future, meter));
    return future;
  }

  private class ReadTask implements Runnable {
    private final long mOffset;
    private final long mLength;
    private final CompletableFuture<Integer> mFuture;
    private final String mUfsPath;
    private final UfsReadOptions mOptions;
    private final Meter mMeter;
    private final long mFileId;
    private final ByteBuffer mBuffuer;

    private ReadTask(ByteBuffer buf, String ufsPath, long fileId, long offset, long length,
        UfsReadOptions options, CompletableFuture<Integer> future, Meter meter) {
      mOptions = options;
      mUfsPath = ufsPath;
      mFileId = fileId;
      mOffset = offset;
      mLength = length;
      mFuture = future;
      mMeter = meter;
      mBuffuer = buf;
    }

    public void run() {
      try {
        mFuture.complete(readInternal());
      } catch (RuntimeException e) {
        mFuture.completeExceptionally(e);
      }
    }

    private int readInternal() {
      int bytesRead = 0;
      InputStream inStream = null;
      try (CloseableResource<UnderFileSystem> ufsResource = mUfsClient.acquireUfsResource()) {
        if (mOptions.hasUser()) {
          // Before interacting with ufs manager, set the user.
          alluxio.security.authentication.AuthenticatedClientUser.set(mOptions.getUser());
        }
        inStream = mUfsInstreamCache.acquire(ufsResource.get(), mUfsPath, mFileId,
            OpenOptions.defaults().setOffset(mOffset)
                .setPositionShort(mOptions.getPositionShort()));
        while (bytesRead < mLength) {
          int read;
          read = Channels.newChannel(inStream).read(mBuffuer);
          if (read == -1) {
            break;
          }
          bytesRead += read;
        }
      } catch (Exception e) {
        throw AlluxioRuntimeException.from(e);
      } finally {
        if (inStream != null) {
          mUfsInstreamCache.release(inStream);
        }
      }
      mMeter.mark(bytesRead);
      return bytesRead;
    }
  }
}
