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

import alluxio.Constants;
import alluxio.Sessions;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.grpc.AsyncCacheRequest;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.io.BufferUtils;
import alluxio.util.logging.SamplingLogger;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Handles client requests to asynchronously cache blocks. Responsible for managing the local
 * worker resources and intelligent pruning of duplicate or meaningless requests.
 */
@ThreadSafe
public class AsyncCacheRequestManager {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncCacheRequestManager.class);
  private static final Logger SAMPLING_LOG = new SamplingLogger(LOG, 10L * Constants.MINUTE_MS);

  /** Executor service for execute the async cache tasks. */
  private final ExecutorService mAsyncCacheExecutor;
  /** The block worker. */
  private final BlockWorker mBlockWorker;
  private final ConcurrentHashMap<Long, AsyncCacheRequest> mPendingRequests;
  private final String mLocalWorkerHostname;
  private final FileSystemContext mFsContext;
  /** Keeps track of the number of rejected cache requests. */
  private final AtomicLong mNumRejected = new AtomicLong(0);

  /**
   * @param service thread pool to run the background caching work
   * @param blockWorker handler to the block worker
   * @param fsContext context
   */
  public AsyncCacheRequestManager(ExecutorService service, BlockWorker blockWorker,
      FileSystemContext fsContext) {
    mAsyncCacheExecutor = service;
    mBlockWorker = blockWorker;
    mFsContext = fsContext;
    mPendingRequests = new ConcurrentHashMap<>();
    mLocalWorkerHostname =
        NetworkAddressUtils.getLocalHostName(
            (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
  }

  /**
   * Handles a request to cache a block asynchronously. This is a non-blocking call.
   *
   * @param request the async cache request fields will be available
   */
  public void submitRequest(AsyncCacheRequest request) {
    ASYNC_CACHE_REQUESTS.inc();
    long blockId = request.getBlockId();
    if (mPendingRequests.putIfAbsent(blockId, request) != null) {
      // This block is already planned.
      ASYNC_CACHE_DUPLICATE_REQUESTS.inc();
      return;
    }
    try {
      mAsyncCacheExecutor.submit(new AsyncCacheTask(request));
    } catch (RejectedExecutionException e) {
      // RejectedExecutionException may be thrown in extreme cases when the
      // gRPC thread pool is drained due to highly concurrent caching workloads. In these cases,
      // return as async caching is at best effort.
      mNumRejected.incrementAndGet();
      SAMPLING_LOG.warn(String.format(
          "Failed to cache block locally (async & best effort) as the thread pool is at capacity."
              + " To increase, update the parameter '%s'. numRejected: {} error: {}",
          PropertyKey.Name.WORKER_NETWORK_ASYNC_CACHE_MANAGER_THREADS_MAX), mNumRejected.get(),
          e.getMessage());
    } catch (Exception e) {
      LOG.warn("Failed to submit async cache request. request: {}", request, e);
      ASYNC_CACHE_FAILED_BLOCKS.inc();
      mPendingRequests.remove(blockId);
    }
  }

  /**
   * Caches the block via the local worker to read from UFS.
   *
   * @param blockId block ID
   * @param blockSize block size
   * @param openUfsBlockOptions options to open the UFS file
   * @return if the block is cached
   */
  private boolean cacheBlockFromUfs(long blockId, long blockSize,
      Protocol.OpenUfsBlockOptions openUfsBlockOptions) {
    try (BlockReader reader = mBlockWorker.createUfsBlockReader(
        Sessions.ASYNC_CACHE_UFS_SESSION_ID, blockId, 0, false, openUfsBlockOptions)) {
      // Read the entire block, caching to block store will be handled internally in UFS block store
      // Note that, we read from UFS with a smaller buffer to avoid high pressure on heap
      // memory when concurrent async requests are received and thus trigger GC.
      long offset = 0;
      while (offset < blockSize) {
        long bufferSize = Math.min(8L * Constants.MB, blockSize - offset);
        reader.read(offset, bufferSize);
        offset += bufferSize;
      }
    } catch (AlluxioException | IOException e) {
      // This is only best effort
      LOG.warn("Failed to async cache block {} from UFS on copying the block: {}", blockId, e);
      return false;
    }
    return true;
  }

  /**
   * Caches the block at best effort from a remote worker (possibly from UFS indirectly).
   *
   * @param blockId block ID
   * @param blockSize block size
   * @param sourceAddress the source to read the block previously by client
   * @param openUfsBlockOptions options to open the UFS file
   * @return if the block is cached
   */
  private boolean cacheBlockFromRemoteWorker(long blockId, long blockSize,
      InetSocketAddress sourceAddress, Protocol.OpenUfsBlockOptions openUfsBlockOptions) {
    try {
      mBlockWorker.createBlock(Sessions.ASYNC_CACHE_WORKER_SESSION_ID, blockId, 0, "", blockSize);
    } catch (BlockAlreadyExistsException e) {
      // It is already cached
      return true;
    } catch (AlluxioException | IOException e) {
      LOG.warn(
          "Failed to async cache block {} from remote worker ({}) on creating the temp block: {}",
          blockId, sourceAddress, e.getMessage());
      return false;
    }
    try (BlockReader reader =
        new RemoteBlockReader(mFsContext, blockId, blockSize, sourceAddress, openUfsBlockOptions);
         BlockWriter writer = mBlockWorker
             .createBlockWriter(Sessions.ASYNC_CACHE_WORKER_SESSION_ID, blockId)) {
      BufferUtils.fastCopy(reader.getChannel(), writer.getChannel());
      mBlockWorker.commitBlock(Sessions.ASYNC_CACHE_WORKER_SESSION_ID, blockId, false);
      return true;
    } catch (AlluxioException | IOException e) {
      LOG.warn("Failed to async cache block {} from remote worker ({}) on copying the block: {}",
          blockId, sourceAddress, e.getMessage());
      try {
        mBlockWorker.abortBlock(Sessions.ASYNC_CACHE_WORKER_SESSION_ID, blockId);
      } catch (AlluxioException | IOException ee) {
        LOG.warn("Failed to abort block {}: {}", blockId, ee.getMessage());
      }
      return false;
    }
  }

  /**
   * AsyncCacheTask is a runnable task that can be considered equal if
   * the blockId of the request is the same.
   */
  @VisibleForTesting
  class AsyncCacheTask implements Runnable {
    private final AsyncCacheRequest mRequest;

    /**
     * Constructor for an AsyncCacheTask.
     *
     * @param request an AsyncCacheRequest
     */
    AsyncCacheTask(AsyncCacheRequest request) {
      mRequest = request;
    }

    @Override
    public int hashCode() {
      // Only care about the block id being the same.
      // Do not care if the source host or port is different.
      return Objects.hash(mRequest.getBlockId());
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof AsyncCacheTask)) {
        return false;
      }
      AsyncCacheTask that = ((AsyncCacheTask) obj);
      if (mRequest == that.mRequest) {
        return true;
      }
      if (this.mRequest == null || that.mRequest == null) {
        return false;
      }
      // Only care about the block id being the same.
      // Do not care if the source host or port is different.
      return mRequest.getBlockId() == that.mRequest.getBlockId();
    }

    @Override
    public void run() {
      boolean result = false;
      long blockId = mRequest.getBlockId();
      long blockLength = mRequest.getLength();
      try {
        boolean isSourceLocal = mLocalWorkerHostname.equals(mRequest.getSourceHost());
        // Check if the block has already been cached on this worker
        if (mBlockWorker.hasBlockMeta(mRequest.getBlockId())) {
          ASYNC_CACHE_DUPLICATE_REQUESTS.inc();
          return;
        }
        Protocol.OpenUfsBlockOptions openUfsBlockOptions = mRequest.getOpenUfsBlockOptions();
        // Depends on the request, cache the target block from different sources
        if (isSourceLocal) {
          ASYNC_CACHE_UFS_BLOCKS.inc();
          result = cacheBlockFromUfs(blockId, blockLength, openUfsBlockOptions);
        } else {
          ASYNC_CACHE_REMOTE_BLOCKS.inc();
          InetSocketAddress sourceAddress =
              new InetSocketAddress(mRequest.getSourceHost(), mRequest.getSourcePort());
          result = cacheBlockFromRemoteWorker(
              blockId, blockLength, sourceAddress, openUfsBlockOptions);
        }
        LOG.debug("Result of async caching block {}: {}", blockId, result);
      } catch (Exception e) {
        LOG.warn("Async cache task failed. request: {}", mRequest, e);
      } finally {
        if (result) {
          ASYNC_CACHE_SUCCEEDED_BLOCKS.inc();
        } else {
          ASYNC_CACHE_FAILED_BLOCKS.inc();
        }
        mPendingRequests.remove(blockId);
      }
    }
  }

  // Metrics
  private static final Counter ASYNC_CACHE_REQUESTS
      = MetricsSystem.counter(MetricKey.WORKER_ASYNC_CACHE_REQUESTS.getName());
  private static final Counter ASYNC_CACHE_DUPLICATE_REQUESTS =
      MetricsSystem.counter(MetricKey.WORKER_ASYNC_CACHE_DUPLICATE_REQUESTS.getName());
  private static final Counter ASYNC_CACHE_FAILED_BLOCKS =
      MetricsSystem.counter(MetricKey.WORKER_ASYNC_CACHE_FAILED_BLOCKS.getName());
  private static final Counter ASYNC_CACHE_REMOTE_BLOCKS =
      MetricsSystem.counter(MetricKey.WORKER_ASYNC_CACHE_REMOTE_BLOCKS.getName());
  private static final Counter ASYNC_CACHE_SUCCEEDED_BLOCKS =
      MetricsSystem.counter(MetricKey.WORKER_ASYNC_CACHE_SUCCEEDED_BLOCKS.getName());
  private static final Counter ASYNC_CACHE_UFS_BLOCKS =
      MetricsSystem.counter(MetricKey.WORKER_ASYNC_CACHE_UFS_BLOCKS.getName());
}
