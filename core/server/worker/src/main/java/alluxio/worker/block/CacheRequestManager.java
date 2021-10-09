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
import alluxio.exception.status.CancelledException;
import alluxio.grpc.CacheRequest;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Handles client requests to synchronously/asynchronously cache blocks. Responsible for
 * managing the local worker resources and intelligent pruning of duplicate or meaningless requests.
 */
@ThreadSafe
public class CacheRequestManager {
  private static final Logger LOG = LoggerFactory.getLogger(CacheRequestManager.class);
  private static final Logger SAMPLING_LOG = new SamplingLogger(LOG, 10L * Constants.MINUTE_MS);

  /** Executor service for execute the async cache tasks. */
  private final ExecutorService mCacheExecutor;
  /** The block worker. */
  private final BlockWorker mBlockWorker;
  private final ConcurrentHashMap<Long, CacheRequest> mActiveCacheRequests;
  private final String mLocalWorkerHostname;
  private final FileSystemContext mFsContext;
  /** Keeps track of the number of rejected cache requests. */
  private final AtomicLong mNumRejected = new AtomicLong(0);

  /**
   * @param service thread pool to run the background caching work
   * @param blockWorker handler to the block worker
   * @param fsContext context
   */
  public CacheRequestManager(ExecutorService service, BlockWorker blockWorker,
      FileSystemContext fsContext) {
    mCacheExecutor = service;
    mBlockWorker = blockWorker;
    mFsContext = fsContext;
    mActiveCacheRequests = new ConcurrentHashMap<>();
    mLocalWorkerHostname = NetworkAddressUtils.getLocalHostName(
        (int) ServerConfiguration.getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
  }

  /**
   * Handles a request to cache a block. If it's async cache, it wouldn't throw exception.
   *
   * @param request the cache request fields will be available
   */
  public void submitRequest(CacheRequest request)
      throws AlluxioException, IOException {
    CACHE_REQUESTS.inc();
    long blockId = request.getBlockId();
    boolean async = request.getAsync();
    if (mActiveCacheRequests.putIfAbsent(blockId, request) != null) {
      // This block is already planned and just just return.
      if (async) {
        LOG.debug("request already planned: {}", request);
      } else {
        try {
          CommonUtils.waitFor("block to be loaded",
              () -> !mActiveCacheRequests.containsKey(blockId),
              WaitForOptions.defaults().setTimeoutMs(30 * Constants.SECOND_MS));
        } catch (InterruptedException e) {
          throw new CancelledException("Fail to finish cache request synchronously. "
              + "Interrupted while waiting for block to be loaded by another request.", e);
        } catch (TimeoutException e) {
          throw new CancelledException("Fail to finish cache request synchronously due to timeout",
              e);
        }
      }
      return;
    }

    if (!async) {
      CACHE_REQUESTS_SYNC.inc();
    } else {
      CACHE_REQUESTS_ASYNC.inc();
    }
    Future<Void> future = null;
    try {
      future = mCacheExecutor.submit(new CacheTask(request));
    } catch (RejectedExecutionException e) {
      // RejectedExecutionException may be thrown in extreme cases when the
      // gRPC thread pool is drained due to highly concurrent caching workloads. In these cases,
      // return as async caching is at best effort.
      mNumRejected.incrementAndGet();
      SAMPLING_LOG.warn(String.format(
          "Failed to cache block locally as the thread pool is at capacity."
              + " To increase, update the parameter '%s'. numRejected: {} error: {}",
          PropertyKey.Name.WORKER_NETWORK_ASYNC_CACHE_MANAGER_THREADS_MAX), mNumRejected.get(),
          e.toString());
      mActiveCacheRequests.remove(blockId);
      if (!async) {
        throw new CancelledException(
            "Fail to finish cache request synchronously as the thread pool is at capacity.", e);
      }
    }
    if (future != null && !async) {
      try {
        future.get();
      } catch (ExecutionException e) {
        CACHE_FAILED_BLOCKS.inc();
        Throwable cause = e.getCause();
        if (cause instanceof AlluxioException) {
          throw new AlluxioException(cause.getMessage(), cause);
        } else {
          throw new IOException(cause);
        }
      } catch (InterruptedException e) {
        throw new CancelledException(
            "Fail to finish cache request synchronously. Interrupted while waiting for response.",
            e);
      }
    }
  }

  /**
   * CacheTask is a callable task that can be considered equal if the blockId of the request is the
   * same.
   */
  @VisibleForTesting
  class CacheTask implements Callable<Void> {
    private final CacheRequest mRequest;

    /**
     * Constructor for an CacheTask.
     *
     * @param request an CacheRequest
     */
    CacheTask(CacheRequest request) {
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
      if (!(obj instanceof CacheTask)) {
        return false;
      }
      CacheTask that = ((CacheTask) obj);
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
    public Void call() throws IOException, AlluxioException {
      long blockId = mRequest.getBlockId();
      long blockLength = mRequest.getLength();
      boolean result = false;
      try {
        result = cacheBlock(mRequest);
      } finally {
        if (result) {
          CACHE_BLOCKS_SIZE.inc(blockLength);
          CACHE_SUCCEEDED_BLOCKS.inc();
        } else {
          CACHE_FAILED_BLOCKS.inc();
        }
        mActiveCacheRequests.remove(blockId);
      }
      return null;
    }
  }

  private boolean cacheBlock(CacheRequest request) throws IOException, AlluxioException {
    boolean result;
    boolean isSourceLocal = mLocalWorkerHostname.equals(request.getSourceHost());
    long blockId = request.getBlockId();
    long blockLength = request.getLength();
    // Check if the block has already been cached on this worker
    if (mBlockWorker.hasBlockMeta(blockId)) {
      LOG.debug("block already cached: {}", blockId);
      return true;
    }
    Protocol.OpenUfsBlockOptions openUfsBlockOptions = request.getOpenUfsBlockOptions();
    // Depends on the request, cache the target block from different sources
    if (isSourceLocal) {
      CACHE_UFS_BLOCKS.inc();
      result = cacheBlockFromUfs(blockId, blockLength, openUfsBlockOptions);
    } else {
      CACHE_REMOTE_BLOCKS.inc();
      InetSocketAddress sourceAddress =
          new InetSocketAddress(request.getSourceHost(), request.getSourcePort());
      result =
          cacheBlockFromRemoteWorker(blockId, blockLength, sourceAddress, openUfsBlockOptions);
    }
    LOG.debug("Result of caching block {}: {}", blockId, result);
    return result;
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
      Protocol.OpenUfsBlockOptions openUfsBlockOptions) throws IOException, AlluxioException {
    try (BlockReader reader = mBlockWorker.createUfsBlockReader(
        Sessions.CACHE_UFS_SESSION_ID, blockId, 0, false, openUfsBlockOptions)) {
      // Read the entire block, caching to block store will be handled internally in UFS block store
      // when close the reader.
      // Note that, we read from UFS with a smaller buffer to avoid high pressure on heap
      // memory when concurrent async requests are received and thus trigger GC.
      long offset = 0;
      while (offset < blockSize) {
        long bufferSize = Math.min(8L * Constants.MB, blockSize - offset);
        reader.read(offset, bufferSize);
        offset += bufferSize;
      }
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
      InetSocketAddress sourceAddress, Protocol.OpenUfsBlockOptions openUfsBlockOptions)
      throws IOException, AlluxioException {
    try {
      mBlockWorker.createBlock(Sessions.CACHE_WORKER_SESSION_ID, blockId, 0, "", blockSize);
    } catch (BlockAlreadyExistsException e) {
      // It is already cached
      LOG.debug("block already cached: {}", blockId);
      return true;
    }
    try (
        BlockReader reader =
            getRemoteBlockReader(blockId, blockSize, sourceAddress, openUfsBlockOptions);
         BlockWriter writer = mBlockWorker
             .createBlockWriter(Sessions.CACHE_WORKER_SESSION_ID, blockId)) {
      BufferUtils.transfer(reader.getChannel(), writer.getChannel());
      mBlockWorker.commitBlock(Sessions.CACHE_WORKER_SESSION_ID, blockId, false);
      return true;
    } catch (AlluxioException | IOException e) {
      LOG.warn("Failed to async cache block {} from remote worker ({}) on copying the block: {}",
          blockId, sourceAddress, e.toString());
      try {
        mBlockWorker.abortBlock(Sessions.CACHE_WORKER_SESSION_ID, blockId);
      } catch (AlluxioException | IOException ee) {
        LOG.warn("Failed to abort block {}: {}", blockId, ee.toString());
      }
      throw e;
    }
  }

  /**
   * only public to test since we want to mock remoteBlockReader.
   * @param blockId block ID
   * @param blockSize block size
   * @param sourceAddress remote source address
   * @param openUfsBlockOptions options to open the UFS file
   * @return remote block reader
   */
  @VisibleForTesting
  public RemoteBlockReader getRemoteBlockReader(long blockId, long blockSize,
      InetSocketAddress sourceAddress, Protocol.OpenUfsBlockOptions openUfsBlockOptions) {
    return new RemoteBlockReader(mFsContext, blockId, blockSize, sourceAddress,
        openUfsBlockOptions);
  }

  // Metrics
  private static final Counter CACHE_REQUESTS =
      MetricsSystem.counter(MetricKey.WORKER_CACHE_REQUESTS.getName());
  private static final Counter CACHE_REQUESTS_ASYNC =
      MetricsSystem.counter(MetricKey.WORKER_CACHE_REQUESTS_ASYNC.getName());
  private static final Counter CACHE_REQUESTS_SYNC =
      MetricsSystem.counter(MetricKey.WORKER_CACHE_REQUESTS_SYNC.getName());
  private static final Counter CACHE_FAILED_BLOCKS =
          MetricsSystem.counter(MetricKey.WORKER_CACHE_FAILED_BLOCKS.getName());
  private static final Counter CACHE_REMOTE_BLOCKS =
          MetricsSystem.counter(MetricKey.WORKER_CACHE_REMOTE_BLOCKS.getName());
  private static final Counter CACHE_SUCCEEDED_BLOCKS =
          MetricsSystem.counter(MetricKey.WORKER_CACHE_SUCCEEDED_BLOCKS.getName());
  private static final Counter CACHE_UFS_BLOCKS =
          MetricsSystem.counter(MetricKey.WORKER_CACHE_UFS_BLOCKS.getName());
  private static final Counter CACHE_BLOCKS_SIZE =
      MetricsSystem.counter(MetricKey.WORKER_CACHE_BLOCKS_SIZE.getName());
}
