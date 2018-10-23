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
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.io.BufferUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import com.codahale.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Handles client requests to asynchronously cache blocks. Responsible for managing the local
 * worker resources and intelligent pruning of duplicate or meaningless requests.
 */
@ThreadSafe
public class AsyncCacheRequestManager {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncCacheRequestManager.class);

  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();
  /** Executor service for execute the async cache tasks. */
  private final ExecutorService mAsyncCacheExecutor;
  /** The block worker. */
  private final BlockWorker mBlockWorker;
  private final ConcurrentHashMap<Long, Protocol.AsyncCacheRequest> mPendingRequests;
  private final String mLocalWorkerHostname;

  /**
   * @param service thread pool to run the background caching work
   * @param blockWorker handler to the block worker
   */
  public AsyncCacheRequestManager(ExecutorService service, BlockWorker blockWorker) {
    mAsyncCacheExecutor = service;
    mBlockWorker = blockWorker;
    mPendingRequests = new ConcurrentHashMap<>();
    mLocalWorkerHostname = NetworkAddressUtils.getLocalHostName();
  }

  /**
   * Handles a request to cache a block asynchronously. This is a non-blocking call.
   *
   * @param request the async cache request fields will be available
   */
  public void submitRequest(Protocol.AsyncCacheRequest request) {
    ASYNC_CACHE_REQUESTS.inc();
    long blockId = request.getBlockId();
    long blockLength = request.getLength();
    if (mPendingRequests.putIfAbsent(blockId, request) != null) {
      // This block is already planned.
      ASYNC_CACHE_DUPLICATE_REQUESTS.inc();
      return;
    }
    try {
      mAsyncCacheExecutor.submit(() -> {
        boolean result = false;
        try {
          // Check if the block has already been cached on this worker
          long lockId = mBlockWorker.lockBlockNoException(Sessions.ASYNC_CACHE_SESSION_ID, blockId);
          if (lockId != BlockLockManager.INVALID_LOCK_ID) {
            try {
              mBlockWorker.unlockBlock(lockId);
            } catch (BlockDoesNotExistException e) {
              LOG.error("Failed to unlock block on async caching. We should never reach here", e);
            }
            ASYNC_CACHE_DUPLICATE_REQUESTS.inc();
            return;
          }
          Protocol.OpenUfsBlockOptions openUfsBlockOptions = request.getOpenUfsBlockOptions();
          boolean isSourceLocal = mLocalWorkerHostname.equals(request.getSourceHost());
          // Depends on the request, cache the target block from different sources
          if (isSourceLocal) {
            ASYNC_CACHE_UFS_BLOCKS.inc();
            result = cacheBlockFromUfs(blockId, blockLength, openUfsBlockOptions);
          } else {
            ASYNC_CACHE_REMOTE_BLOCKS.inc();
            InetSocketAddress sourceAddress =
                new InetSocketAddress(request.getSourceHost(), request.getSourcePort());
            result = cacheBlockFromRemoteWorker(
                    blockId, blockLength, sourceAddress, openUfsBlockOptions);
          }
          LOG.debug("Result of async caching block {}: {}", blockId, result);
        } catch (Exception e) {
          LOG.warn("Async cache request failed.\n{}\nError: {}", request, e);
        } finally {
          if (result) {
            ASYNC_CACHE_SUCCEEDED_BLOCKS.inc();
          } else {
            ASYNC_CACHE_FAILED_BLOCKS.inc();
          }
          mPendingRequests.remove(blockId);
        }
      });
    } catch (Exception e) {
      // RuntimeExceptions (e.g. RejectedExecutionException) may be thrown in extreme cases when the
      // netty thread pool is drained due to highly concurrent caching workloads. In these cases,
      // return as async caching is at best effort.
      LOG.warn("Failed to submit async cache request.\n{}\nError: {}", request, e);
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
    // Check if the block has been requested in UFS block store
    try {
      if (!mBlockWorker
          .openUfsBlock(Sessions.ASYNC_CACHE_SESSION_ID, blockId, openUfsBlockOptions)) {
        LOG.warn("Failed to async cache block {} from UFS on opening the block", blockId);
        return false;
      }
    } catch (BlockAlreadyExistsException e) {
      // It is already cached
      return true;
    }
    try (BlockReader reader = mBlockWorker
        .readUfsBlock(Sessions.ASYNC_CACHE_SESSION_ID, blockId, 0)) {
      // Read the entire block, caching to block store will be handled internally in UFS block store
      // Note that, we read from UFS with a smaller buffer to avoid high pressure on heap
      // memory when concurrent async requests are received and thus trigger GC.
      long offset = 0;
      while (offset < blockSize) {
        long bufferSize = Math.min(8 * Constants.MB, blockSize - offset);
        reader.read(offset, bufferSize);
        offset += bufferSize;
      }
    } catch (AlluxioException | IOException e) {
      // This is only best effort
      LOG.warn("Failed to async cache block {} from UFS on copying the block: {}", blockId, e);
      return false;
    } finally {
      try {
        mBlockWorker.closeUfsBlock(Sessions.ASYNC_CACHE_SESSION_ID, blockId);
      } catch (AlluxioException | IOException ee) {
        LOG.warn("Failed to close UFS block {}: {}", blockId, ee);
        return false;
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
      InetSocketAddress sourceAddress, Protocol.OpenUfsBlockOptions openUfsBlockOptions) {
    try {
      mBlockWorker.createBlockRemote(Sessions.ASYNC_CACHE_SESSION_ID, blockId,
          mStorageTierAssoc.getAlias(0), blockSize);
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
        new RemoteBlockReader(blockId, blockSize, sourceAddress, openUfsBlockOptions);
        BlockWriter writer =
            mBlockWorker.getTempBlockWriterRemote(Sessions.ASYNC_CACHE_SESSION_ID, blockId)) {
      BufferUtils.fastCopy(reader.getChannel(), writer.getChannel());
      mBlockWorker.commitBlock(Sessions.ASYNC_CACHE_SESSION_ID, blockId);
      return true;
    } catch (AlluxioException | IOException e) {
      LOG.warn("Failed to async cache block {} from remote worker ({}) on copying the block: {}",
          blockId, sourceAddress, e.getMessage());
      try {
        mBlockWorker.abortBlock(Sessions.ASYNC_CACHE_SESSION_ID, blockId);
      } catch (AlluxioException | IOException ee) {
        LOG.warn("Failed to abort block {}: {}", blockId, ee.getMessage());
      }
      return false;
    }
  }

  // Metrics
  private static final Counter ASYNC_CACHE_REQUESTS = MetricsSystem.counter("AsyncCacheRequests");
  private static final Counter ASYNC_CACHE_DUPLICATE_REQUESTS =
      MetricsSystem.counter("AsyncCacheDuplicateRequests");
  private static final Counter ASYNC_CACHE_FAILED_BLOCKS =
      MetricsSystem.counter("AsyncCacheFailedBlocks");
  private static final Counter ASYNC_CACHE_REMOTE_BLOCKS =
      MetricsSystem.counter("AsyncCacheRemoteBlocks");
  private static final Counter ASYNC_CACHE_SUCCEEDED_BLOCKS =
      MetricsSystem.counter("AsyncCacheSucceededBlocks");
  private static final Counter ASYNC_CACHE_UFS_BLOCKS =
      MetricsSystem.counter("AsyncCacheUfsBlocks");
}
