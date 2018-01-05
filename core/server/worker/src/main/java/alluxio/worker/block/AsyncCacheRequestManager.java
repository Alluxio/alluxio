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

import alluxio.Sessions;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.io.BufferUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

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
   * @param request the async cache request
   * fields will be available
   */
  public void submitRequest(Protocol.AsyncCacheRequest request) {
    long blockId = request.getBlockId();
    if (mPendingRequests.putIfAbsent(blockId, request) != null) {
      // This block is already planned.
      return;
    }
    try {
      mAsyncCacheExecutor.submit(() -> {
        Protocol.OpenUfsBlockOptions openUfsBlockOptions = request.getOpenUfsBlockOptions();
        long blockSize = openUfsBlockOptions.getBlockSize();
        boolean isSourceLocal = mLocalWorkerHostname.equals(request.getSourceHost());
        // Depends on the request, cache the target block from different sources
        boolean result;
        if (isSourceLocal) {
          result = cacheBlockFromUfs(blockId, blockSize, openUfsBlockOptions);
        } else {
          InetSocketAddress sourceAddress =
              new InetSocketAddress(request.getSourceHost(), request.getSourcePort());
          result =
              cacheBlockFromRemoteWorker(blockId, blockSize, sourceAddress, openUfsBlockOptions);
        }
        LOG.debug("Result of async caching block {}: {}", blockId, result);
        mPendingRequests.remove(blockId);
      });
    } catch (Exception e) {
      // RuntimeExceptions (e.g. RejectedExecutionException) may be thrown in extreme cases when the
      // netty thread pool is drained due to highly concurrent caching workloads. In these cases,
      // return as async caching is at best effort.
      LOG.warn("Failed to submit async cache request {}: {}", request, e.getMessage());
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
      reader.read(0, blockSize);
    } catch (AlluxioException | IOException e) {
      // This is only best effort
      LOG.warn("Failed to async cache block {} from UFS on copying the block: {}", blockId,
          e.getMessage());
      return false;
    } finally {
      try {
        mBlockWorker.closeUfsBlock(Sessions.ASYNC_CACHE_SESSION_ID, blockId);
      } catch (AlluxioException | IOException ee) {
        LOG.warn("Failed to close UFS block {}: {}", blockId, ee.getMessage());
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
    try (BlockReader reader = new RemoteBlockReader(blockId, sourceAddress, openUfsBlockOptions);
         BlockWriter writer = mBlockWorker
             .getTempBlockWriterRemote(Sessions.ASYNC_CACHE_SESSION_ID, blockId)) {
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
}
