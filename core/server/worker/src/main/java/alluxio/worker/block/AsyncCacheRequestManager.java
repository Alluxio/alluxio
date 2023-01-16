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
import alluxio.DefaultStorageTierAssoc;
import alluxio.Sessions;
import alluxio.StorageTierAssoc;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
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

  private final StorageTierAssoc mStorageTierAssoc = new DefaultStorageTierAssoc(
      PropertyKey.WORKER_TIERED_STORE_LEVELS,
      PropertyKey.Template.WORKER_TIERED_STORE_LEVEL_ALIAS);

  /**
   * Executor service for execute the async cache tasks.
   */
  private final ExecutorService mAsyncCacheExecutor;
  /**
   * The block worker.
   */
  private final BlockWorker mBlockWorker;
  private final ConcurrentHashMap<Long, Protocol.AsyncCacheRequest> mPendingRequests;
  private final String mLocalWorkerHostname;

  /**
   * @param service     thread pool to run the background caching work
   * @param blockWorker handler to the block worker
   */
  public AsyncCacheRequestManager(ExecutorService service, BlockWorker blockWorker) {
    mAsyncCacheExecutor = service;
    mBlockWorker = blockWorker;
    mPendingRequests = new ConcurrentHashMap<>();
    mLocalWorkerHostname = NetworkAddressUtils.getLocalHostName((int) Configuration.global()
        .getMs(PropertyKey.NETWORK_HOST_RESOLUTION_TIMEOUT_MS));
  }

  /**
   * Handles a request to cache a block asynchronously. This is a non-blocking call.
   *
   * @param request the async cache request fields will be available
   */
  public void submitRequest(Protocol.AsyncCacheRequest request) {
    long blockId = request.getBlockId();
    long blockLength = request.getLength();
    if (mPendingRequests.putIfAbsent(blockId, request) != null) {
      // This block is already planned.
      return;
    }
    try {
      mAsyncCacheExecutor.submit(() -> {
        try {
          // Check if the block has already been cached on this worker
          long lockId =
              mBlockWorker.lockBlockNoException(Sessions.CACHE_WORKER_SESSION_ID, blockId);
          if (lockId != BlockLockManager.INVALID_LOCK_ID) {
            try {
              mBlockWorker.unlockBlock(lockId);
            } catch (BlockDoesNotExistException e) {
              LOG.error("Failed to unlock block on async caching. We should never reach here", e);
            }
            return;
          }
          Protocol.OpenUfsBlockOptions openUfsBlockOptions = request.getOpenUfsBlockOptions();
          boolean isSourceLocal = mLocalWorkerHostname.equals(request.getSourceHost());
          // Depends on the request, cache the target block from different sources
          boolean result;
          if (isSourceLocal) {
            result = cacheBlockFromUfs(blockId, blockLength, openUfsBlockOptions);
          } else {
            InetSocketAddress sourceAddress =
                new InetSocketAddress(request.getSourceHost(), request.getSourcePort());
            result = cacheBlockFromRemoteWorker(
                blockId, blockLength, sourceAddress, openUfsBlockOptions);
          }
          LOG.debug("Result of async caching block {}: {}", blockId, result);
        } catch (Exception e) {
          LOG.warn("Failed to complete async cache request {} from UFS", request, e.getMessage());
        } finally {
          mPendingRequests.remove(blockId);
        }
      });
    } catch (Exception e) {
      // RuntimeExceptions (e.g. RejectedExecutionException) may be thrown in extreme cases when the
      // netty thread pool is drained due to highly concurrent caching workloads. In these cases,
      // return as async caching is at best effort.
      LOG.warn("Failed to submit async cache request {}: {}", request, e.getMessage());
      mPendingRequests.remove(blockId);
    }
  }

  /**
   * Caches the block via the local worker to read from UFS.
   *
   * @param blockId             block ID
   * @param blockSize           block size
   * @param openUfsBlockOptions options to open the UFS file
   * @return if the block is cached
   */
  private boolean cacheBlockFromUfs(long blockId, long blockSize,
                                    Protocol.OpenUfsBlockOptions openUfsBlockOptions) {
    // Check if the block has been requested in UFS block store
    try {
      if (!mBlockWorker
          .openUfsBlock(Sessions.CACHE_WORKER_SESSION_ID, blockId, openUfsBlockOptions)) {
        LOG.warn("Failed to async cache block {} from UFS on opening the block", blockId);
        return false;
      }
    } catch (BlockAlreadyExistsException e) {
      // It is already cached
      return true;
    }
    try (BlockReader reader = mBlockWorker
        .readUfsBlock(Sessions.CACHE_WORKER_SESSION_ID, blockId, 0)) {
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
      LOG.warn("Failed to async cache block {} from UFS on copying the block: {}", blockId,
          e.getMessage());
      return false;
    } finally {
      try {
        mBlockWorker.closeUfsBlock(Sessions.CACHE_WORKER_SESSION_ID, blockId);
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
   * @param blockId             block ID
   * @param blockSize           block size
   * @param sourceAddress       the source to read the block previously by client
   * @param openUfsBlockOptions options to open the UFS file
   * @return if the block is cached
   */
  private boolean cacheBlockFromRemoteWorker(long blockId, long blockSize,
                                             InetSocketAddress sourceAddress,
                                             Protocol.OpenUfsBlockOptions openUfsBlockOptions) {
    try {
      mBlockWorker.createBlockRemote(Sessions.CACHE_WORKER_SESSION_ID, blockId,
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
    AlluxioConfiguration conf = Configuration.global();
    FileSystemContext fsContext = FileSystemContext.create(conf);
    try (BlockReader reader =
             new RemoteBlockReader(fsContext, blockId, blockSize, sourceAddress,
                 openUfsBlockOptions);
         BlockWriter writer =
             mBlockWorker.getTempBlockWriterRemote(Sessions.CACHE_WORKER_SESSION_ID, blockId)) {
      BufferUtils.fastCopy(reader.getChannel(), writer.getChannel());
      mBlockWorker.commitBlock(Sessions.CACHE_WORKER_SESSION_ID, blockId, false);
      return true;
    } catch (AlluxioException | IOException e) {
      LOG.warn("Failed to async cache block {} from remote worker ({}) on copying the block: {}",
          blockId, sourceAddress, e.getMessage());
      try {
        mBlockWorker.abortBlock(Sessions.CACHE_WORKER_SESSION_ID, blockId);
      } catch (IOException ee) {
        LOG.warn("Failed to abort block {}: {}", blockId, ee.getMessage());
      }
      return false;
    }
  }
}
