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

import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.IdUtils;
import alluxio.util.io.BufferUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.annotation.concurrent.GuardedBy;
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
  private final Object mLock = new Object();
  @GuardedBy("mLock")
  private final Set<Long> mPendingRequests;
  private final WorkerNetAddress mLocalWorkerAddress;

  public AsyncCacheRequestManager(ExecutorService service, BlockWorker blockWorker) {
    mAsyncCacheExecutor = service;
    mBlockWorker = blockWorker;
    mPendingRequests = new HashSet<>();
    try {
      mLocalWorkerAddress = FileSystemContext.INSTANCE.getLocalWorker();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Handles a request to cache a block asynchronously. This is a non-blocking call.
   *
   * @param request the async cache request
   * fields will be available
   */
  public void submitRequest(Protocol.AsyncCacheRequest request) {
    long blockId = request.getBlockId();
    synchronized (mLock) {
      if (mPendingRequests.contains(blockId)) {
        // This block is already planned.
        return;
      }
      mPendingRequests.add(request.getBlockId());
    }
    mAsyncCacheExecutor.submit(() -> {
      Protocol.OpenUfsBlockOptions openUfsBlockOptions = request.getOpenUfsBlockOptions();
      long sessionId = IdUtils.createSessionId();
      long blockSize = openUfsBlockOptions.getBlockSize();
      boolean isSourceLocal = mLocalWorkerAddress.getHost().equals(request.getSourceHost())
          && mLocalWorkerAddress.getRpcPort() == request.getSourcePort();
      // Depends on the request, cache the target block from different sources
      try {
        if (isSourceLocal) {
          cacheBlockFromUfs(sessionId, blockId, blockSize, openUfsBlockOptions);
        } else {
          InetSocketAddress sourceAddress =
              new InetSocketAddress(request.getSourceHost(), request.getSourcePort());
          cacheBlockFromRemoteWorker(sessionId, blockId, blockSize, sourceAddress,
              openUfsBlockOptions);
        }
      } catch (BlockAlreadyExistsException e) {
        // It is already cached
      } catch (AlluxioException | IOException e) {
        // This is only best effort
        LOG.warn("Failed to async cache block {}: {}", blockId, e.getMessage());
      }
      mPendingRequests.remove(request.getBlockId());
    });
  }

  /**
   * Caches the block via the local worker to read from UFS.
   *
   * @param sessionId session ID
   * @param blockId block ID
   * @param blockSize block size
   * @param openUfsBlockOptions options to open the UFS file
   */
  private void cacheBlockFromUfs(long sessionId, long blockId, long blockSize,
      Protocol.OpenUfsBlockOptions openUfsBlockOptions)
      throws BlockAlreadyExistsException, BlockDoesNotExistException, IOException {
    if (!mBlockWorker.openUfsBlock(sessionId, blockId, openUfsBlockOptions)) {
      return;
    }
    try (BlockReader reader = mBlockWorker.readUfsBlock(sessionId, blockId, 0)) {
      reader.read(0, blockSize); // cache will be handled internally in UFS block store
    }
  }

  /**
   * Caches the block at best effort from a remote worker (possibly from UFS indirectly).
   *
   * @param sessionId session ID
   * @param blockId block ID
   * @param blockSize block size
   * @param sourceAddress the source to read the block previously by client
   * @param openUfsBlockOptions options to open the UFS file
   */
  private void cacheBlockFromRemoteWorker(long sessionId, long blockId, long blockSize,
      InetSocketAddress sourceAddress, Protocol.OpenUfsBlockOptions openUfsBlockOptions)
      throws AlluxioException, IOException {
    mBlockWorker.createBlockRemote(sessionId, blockId, mStorageTierAssoc.getAlias(0), blockSize);
    try (BlockReader reader = new RemoteBlockReader(blockId, sourceAddress, openUfsBlockOptions);
         BlockWriter writer = mBlockWorker.getTempBlockWriterRemote(sessionId, blockId)) {
      BufferUtils.fastCopy(reader.getChannel(), writer.getChannel());
    } catch (AlluxioException | IOException e) {
      try {
        mBlockWorker.abortBlock(sessionId, blockId);
      } catch (AlluxioException | IOException ee) {
        LOG.warn("Failed to abort block {}: {}", blockId, ee.getMessage());
      }
      throw e;
    }
    mBlockWorker.commitBlock(sessionId, blockId);
  }
}
