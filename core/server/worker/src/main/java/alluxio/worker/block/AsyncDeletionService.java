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
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.util.ThreadFactoryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Asychronous service to delete blocks.
 */
@ThreadSafe
public class AsyncDeletionService {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncDeletionService.class);

  private static final int DEFAULT_BLOCK_REMOVER_POOL_SIZE = 10;

  private final BlockWorker mBlockWorker;
  private final BlockingQueue<Long> mPendingRemovedBlocks;
  private final Set<Long> mToAvoidDuplicateBlock;
  private final ExecutorService mRemoverPool;

  /**
   * Constructor of AsyncDeletionService
   * @param worker block worker
   * @param queue a blocking queue to store pending deletion blocks
   */
  public AsyncDeletionService(BlockWorker worker, BlockingQueue queue) {
    mBlockWorker = worker;
    mPendingRemovedBlocks = queue;
    mToAvoidDuplicateBlock = Collections.newSetFromMap(new ConcurrentHashMap<>());
    mRemoverPool = Executors.newFixedThreadPool(DEFAULT_BLOCK_REMOVER_POOL_SIZE,
        ThreadFactoryUtils.build("block-removal-service-%d", true));
    int i = 0;
    while (++i <= DEFAULT_BLOCK_REMOVER_POOL_SIZE) {
      mRemoverPool.execute(new BlockRemover());
    }
  }

  /**
   * Put blocks into async deletion service. This method will take care of the duplicate blocks.
   * @param blocks blocks to be deleted
   */
  public void pendingBlocksToBeDeleted(List<Long> blocks) {
    for (long id : blocks) {
      if (mToAvoidDuplicateBlock.contains(id)) {
        LOG.debug("{} is pending. Current queue size is {}.", id, mPendingRemovedBlocks.size());
        continue;
      }
      try {
        mPendingRemovedBlocks.put(id);
        mToAvoidDuplicateBlock.add(id);
      } catch (InterruptedException e) {
        LOG.warn("AsyncDeletionService got interrupted while it was putting block {}.", id);
      }
    }
  }

  /**
   * Shutdown async deletion service.
   */
  public void shutDown() {
    mRemoverPool.shutdown();
  }

  private class BlockRemover implements Runnable {
    private String mThreadName;

    @Override
    public void run() {
      mThreadName = Thread.currentThread().getName();
      long blockToBeRemoved;
      while (true) {
        blockToBeRemoved = -1L;
        try {
          blockToBeRemoved = mPendingRemovedBlocks.take();
          mBlockWorker.removeBlock(Sessions.MASTER_COMMAND_SESSION_ID, blockToBeRemoved);
          LOG.info("Block {} is removed in thread {}.", blockToBeRemoved, mThreadName);
        } catch (InterruptedException e) {
          LOG.warn("{} got interrupted while it was cleaning block {}.",
              mThreadName, blockToBeRemoved);
        } catch (IOException e) {
          LOG.warn("IOException occurred while {} was cleaning block {}, exception is {}.",
              mThreadName, blockToBeRemoved, e.getMessage());
        } catch (BlockDoesNotExistException e) {
          LOG.warn("{}: block {} may be deleted already. exception is {}.",
              mThreadName, blockToBeRemoved, e.getMessage());
        } catch (InvalidWorkerStateException e) {
          LOG.warn("{}: invalid block state for block {}, exception is {}.",
              mThreadName, blockToBeRemoved, e.getMessage());
        } finally {
          if (blockToBeRemoved != -1L) {
            mBlockWorker.unlockBlock(Sessions.MASTER_COMMAND_SESSION_ID, blockToBeRemoved);
            mToAvoidDuplicateBlock.remove(blockToBeRemoved);
          }
        }
      }
    }
  }
}
