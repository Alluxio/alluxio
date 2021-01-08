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
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.ThreadFactoryUtils;

import com.codahale.metrics.Counter;
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
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Asynchronous block removal service.
 */
@ThreadSafe
public class AsyncBlockRemover {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncBlockRemover.class);

  private static final int DEFAULT_BLOCK_REMOVER_POOL_SIZE = 10;
  private static final int INVALID_BLOCK_ID = -1;

  private final BlockWorker mBlockWorker;
  /** This list is used for queueing blocks to be removed by BlockWorker. */
  private final BlockingQueue<Long> mBlocksToRemove;
  /** This set is used for recording blocks in BlockRemover. */
  private final Set<Long> mRemovingBlocks;
  private final ExecutorService mRemoverPool;
  private final Counter mTakeCount;
  private final Counter mRemovedSuccessCount;

  private volatile boolean mShutdown = false;

  /**
   * Constructor of AsyncBlockRemover.
   * @param worker block worker
   */
  public AsyncBlockRemover(BlockWorker worker) {
    mBlockWorker = worker;
    mBlocksToRemove = new LinkedBlockingQueue<>();
    mRemovingBlocks = Collections.newSetFromMap(new ConcurrentHashMap<>());
    mTakeCount = MetricsSystem.counter(MetricKey.WORKER_BLOCK_REMOVER_TRY_REMOVE_COUNT
        .getName());
    mRemovedSuccessCount =
        MetricsSystem.counter(MetricKey.WORKER_BLOCK_REMOVER_REMOVED_COUNT
            .getName());
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.WORKER_BLOCK_REMOVER_TRY_REMOVE_BLOCKS_SIZE.getName(),
        () -> mBlocksToRemove.size());
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.WORKER_BLOCK_REMOVER_REMOVING_BLOCKS_SIZE.getName(),
        () -> mRemovingBlocks.size());

    mRemoverPool = Executors.newFixedThreadPool(DEFAULT_BLOCK_REMOVER_POOL_SIZE,
        ThreadFactoryUtils.build("block-removal-service-%d", true));
    for (int i = 0; i < DEFAULT_BLOCK_REMOVER_POOL_SIZE; i++) {
      mRemoverPool.execute(new BlockRemover());
    }
  }

  /**
   * Put blocks into async block remover. This method will take care of the duplicate blocks.
   * @param blocks blocks to be deleted
   */
  public void addBlocksToDelete(List<Long> blocks) {
    for (long id : blocks) {
      if (mRemovingBlocks.contains(id)) {
        LOG.debug("{} is being removed. Current queue size is {}.", id, mBlocksToRemove.size());
        continue;
      }
      try {
        mBlocksToRemove.put(id);
        mRemovingBlocks.add(id);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("AsyncBlockRemover got interrupted while it was putting block {}.", id);
      }
    }
  }

  /**
   * Shutdown async block remover.
   */
  public void shutDown() {
    mShutdown = true;
    mRemoverPool.shutdownNow();
  }

  private class BlockRemover implements Runnable {
    private String mThreadName;

    @Override
    public void run() {
      mThreadName = Thread.currentThread().getName();
      long blockToBeRemoved;
      while (true) {
        blockToBeRemoved = INVALID_BLOCK_ID;
        try {
          blockToBeRemoved = mBlocksToRemove.take();
          mTakeCount.inc();
          mBlockWorker.removeBlock(Sessions.MASTER_COMMAND_SESSION_ID, blockToBeRemoved);
          mRemovedSuccessCount.inc();
          LOG.debug("Block {} is removed in thread {}.", blockToBeRemoved, mThreadName);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          // Only log warning if interrupted not due to a shutdown.
          if (!mShutdown) {
            LOG.warn("{} got interrupted while it was cleaning block {}.", mThreadName,
                blockToBeRemoved);
          }
          break;
        } catch (IOException e) {
          LOG.warn("IOException occurred while {} was cleaning block {}, exception is {}.",
              mThreadName, blockToBeRemoved, e.getMessage());
        } catch (BlockDoesNotExistException e) {
          LOG.warn("{}: block {} may be deleted already. exception is {}.",
              mThreadName, blockToBeRemoved, e.getMessage());
        } catch (InvalidWorkerStateException e) {
          LOG.warn("{}: invalid block state for block {}, exception is {}.",
              mThreadName, blockToBeRemoved, e.getMessage());
        } catch (Exception e) {
          LOG.warn("Unexpected exception: {}.", e);
        } finally {
          if (blockToBeRemoved != INVALID_BLOCK_ID) {
            mRemovingBlocks.remove(blockToBeRemoved);
          }
        }
      }
    }
  }
}
