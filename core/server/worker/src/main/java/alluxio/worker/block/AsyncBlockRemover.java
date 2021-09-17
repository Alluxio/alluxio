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
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.ThreadFactoryUtils;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Asynchronous block removal service.
 */
@ThreadSafe
public class AsyncBlockRemover {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncBlockRemover.class);

  private static final int DEFAULT_BLOCK_REMOVER_POOL_SIZE = 10;

  private final BlockWorker mBlockWorker;
  /** This list is used for queueing blocks to be removed by BlockWorker. */
  private final BlockingQueue<Long> mBlocksToRemove;
  /** This set is used for recording blocks in BlockRemover. */
  private final Set<Long> mRemovingBlocks;
  private final ExecutorService mRemoverPool;
  private final Counter mTryRemoveCount;
  private final Counter mRemovedCount;
  private final int mPoolSize;
  private volatile boolean mShutdown = false;

  /**
   * Constructor of AsyncBlockRemover.
   * @param worker block worker
   */
  public AsyncBlockRemover(BlockWorker worker) {
    this(worker, DEFAULT_BLOCK_REMOVER_POOL_SIZE, new LinkedBlockingQueue<>(),
        Collections.newSetFromMap(new ConcurrentHashMap<>()));
  }

  /**
   * Constructor of AsyncBlockRemover.
   *
   * @param worker block worker
   * @param threads number of threads
   * @param blocksToRemove blocks to remove
   * @param removingBlocks blocks being removed
   */
  @VisibleForTesting
  public AsyncBlockRemover(BlockWorker worker, int threads, BlockingQueue<Long> blocksToRemove,
      Set<Long> removingBlocks) {
    mBlockWorker = worker;
    mPoolSize = threads;
    mBlocksToRemove = blocksToRemove;
    mRemovingBlocks = removingBlocks;
    mTryRemoveCount = MetricsSystem.counter(
        MetricKey.WORKER_BLOCK_REMOVER_TRY_REMOVE_COUNT.getName());
    mRemovedCount = MetricsSystem.counter(
        MetricKey.WORKER_BLOCK_REMOVER_REMOVED_COUNT.getName());
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.WORKER_BLOCK_REMOVER_TRY_REMOVE_BLOCKS_SIZE.getName(), mBlocksToRemove::size);
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.WORKER_BLOCK_REMOVER_REMOVING_BLOCKS_SIZE.getName(), mRemovingBlocks::size);

    mRemoverPool = Executors.newFixedThreadPool(mPoolSize,
        ThreadFactoryUtils.build("block-removal-service-%d", true));
    for (int i = 0; i < mPoolSize; i++) {
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
    @Override
    public void run() {
      String threadName = Thread.currentThread().getName();
      while (true) {
        Long blockToBeRemoved = null;
        try {
          blockToBeRemoved = mBlocksToRemove.take();
          mTryRemoveCount.inc();
          mBlockWorker.removeBlock(Sessions.MASTER_COMMAND_SESSION_ID, blockToBeRemoved);
          mRemovedCount.inc();
          LOG.debug("Block {} is removed in thread {}.", blockToBeRemoved, threadName);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          // Only log warning if interrupted not due to a shutdown.
          if (!mShutdown) {
            LOG.warn("{} got interrupted while it was cleaning block {}.", threadName,
                blockToBeRemoved);
          }
          break;
        } catch (BlockDoesNotExistException e) {
          // Ignore the case when block is already removed. This could happen when master is asking
          // worker to remove blocks based on stale information
        } catch (Exception e) {
          LOG.warn("Failed to remove block {} instructed by master. This is best-effort and "
              + "will be tried later. threadName {}, error {}", blockToBeRemoved,
              threadName, e.getMessage());
        } finally {
          if (blockToBeRemoved != null) {
            mRemovingBlocks.remove(blockToBeRemoved);
          }
        }
      }
    }
  }
}
