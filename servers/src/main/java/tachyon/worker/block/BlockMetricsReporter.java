/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block;

import tachyon.worker.BlockStoreLocation;
import tachyon.worker.WorkerSource;

/**
 * This class listens on block events and increases the metrics counters.
 */
public class BlockMetricsReporter implements BlockMetaEventListener, BlockAccessEventListener {
  private final WorkerSource mWorkerSource;

  public BlockMetricsReporter(WorkerSource workerSource) {
    mWorkerSource = workerSource;
  }

  @Override
  public void onAccessBlock(long userId, long blockId) {
    mWorkerSource.incBlocksAccessed();
  }

  @Override
  public void preCommitBlock(long userId, long blockId, BlockStoreLocation location) {
    // Do nothing
  }

  @Override
  public void postCommitBlock(long userId, long blockId, BlockStoreLocation location) {
    // Do nothing
  }

  @Override
  public void preMoveBlockByClient(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    // Do nothing
  }

  @Override
  public void postMoveBlockByClient(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    int oldTier = oldLocation.tierAlias();
    int newTier = newLocation.tierAlias();
    if (newTier == 1 && oldTier != newTier) {
      mWorkerSource.incBlocksPromoted();
    }
  }

  @Override
  public void preRemoveBlockByClient(long userId, long blockId) {
    // Do nothing
  }

  @Override
  public void postRemoveBlockByClient(long userId, long blockId) {
    mWorkerSource.incBlocksDeleted();
  }

  @Override
  public void preMoveBlockByWorker(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    // Do nothing
  }

  @Override
  public void postMoveBlockByWorker(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    int oldTier = oldLocation.tierAlias();
    int newTier = newLocation.tierAlias();
    if (newTier == 1 && oldTier != newTier) {
      mWorkerSource.incBlocksPromoted();
    }
  }

  @Override
  public void preRemoveBlockByWorker(long userId, long blockId) {
    // Do nothing
  }

  @Override
  public void postRemoveBlockByWorker(long userId, long blockId) {
    mWorkerSource.incBlocksEvicted();
  }

  @Override
  public void preAbortBlock(long userId, long blockId) {
    // Do nothing
  }

  @Override
  public void postAbortBlock(long userId, long blockId) {
    mWorkerSource.incBlocksCanceled();
  }
}
