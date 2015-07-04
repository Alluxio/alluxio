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

import tachyon.worker.WorkerSource;

/**
 * This class listens on block events and increases the metrics counters.
 */
public class BlockMetricsReporter extends BlockStoreEventListenerBase {
  private final WorkerSource mWorkerSource;

  public BlockMetricsReporter(WorkerSource workerSource) {
    mWorkerSource = workerSource;
  }

  @Override
  public void onAccessBlock(long userId, long blockId) {
    mWorkerSource.incBlocksAccessed();
  }

  @Override
  public void onMoveBlockByClient(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    int oldTier = oldLocation.tierAlias();
    int newTier = newLocation.tierAlias();
    if (newTier == 1 && oldTier != newTier) {
      mWorkerSource.incBlocksPromoted();
    }
  }

  @Override
  public void onRemoveBlockByClient(long userId, long blockId) {
    mWorkerSource.incBlocksDeleted();
  }

  @Override
  public void onMoveBlockByWorker(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    int oldTier = oldLocation.tierAlias();
    int newTier = newLocation.tierAlias();
    if (newTier == 1 && oldTier != newTier) {
      mWorkerSource.incBlocksPromoted();
    }
  }

  @Override
  public void onRemoveBlockByWorker(long userId, long blockId) {
    mWorkerSource.incBlocksEvicted();
  }

  @Override
  public void onAbortBlock(long userId, long blockId) {
    mWorkerSource.incBlocksCanceled();
  }
}
