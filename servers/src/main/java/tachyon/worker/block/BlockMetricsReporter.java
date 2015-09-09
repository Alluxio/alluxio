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

import java.util.List;

import tachyon.Constants;
import tachyon.worker.WorkerSource;

/**
 * This class listens on block events and increases the metrics counters.
 */
public final class BlockMetricsReporter extends BlockStoreEventListenerBase {
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
    int oldTier = oldLocation.tierLevel();
    int newTier = newLocation.tierLevel();
    if (newTier == 0 && oldTier != newTier) {
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
    int oldTier = oldLocation.tierLevel();
    int newTier = newLocation.tierLevel();
    if (newTier == 0 && oldTier != newTier) {
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

  /**
   * Updates user metrics from the heartbeat from a client.
   *
   * @param metrics The set of metrics the client has gathered since the last heartbeat
   */
  public void updateClientMetrics(List<Long> metrics) {
    if (null != metrics && !metrics.isEmpty() && metrics.get(Constants.CLIENT_METRICS_VERSION_INDEX)
        == Constants.CLIENT_METRICS_VERSION) {
      mWorkerSource.incBlocksReadLocal(metrics.get(Constants.BLOCKS_READ_LOCAL_INDEX));
      mWorkerSource.incBlocksReadRemote(metrics.get(Constants.BLOCKS_READ_REMOTE_INDEX));
      mWorkerSource.incBlocksWrittenLocal(metrics.get(Constants.BLOCKS_WRITTEN_LOCAL_INDEX));
      mWorkerSource.incBlocksWrittenRemote(metrics.get(Constants.BLOCKS_WRITTEN_REMOTE_INDEX));
      mWorkerSource.incBytesReadLocal(metrics.get(Constants.BYTES_READ_LOCAL_INDEX));
      mWorkerSource.incBytesReadRemote(metrics.get(Constants.BYTES_READ_REMOTE_INDEX));
      mWorkerSource.incBytesReadUfs(metrics.get(Constants.BYTES_READ_UFS_INDEX));
      mWorkerSource.incBytesWrittenLocal(metrics.get(Constants.BYTES_WRITTEN_LOCAL_INDEX));
      mWorkerSource.incBytesWrittenRemote(metrics.get(Constants.BYTES_WRITTEN_REMOTE_INDEX));
      mWorkerSource.incBytesWrittenUfs(metrics.get(Constants.BYTES_WRITTEN_UFS_INDEX));
    }
  }
}
