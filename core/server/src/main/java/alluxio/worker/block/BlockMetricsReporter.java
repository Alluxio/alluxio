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
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.worker.WorkerSource;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class listens on block events and increases the metrics counters.
 */
@NotThreadSafe
public final class BlockMetricsReporter extends AbstractBlockStoreEventListener {
  private final WorkerSource mWorkerSource;
  private final StorageTierAssoc mStorageTierAssoc;

  /**
   * Creates a new instance of {@link BlockMetricsReporter}.
   *
   * @param workerSource a worker source handle
   */
  public BlockMetricsReporter(WorkerSource workerSource) {
    mWorkerSource = workerSource;
    mStorageTierAssoc = new WorkerStorageTierAssoc();
  }

  @Override
  public void onAccessBlock(long sessionId, long blockId) {
    mWorkerSource.incBlocksAccessed(1);
  }

  @Override
  public void onMoveBlockByClient(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    int oldTierOrdinal = mStorageTierAssoc.getOrdinal(oldLocation.tierAlias());
    int newTierOrdinal = mStorageTierAssoc.getOrdinal(newLocation.tierAlias());
    if (newTierOrdinal == 0 && oldTierOrdinal != newTierOrdinal) {
      mWorkerSource.incBlocksPromoted(1);
    }
  }

  @Override
  public void onRemoveBlockByClient(long sessionId, long blockId) {
    mWorkerSource.incBlocksDeleted(1);
  }

  @Override
  public void onMoveBlockByWorker(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    int oldTierOrdinal = mStorageTierAssoc.getOrdinal(oldLocation.tierAlias());
    int newTierOrdinal = mStorageTierAssoc.getOrdinal(newLocation.tierAlias());
    if (newTierOrdinal == 0 && oldTierOrdinal != newTierOrdinal) {
      mWorkerSource.incBlocksPromoted(1);
    }
  }

  @Override
  public void onRemoveBlockByWorker(long sessionId, long blockId) {
    mWorkerSource.incBlocksEvicted(1);
  }

  @Override
  public void onAbortBlock(long sessionId, long blockId) {
    mWorkerSource.incBlocksCanceled(1);
  }

  /**
   * Updates session metrics from the heartbeat from a client.
   *
   * @param metrics the set of metrics the client has gathered since the last heartbeat
   */
  public void updateClientMetrics(List<Long> metrics) {
    if (metrics != null && !metrics.isEmpty() && metrics.get(Constants.CLIENT_METRICS_VERSION_INDEX)
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
      mWorkerSource.incSeeksLocal(metrics.get(Constants.SEEKS_LOCAL_INDEX));
      mWorkerSource.incSeeksRemote(metrics.get(Constants.SEEKS_REMOTE_INDEX));
    }
  }
}
