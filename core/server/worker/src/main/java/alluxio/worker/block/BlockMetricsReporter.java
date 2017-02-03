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
import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.Counter;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class listens on block events and increases the metrics counters.
 */
@ThreadSafe
public final class BlockMetricsReporter extends AbstractBlockStoreEventListener {
  private final StorageTierAssoc mStorageTierAssoc;

  private static final Counter BLOCKS_ACCESSED = MetricsSystem.workerCounter("BlocksAccessed");
  private static final Counter BLOCKS_PROMOTED = MetricsSystem.workerCounter("BlocksPromoted");
  private static final Counter BLOCKS_DELETED = MetricsSystem.workerCounter("BlocksDeleted");
  private static final Counter BLOCKS_EVICTED = MetricsSystem.workerCounter("BlocksEvicted");
  private static final Counter BLOCKS_CANCELLED = MetricsSystem.workerCounter("BlocksCanceled");

  /**
   * Creates a new instance of {@link BlockMetricsReporter}.
   */
  public BlockMetricsReporter() {
    mStorageTierAssoc = new WorkerStorageTierAssoc();
  }

  @Override
  public void onAccessBlock(long sessionId, long blockId) {
    BLOCKS_ACCESSED.inc();
  }

  @Override
  public void onMoveBlockByClient(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    int oldTierOrdinal = mStorageTierAssoc.getOrdinal(oldLocation.tierAlias());
    int newTierOrdinal = mStorageTierAssoc.getOrdinal(newLocation.tierAlias());
    if (newTierOrdinal == 0 && oldTierOrdinal != newTierOrdinal) {
      BLOCKS_PROMOTED.inc();
    }
  }

  @Override
  public void onRemoveBlockByClient(long sessionId, long blockId) {
    BLOCKS_DELETED.inc();
  }

  @Override
  public void onMoveBlockByWorker(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    int oldTierOrdinal = mStorageTierAssoc.getOrdinal(oldLocation.tierAlias());
    int newTierOrdinal = mStorageTierAssoc.getOrdinal(newLocation.tierAlias());
    if (newTierOrdinal == 0 && oldTierOrdinal != newTierOrdinal) {
      BLOCKS_PROMOTED.inc();
    }
  }

  @Override
  public void onRemoveBlockByWorker(long sessionId, long blockId) {
    BLOCKS_EVICTED.inc();
  }

  @Override
  public void onAbortBlock(long sessionId, long blockId) {
    BLOCKS_CANCELLED.inc();
  }
}
