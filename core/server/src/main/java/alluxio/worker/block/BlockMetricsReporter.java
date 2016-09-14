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
import alluxio.metrics.MetricsSystem;
import alluxio.worker.WorkerSource;

import com.codahale.metrics.Counter;
import org.omg.CORBA.PRIVATE_MEMBER;

import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class listens on block events and increases the metrics counters.
 */
@ThreadSafe
public final class BlockMetricsReporter extends AbstractBlockStoreEventListener {
  private final StorageTierAssoc mStorageTierAssoc;

  public static final String BLOCKS_ACCESSED = "BLOCKS_ACCESSED";
  public static final String BLOCKS_PROMOTED = "BLOCKS_PROMOTED";
  public static final String BLOCKS_DELETED = "BLOCKS_DELETED";
  public static final String BLOCKS_EVICTED = "BLOCKS_EVICTED";
  public static final String BLOCKS_CANCELLED = "BLOCKS_CANCELLED";

  private static final Counter BLOCKS_ACCESSED_COUNTER =
      MetricsSystem.workerCounter(BLOCKS_ACCESSED);
  private static final Counter BLOCKS_PROMOTED_COUNTER =
      MetricsSystem.workerCounter(BLOCKS_PROMOTED);
  private static final Counter BLOCKS_DELETED_COUNTER =
      MetricsSystem.workerCounter(BLOCKS_DELETED);
  private static final Counter BLOCKS_EVICTED_COUNTER =
      MetricsSystem.workerCounter(BLOCKS_EVICTED);
  private static final Counter BLOCKS_CANCELLED_COUNTER =
      MetricsSystem.workerCounter(BLOCKS_CANCELLED);

  /**
   * Creates a new instance of {@link BlockMetricsReporter}.
   */
  public BlockMetricsReporter() {
    mStorageTierAssoc = new WorkerStorageTierAssoc();
  }

  @Override
  public void onAccessBlock(long sessionId, long blockId) {
    BLOCKS_ACCESSED_COUNTER.inc();
  }

  @Override
  public void onMoveBlockByClient(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    int oldTierOrdinal = mStorageTierAssoc.getOrdinal(oldLocation.tierAlias());
    int newTierOrdinal = mStorageTierAssoc.getOrdinal(newLocation.tierAlias());
    if (newTierOrdinal == 0 && oldTierOrdinal != newTierOrdinal) {
      BLOCKS_PROMOTED_COUNTER.inc();
    }
  }

  @Override
  public void onRemoveBlockByClient(long sessionId, long blockId) {
    BLOCKS_DELETED_COUNTER.inc();
  }

  @Override
  public void onMoveBlockByWorker(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    int oldTierOrdinal = mStorageTierAssoc.getOrdinal(oldLocation.tierAlias());
    int newTierOrdinal = mStorageTierAssoc.getOrdinal(newLocation.tierAlias());
    if (newTierOrdinal == 0 && oldTierOrdinal != newTierOrdinal) {
      BLOCKS_PROMOTED_COUNTER.inc();
    }
  }

  @Override
  public void onRemoveBlockByWorker(long sessionId, long blockId) {
    BLOCKS_EVICTED_COUNTER.inc();
  }

  @Override
  public void onAbortBlock(long sessionId, long blockId) {
    BLOCKS_CANCELLED_COUNTER.inc();
  }
}
