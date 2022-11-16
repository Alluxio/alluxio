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

package alluxio.worker.block.annotator;

import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link BlockAnnotator} implementation of ReplicaNum based scheme.
 */
public class ReplicaBasedAnnotator implements
        BlockAnnotator<ReplicaBasedAnnotator.ReplicaSortedField> {
  private static final Logger LOG = LoggerFactory.getLogger(LRUAnnotator.class);

  private final AtomicLong mLRUClock;

  private static final double LRU_RATIO;
  private static final double REPLICA_RATIO;

  static {
    LRU_RATIO = Configuration.getDouble(
              PropertyKey.WORKER_BLOCK_ANNOTATOR_REPLICA_LRU_RATIO);
    REPLICA_RATIO = Configuration.getDouble(
              PropertyKey.WORKER_BLOCK_ANNOTATOR_REPLICA_REPLICA_RATIO);
  }

  /**
   * Creates a new ReplicaBased annotator.
   * */
  public ReplicaBasedAnnotator() {
    mLRUClock = new AtomicLong(0);
  }

  @Override
  public BlockSortedField updateSortedField(long blockId, ReplicaSortedField oldValue) {
    long clockValue = mLRUClock.incrementAndGet();
    long replicaNum =  (oldValue != null) ? oldValue.mReplicaNum : 0;
    if (LOG.isDebugEnabled()) {
      LOG.debug("ReplicaBased update for Block: {}. Clock: {}", blockId, clockValue);
    }
    return new ReplicaSortedField(clockValue, replicaNum);
  }

  @Override
  public BlockSortedField updateSortedFieldReplica(long blockId, ReplicaSortedField oldValue,
                                                   Long value) {
    long clockValue = mLRUClock.incrementAndGet();
    Long replicaNum  = (oldValue != null) ? oldValue.mReplicaNum : 0;
    if (LOG.isDebugEnabled()) {
      LOG.debug("ReplicaBased update for Block: {}. Clock: {}", blockId, clockValue);
    }
    return new ReplicaSortedField(clockValue, replicaNum + value);
  }

  @Override
  public void updateSortedFields(List<Pair<Long, ReplicaSortedField>> blocks) {
    long currentClock = mLRUClock.get();
    for (Pair<Long, ReplicaSortedField> blockField : blocks) {
      long replicaNum =  (blockField.getSecond() != null) ? blockField.getSecond().mReplicaNum : 0;
      blockField.setSecond(new ReplicaSortedField(currentClock, replicaNum));
    }
  }

  /**
   * ReplicaNumber is an online scheme.
   *
   * @return {@code true}
   */
  @Override
  public boolean isOnlineSorter() {
    return true;
  }

  /**
   * Sorted-field for ReplicaBasedAnnotator.
    */
  protected class ReplicaSortedField implements BlockSortedField {
    private final Long mClockValue;
    private final Long mReplicaNum;
    private final double mValue;

    private ReplicaSortedField(long clockValue, long ReplicaNum) {
      mClockValue = clockValue;
      mReplicaNum = ReplicaNum;
      mValue = clockValue * LRU_RATIO - ReplicaNum * REPLICA_RATIO;
    }

    @Override
    public int compareTo(BlockSortedField o) {
      Preconditions.checkState(o instanceof ReplicaSortedField);
      return Double.compare(mValue, ((ReplicaSortedField) o).mValue);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof ReplicaSortedField)) {
        return false;
      }
      return Double.compare(mValue, ((ReplicaSortedField) o).mValue) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hash(mValue);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                .add("Clock", mClockValue)
                .add("Replica", mReplicaNum)
                .toString();
    }
  }
}
