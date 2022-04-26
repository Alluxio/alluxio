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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link BlockAnnotator} implementation of LRFU scheme.
 */
public class LRFUAnnotator implements BlockAnnotator<LRFUAnnotator.LRFUSortedField> {
  private static final Logger LOG = LoggerFactory.getLogger(LRFUAnnotator.class);

  /** LRU logical clock. */
  private final AtomicLong mLRUClock = new AtomicLong();

  /** In the range of [0, 1]. Closer to 0, LRFU closer to LFU. Closer to 1, LRFU closer to LRU. */
  private static final double STEP_FACTOR;
  /** The attenuation factor is in the range of [2, INF]. */
  private static final double ATTENUATION_FACTOR;

  static {
    STEP_FACTOR = ServerConfiguration.getDouble(
        PropertyKey.WORKER_BLOCK_ANNOTATOR_LRFU_STEP_FACTOR);
    ATTENUATION_FACTOR = ServerConfiguration.getDouble(
        PropertyKey.WORKER_BLOCK_ANNOTATOR_LRFU_ATTENUATION_FACTOR);
  }

  @Override
  public BlockSortedField updateSortedField(long blockId, LRFUSortedField oldValue) {
    return getNewSortedField(blockId, oldValue, mLRUClock.incrementAndGet());
  }

  @Override
  public void updateSortedFields(List<Pair<Long, LRFUSortedField>> blockList) {
    // Grab the current logical clock, for updating the given entries under.
    long clockValue = mLRUClock.get();
    for (Pair<Long, LRFUSortedField> blockField : blockList) {
      blockField.setSecond(getNewSortedField(
          blockField.getFirst(), blockField.getSecond(), clockValue));
    }
  }

  /**
   * LRFU is an offline scheme.
   *
   * @return {@code false}
   */
  @Override
  public boolean isOnlineSorter() {
    return false;
  }

  private LRFUSortedField getNewSortedField(long blockId, LRFUSortedField oldValue, long clock) {
    double crfValue = (oldValue != null) ? oldValue.mCrfValue : 1.0;
    if (oldValue != null && clock != oldValue.mClockValue) {
      // CRF(currentLogicTime)=CRF(lastUpdateTime)*F(currentLogicTime-lastUpdateTime)+F(0)
      crfValue = oldValue.mCrfValue * calculateAccessWeight(clock - oldValue.mClockValue) + 1.0;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("LRFU update for Block: {}. Clock:{}, CRF: {}", blockId, clock, crfValue);
    }

    return new LRFUSortedField(clock, crfValue);
  }

  private double calculateAccessWeight(long logicTimeInterval) {
    return Math.pow(1.0 / ATTENUATION_FACTOR, logicTimeInterval * STEP_FACTOR);
  }

  /**
   * Sorted-field for LRFU.
   */
  protected static class LRFUSortedField implements BlockSortedField {
    private final long mClockValue;
    private final double mCrfValue;

    private LRFUSortedField(long clockValue, double crfValue) {
      mClockValue = clockValue;
      mCrfValue = crfValue;
    }

    @Override
    public int compareTo(BlockSortedField o) {
      Preconditions.checkState(o instanceof LRFUSortedField);
      return Double.compare(mCrfValue, ((LRFUSortedField) o).mCrfValue);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof LRFUSortedField)) {
        return false;
      }
      return Double.compare(mCrfValue, ((LRFUSortedField) o).mCrfValue) == 0;
    }

    @Override
    public int hashCode() {
      return Objects.hash(mCrfValue);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("Clock",  mClockValue)
          .add("CRF", mCrfValue)
          .toString();
    }
  }
}
