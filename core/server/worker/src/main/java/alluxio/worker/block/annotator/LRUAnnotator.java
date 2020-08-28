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

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link BlockAnnotator} implementation of LRU scheme.
 */
public class LRUAnnotator implements BlockAnnotator<LRUAnnotator.LRUSortedField> {
  private static final Logger LOG = LoggerFactory.getLogger(LRUAnnotator.class);

  private AtomicLong mLRUClock;

  /**
   * Creates a new LRU annotator.
   */
  public LRUAnnotator() {
    mLRUClock = new AtomicLong(0);
  }

  @Override
  public BlockSortedField updateSortedField(long blockId, LRUSortedField oldValue) {
    long clockValue = mLRUClock.incrementAndGet();
    if (LOG.isDebugEnabled()) {
      LOG.debug("LRU update for Block: {}. Clock: {}", blockId, clockValue);
    }
    return new LRUSortedField(clockValue);
  }

  @Override
  public void updateSortedFields(List<Pair<Long, LRUSortedField>> blocks) {
    long currentClock = mLRUClock.get();
    for (Pair<Long, LRUSortedField> blockField : blocks) {
      blockField.setSecond(new LRUSortedField(currentClock));
    }
  }

  /**
   * LRU is an online scheme.
   *
   * @return {@code true}
   */
  @Override
  public boolean isOnlineSorter() {
    return true;
  }

  /**
   * Sorted-field for LRU.
   */
  protected class LRUSortedField implements BlockSortedField {
    private Long mClockValue;

    private LRUSortedField(long clockValue) {
      mClockValue = clockValue;
    }

    @Override
    public int compareTo(BlockSortedField o) {
      Preconditions.checkState(o instanceof LRUSortedField);
      return mClockValue.compareTo(((LRUSortedField) o).mClockValue);
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || !(o instanceof LRUSortedField)) {
        return false;
      }
      return Long.compare(mClockValue, ((LRUSortedField) o).mClockValue) == 0;
    }

    @Override
    public int hashCode() {
      return mClockValue.hashCode();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("Clock", mClockValue)
          .toString();
    }
  }
}
