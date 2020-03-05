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

import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Concurrent set implementation for storing block Ids in sorted form.
 *
 * @param <T> Field type used for sorting block Ids
 */
public class SortedBlockSet<T extends BlockSortedField> {
  /** Used for ordering among block Ids with equal sort values. */
  private final AtomicLong mChangeIndex = new AtomicLong(0);

  /** Underlying concurrent set. */
  private final ConcurrentSkipListSet<SortedBlockSetEntry<T>> mSortedSet;
  /** Used to overcome equality side effect of java's sorted sets. */
  private final ConcurrentHashMap<Long, Pair<Long, T>> mLastSortId;

  /**
   * Creates a new sorted block set.
   */
  public SortedBlockSet() {
    mSortedSet = new ConcurrentSkipListSet<>();
    mLastSortId = new ConcurrentHashMap<>();
  }

  /**
   * Used to get the current sort field for a block Id.
   *
   * @param blockId block Id
   * @return the current sort field
   */
  public T getSortField(long blockId) {
    Pair<Long, T> sortId = mLastSortId.compute(blockId, (k, v) -> v);
    return (sortId != null) ? sortId.getSecond() : null;
  }

  /**
   * Updates or inserts the collection with new block Id.
   *
   * @param blockId block id
   * @param sortedField sorted field for the block id
   */
  public void put(long blockId, T sortedField) {
    mLastSortId.compute(blockId, (k, v) -> {
      if (v != null) {
        mSortedSet.remove(new SortedBlockSetEntry<>(blockId, v.getFirst(), v.getSecond()));
      }

      Pair<Long, T> newSortId = new Pair<>(mChangeIndex.incrementAndGet(), sortedField);
      mSortedSet
          .add(new SortedBlockSetEntry<>(blockId, newSortId.getFirst(), newSortId.getSecond()));
      return newSortId;
    });
  }

  /**
   * Removes a block id from the collection.
   *
   * @param blockId block id
   */
  public void remove(long blockId) {
    mLastSortId.compute(blockId, (k, v) -> {
      if (v != null) {
        mSortedSet.remove(new SortedBlockSetEntry<>(blockId, v.getFirst(), v.getSecond()));
      }
      return null;
    });
  }

  /**
   * @return the size of the collection
   */
  public int size() {
    return mSortedSet.size();
  }

  /**
   * @return an ascending iterator of "<BlockId,SortedField>" pairs
   */
  public Iterator<Pair<Long, T>> getAscendingIterator() {
    return Iterators.transform(mSortedSet.iterator(),
        (e) -> new Pair<>(e.mBlockId, e.mSortedField));
  }

  /**
   * @return a descending iterator of "<BlockId,SortedField>" pairs
   */
  public Iterator<Pair<Long, T>> getDescendingIterator() {
    return Iterators.transform(mSortedSet.descendingIterator(),
        (e) -> new Pair<>(e.mBlockId, e.mSortedField));
  }

  /**
   * An entry that is stored in an internal set that does its sorting/equality
   * based on the given sorted-field.
   *
   * @param <T> type of the sorted field
   */
  class SortedBlockSetEntry<T extends Comparable> implements Comparable<SortedBlockSetEntry> {
    private long mBlockId;
    private T mSortedField;
    private long mChangeIndex;

    public SortedBlockSetEntry(long blockId, long changeIndex, T sortId) {
      mBlockId = blockId;
      mChangeIndex = changeIndex;
      mSortedField = sortId;
    }

    @Override
    public int compareTo(SortedBlockSetEntry o) {
      int sortRes = mSortedField.compareTo(o.mSortedField);
      if (sortRes == 0) {
        return Long.compare(mChangeIndex, o.mChangeIndex);
      } else {
        return sortRes;
      }
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SortedBlockSetEntry)) {
        return false;
      }
      SortedBlockSetEntry other = (SortedBlockSetEntry) o;
      return Objects.equals(mBlockId, other.mBlockId)
          && Objects.equals(mSortedField, other.mSortedField)
          && Objects.equals(mChangeIndex, other.mChangeIndex);
    }

    @Override
    public int hashCode() {
      return Objects.hash(mBlockId, mSortedField);
    }
  }
}
