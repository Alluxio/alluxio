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

package alluxio.client.file.cache.cuckoofilter;

import alluxio.client.quota.CacheScope;

/**
 * This interface represents a clock cuckoo filter that supports Put/Get/Delete operations.
 *
 * <p>
 * Cuckoo filter is an approximate structure for membership query. Apart from Put and Get, it also
 * supports deleting items from filter, which is not allowed in bloom filter. See "Cuckoo Filter:
 * Practically Better Than Bloom" by Fan et al. for more detailed description.
 *
 * <p>
 * Clock cuckoo filter is an extension of basic cuckoo filter. Specifically, it has the following
 * features: 1) sliding window model: it will automatically delete stale items, which means they are
 * not accessed over a recent interval; 2) size estimation: it can not only report the approximate
 * number of unique items in it, but also the total size of those items; 3) hierarchy: we assume
 * items are organized in a hierarchical level structure, and it can report the statistics of a
 * specific level.
 *
 * @param <T> the type of instances that the {@code ClockCuckooFilter} accepts
 */
public interface ClockCuckooFilter<T> {
  /**
   * Insert an item into cuckoo filter.
   *
   * @param item the object to be inserted
   * @param size the size of this item
   * @param scopeInfo the scope this item belongs to
   * @return true if inserted successfully; false otherwise
   */
  boolean put(T item, int size, CacheScope scopeInfo);

  /**
   * Check whether an item is in cuckoo filter (and reset its clock to MAX) or not.
   *
   * @param item the item to be checked
   * @return true if item is in cuckoo filter; false otherwise
   */
  boolean mightContainAndResetClock(T item);

  /**
   * Check whether an item is in cuckoo filter or not. This method will not change item's clock.
   *
   * @param item the item to be checked
   * @return true if item is in cuckoo filter; false otherwise
   */
  boolean mightContain(T item);

  /**
   * Delete an item from cuckoo filter.
   *
   * @param item the item to be deleted
   * @return true if the item is deleted; false otherwise
   */
  boolean delete(T item);

  /**
   * A thread-safe method to check aging progress of each segment. Should be called on each T/(2^C),
   * where T is the window size and C is the bits number of the CLOCK field.
   */
  void aging();

  /**
   * @return the probability that {@linkplain #mightContain(Object)} will erroneously return {@code
   * true} for an object that has not actually been put in the {@code ConcurrentCuckooFilter}.
   */
  double expectedFpp();

  /**
   * @return the number of items in this cuckoo filter
   */
  long approximateElementCount();

  /**
   * @param scopeInfo the scope to be queried
   * @return the number of items of specified scope in this cuckoo filter
   */
  long approximateElementCount(CacheScope scopeInfo);

  /**
   * @return the size of items in this cuckoo filter
   */
  long approximateElementSize();

  /**
   * @param scopeInfo the scope to be queried
   * @return the size of items of specified scope in this cuckoo filter
   */
  long approximateElementSize(CacheScope scopeInfo);
}
