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

package alluxio.client.file.cache;

import com.google.common.base.MoreObjects;

import java.util.Objects;
import java.util.Optional;

/**
 * Immutable view of cache usage stats.
 */
public interface CacheUsageView {
  /**
   * Bytes currently used by the cache.
   * This includes both evictable and inevitable pages.
   *
   * @return number of bytes being used
   */
  long used();

  /**
   * Bytes that are available for caching.
   * This includes free space, as well as the space occupied by evictable pages.
   *
   * @return number of bytes that can be used for caching
   */
  long available();

  /**
   * Total capacity of the cache.
   * @return total number of bytes the cache is allowed to use
   */
  long capacity();

  /**
   * Immutable holder of cache stats.
   */
  final class ImmutableCacheUsageView implements CacheUsage {
    private final long mUsed;
    private final long mAvailable;
    private final long mCapacity;

    /**
     * Constructor.
     *
     * @param used used
     * @param available available
     * @param capacity capacity
     */
    public ImmutableCacheUsageView(long used, long available, long capacity) {
      mUsed = used;
      mAvailable = available;
      mCapacity = capacity;
    }

    @Override
    public long used() {
      return mUsed;
    }

    @Override
    public long available() {
      return mAvailable;
    }

    @Override
    public long capacity() {
      return mCapacity;
    }

    @Override
    public CacheUsageView snapshot() {
      return this;
    }

    @Override
    public Optional<CacheUsage> partitionedBy(PartitionDescriptor<?> partition) {
      return Optional.empty();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ImmutableCacheUsageView that = (ImmutableCacheUsageView) o;
      return mUsed == that.mUsed
          && mAvailable == that.mAvailable
          && mCapacity == that.mCapacity;
    }

    @Override
    public int hashCode() {
      return Objects.hash(mUsed, mAvailable, mCapacity);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("used", mUsed)
          .add("available", mAvailable)
          .add("capacity", mCapacity)
          .toString();
    }
  }
}
