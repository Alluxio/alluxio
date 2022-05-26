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

package alluxio.client.file.cache.store;

import com.google.common.base.MoreObjects;

/**
 * Options used to instantiate the {@link MemoryPageStore}.
 */
public class MemoryPageStoreOptions extends PageStoreOptions {
  // We assume there will be some overhead using ByteBuffer as a page store,
  // i.e., with 1GB space allocated, we
  // expect no more than 1024MB / (1 + BUFF_MEMORY_OVERHEAD_RATIO) logical data stored
  private static final double MEMORY_OVERHEAD_RATIO = 0.1;

  /**
   * Creates a new instance of {@link MemoryPageStoreOptions}.
   */
  public MemoryPageStoreOptions() {
    mOverheadRatio = MemoryPageStoreOptions.MEMORY_OVERHEAD_RATIO;
  }

  @Override
  public PageStoreType getType() {
    return PageStoreType.MEM;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("AlluxioVersion", mAlluxioVersion)
        .add("CacheSize", mCacheSize)
        .add("OverheadRatio", mOverheadRatio)
        .add("PageSize", mPageSize)
        .add("TimeoutDuration", mTimeoutDuration)
        .add("TimeoutThreads", mTimeoutThreads)
        .toString();
  }
}
