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
 * Options used to instantiate the {@link LocalPageStore}.
 */
public class MemoryPageStoreOptions extends PageStoreOptions {

  /**
   * Creates a new instance of {@link MemoryPageStoreOptions}.
   */
  public MemoryPageStoreOptions() {
  }

  @Override
  public PageStoreType getType() {
    return PageStoreType.MEMORY;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("AlluxioVersion", mAlluxioVersion)
        .add("CacheSize", mCacheSize)
        .add("PageSize", mPageSize)
        .toString();
  }
}
