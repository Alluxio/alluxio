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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * LRU client-side cache eviction policy.
 */
public class LRUCacheEvictor implements CacheEvictor {
  private static final int LINKED_HASH_MAP_INIT_CAPACITY = 200;
  private static final float LINKED_HASH_MAP_INIT_LOAD_FACTOR = 0.75f;
  private static final boolean LINKED_HASH_MAP_ACCESS_ORDERED = true;
  private static final boolean UNUSED_MAP_VALUE = true;

  // TODO(feng): unify with worker side evictor
  protected Map<PageId, Boolean> mLRUCache =
      Collections.synchronizedMap(new LinkedHashMap<>(LINKED_HASH_MAP_INIT_CAPACITY,
          LINKED_HASH_MAP_INIT_LOAD_FACTOR, LINKED_HASH_MAP_ACCESS_ORDERED));

  @Override
  public void updateOnGet(PageId pageId) {
    mLRUCache.put(pageId, UNUSED_MAP_VALUE);
  }

  @Override
  public void updateOnPut(PageId pageId) {
    mLRUCache.put(pageId, UNUSED_MAP_VALUE);
  }

  @Override
  public void updateOnDelete(PageId pageId) {
    mLRUCache.remove(pageId, UNUSED_MAP_VALUE);
  }

  @Override
  public PageId evict() {
    return mLRUCache.keySet().iterator().next();
  }
}
