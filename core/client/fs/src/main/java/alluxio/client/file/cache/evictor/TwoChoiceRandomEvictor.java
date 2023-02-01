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

package alluxio.client.file.cache.evictor;

import alluxio.client.file.cache.PageId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Two Choice Random client-side cache eviction policy.
 * It selects two random page IDs and evicts the one least-recently used.
 */
@ThreadSafe
public class TwoChoiceRandomEvictor implements CacheEvictor {
  private final Map<PageId, Long> mCache = Collections.synchronizedMap(new HashMap<>());

  /**
   * Constructor.
   * @param options
   */
  public TwoChoiceRandomEvictor(CacheEvictorOptions options) {
  }

  @Override
  public void updateOnGet(PageId pageId) {
    mCache.put(pageId, Instant.now().toEpochMilli());
  }

  @Override
  public void updateOnPut(PageId pageId) {
    mCache.put(pageId, Instant.now().toEpochMilli());
  }

  @Override
  public void updateOnDelete(PageId pageId) {
    mCache.remove(pageId);
  }

  @Nullable
  @Override
  public PageId evict() {
    synchronized (mCache) {
      if (mCache.isEmpty()) {
        return null;
      }

      // TODO(chunxu): improve the performance here
      List<PageId> keys = new ArrayList<>(mCache.keySet());
      Random rand = new Random();
      PageId key1 = keys.get(rand.nextInt(keys.size()));
      PageId key2 = keys.get(rand.nextInt(keys.size()));
      if (mCache.get(key1) < mCache.get(key2)) {
        return key1;
      }
      return key2;
    }
  }

  @Nullable
  @Override
  public PageId evictMatching(Predicate<PageId> criterion) {
    synchronized (mCache) {
      for (PageId candidate : mCache.keySet()) {
        if (criterion.test(candidate)) {
          return candidate;
        }
      }
      return null;
    }
  }

  @Override
  public void reset() {
    mCache.clear();
  }
}
