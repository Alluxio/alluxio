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

import java.util.LinkedList;
import java.util.function.Predicate;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Implementation of Evictor using FIFO eviction policy.
 */
@NotThreadSafe
public class FIFOCacheEvictor implements CacheEvictor {
  private final LinkedList<PageId> mQueue = new LinkedList<>();

  /**
   * Required constructor.
   *
   * @param options
   */
  public FIFOCacheEvictor(CacheEvictorOptions options) {}

  @Override
  public void updateOnGet(PageId pageId) {
    // noop
  }

  @Override
  public void updateOnPut(PageId pageId) {
    mQueue.add(pageId);
  }

  @Override
  public void updateOnDelete(PageId pageId) {
    int idx = mQueue.indexOf(pageId);
    if (idx >= 0) {
      mQueue.remove(idx);
    }
  }

  @Override
  public PageId evict() {
    return mQueue.peek();
  }

  @Nullable
  @Override
  public PageId evictMatching(Predicate<PageId> criterion) {
    for (PageId candidate : mQueue) {
      if (criterion.test(candidate)) {
        return candidate;
      }
    }
    return null;
  }

  @Override
  public void reset() {
    mQueue.clear();
  }
}
