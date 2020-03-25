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

import alluxio.client.file.cache.CacheEvictor;
import alluxio.client.file.cache.PageId;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * LFU client-side cache eviction policy.
 */
@ThreadSafe
public class LFUCacheEvictor implements CacheEvictor {
  private static final int LINKED_HASH_MAP_INIT_CAPACITY = 200;
  private static final float LINKED_HASH_MAP_INIT_LOAD_FACTOR = 0.75f;

  private final Map<PageId, Integer> mPageMap = new HashMap<>(
      LINKED_HASH_MAP_INIT_CAPACITY, LINKED_HASH_MAP_INIT_LOAD_FACTOR);

  private final Map<Integer, Set<PageId>> mBucketMap =
      new HashMap<>(LINKED_HASH_MAP_INIT_CAPACITY, LINKED_HASH_MAP_INIT_LOAD_FACTOR);
  private int mMinBucket = -1;
  private double mLogBase = 2;

  private int getBucket(int count) {
    return (int) (Math.log(count) / Math.log(mLogBase));
  }

  private void addPageToBucket(PageId pageId, int bucket) {
    mBucketMap.compute(bucket, (bucketKey, pageSet) -> {
      Set<PageId> set = pageSet == null ? new LinkedHashSet<>() : pageSet;
      set.add(pageId);
      return set;
    });
  }

  private Set<PageId> removePageFromBucket(PageId pageId, int bucket) {
    return mBucketMap.computeIfPresent(bucket, (bucketKey, pageSet) -> {
      pageSet.remove(pageId);
      return pageSet.isEmpty() ? null : pageSet;
    });
  }

  @Override
  // TODO(feng): explore more efficient locking scheme
  public synchronized void updateOnGet(PageId pageId) {
    int newCount = mPageMap.compute(pageId, (id, count) -> count == null ? 1 : count + 1);
    int newBucket = getBucket(newCount);
    if (newCount > 1) {
      int oldBucket = getBucket(newCount - 1);
      if (newBucket != oldBucket) {
        Set<PageId> pagesLeft = removePageFromBucket(pageId, oldBucket);
        if (pagesLeft == null && oldBucket == mMinBucket) {
          mMinBucket = newBucket;
        }
        addPageToBucket(pageId, newBucket);
      }
    } else {
      mMinBucket = newBucket;
      addPageToBucket(pageId, newBucket);
    }
  }

  @Override
  public void updateOnPut(PageId pageId) {
    updateOnGet(pageId);
  }

  @Override
  public synchronized void updateOnDelete(PageId pageId) {
    Integer count = mPageMap.get(pageId);
    if (count == null) {
      return;
    }
    int bucket = getBucket(count);
    Set<PageId> pageSet = removePageFromBucket(pageId, bucket);
    if (pageSet == null && bucket == mMinBucket && !mBucketMap.isEmpty()) {
      // should not be expensive given logarithmic bucket key
      while (!mBucketMap.containsKey(mMinBucket)) {
        mMinBucket++;
      }
    }
  }

  @Nullable
  @Override
  public synchronized PageId evict() {
    Set<PageId> pageSet = mBucketMap.get(mMinBucket);
    return pageSet != null ? pageSet.iterator().next() : null;
  }

  @Override
  public synchronized void reset() {
    mPageMap.clear();
    mBucketMap.clear();
    mMinBucket = -1;
  }
}
