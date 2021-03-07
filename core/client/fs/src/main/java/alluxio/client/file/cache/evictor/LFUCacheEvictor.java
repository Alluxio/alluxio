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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * LFU client-side cache eviction policy.
 * Pages are sorted in bucket order based on logarithmic count.
 * Pages inside bucket are sorted in LRU order.
 */
@ThreadSafe
public class LFUCacheEvictor implements CacheEvictor {
  private static final Logger LOG = LoggerFactory.getLogger(LFUCacheEvictor.class);
  private static final int PAGE_MAP_INIT_CAPACITY = 200;
  private static final float PAGE_MAP_INIT_LOAD_FACTOR = 0.75f;
  private static final int BUCKET_MAP_INIT_CAPACITY = 32;
  private static final float BUCKET_MAP_INIT_LOAD_FACTOR = 0.75f;
  private static final int BUCKET_LRU_PAGE_MAP_INIT_CAPACITY = 200;
  private static final float BUCKET_LRU_PAGE_MAP_INIT_LOAD_FACTOR = 0.75f;
  private static final boolean UNUSED_MAP_VALUE = true;

  private final Map<PageId, Integer> mPageMap = new HashMap<>(
      PAGE_MAP_INIT_CAPACITY, PAGE_MAP_INIT_LOAD_FACTOR);
  private final Map<Integer, Map<PageId, Boolean>> mBucketMap =
      new HashMap<>(BUCKET_MAP_INIT_CAPACITY, BUCKET_MAP_INIT_LOAD_FACTOR);
  private int mMinBucket = -1;
  private final double mDivisor;

  /**
   * Required constructor.
   *
   * @param conf Alluxio configuration
   */
  public LFUCacheEvictor(AlluxioConfiguration conf) {
    mDivisor = Math.log(conf.getDouble(PropertyKey.USER_CLIENT_CACHE_EVICTOR_LFU_LOGBASE));
  }

  private int getBucket(int count) {
    return (int) (Math.log(count) / mDivisor);
  }

  private void addPageToBucket(PageId pageId, int bucket) {
    mBucketMap.compute(bucket, (bucketKey, lruMap) -> {
      Map<PageId, Boolean> map = lruMap == null ? new LinkedHashMap<>(
          BUCKET_LRU_PAGE_MAP_INIT_CAPACITY, BUCKET_LRU_PAGE_MAP_INIT_LOAD_FACTOR, true)
          : lruMap;
      map.put(pageId, UNUSED_MAP_VALUE);
      return map;
    });
    LOG.debug("added page {} to bucket {}", pageId, bucket);
  }

  private Map<PageId, Boolean> removePageFromBucket(PageId pageId, int bucket) {
    return mBucketMap.computeIfPresent(bucket, (bucketKey, lruMap) -> {
      if (lruMap.remove(pageId) == null) {
        LOG.debug("cannot remove page {} because it is not found in bucket {}", pageId, bucket);
      } else {
        LOG.debug("removed page {} from bucket {}", pageId, bucket);
      }
      return lruMap.isEmpty() ? null : lruMap;
    });
  }

  private void touchPageInBucket(PageId pageId, int bucket) {
    mBucketMap.computeIfPresent(bucket, (bucketKey, lruMap) -> {
      if (lruMap.get(pageId) == null) {
        LOG.debug("cannot touch page {} - page was not found in bucket {}", pageId, bucket);
      } else {
        LOG.debug("touched page {} in bucket {}", pageId, bucket);
      }
      return lruMap;
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
        Map<PageId, Boolean> pagesLeft = removePageFromBucket(pageId, oldBucket);
        if (pagesLeft == null && oldBucket == mMinBucket) {
          mMinBucket = newBucket;
        }
        addPageToBucket(pageId, newBucket);
      } else {
        touchPageInBucket(pageId, newBucket);
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
    Integer count = mPageMap.remove(pageId);
    if (count == null) {
      LOG.debug("cannot delete page {} - page not found", pageId);
      return;
    }
    int bucket = getBucket(count);
    Map<PageId, Boolean> pagesLeft = removePageFromBucket(pageId, bucket);
    if (pagesLeft == null && bucket == mMinBucket && !mBucketMap.isEmpty()) {
      // should not be expensive given logarithmic bucket key
      while (!mBucketMap.containsKey(mMinBucket)) {
        mMinBucket++;
      }
    }
  }

  @Nullable
  @Override
  public synchronized PageId evict() {
    Map<PageId, Boolean> lruMap = mBucketMap.get(mMinBucket);
    if (lruMap == null) {
      LOG.debug("cannot evict page - bucket {} is empty", mMinBucket);
      return null;
    }
    PageId pageToEvict = lruMap.keySet().iterator().next();
    LOG.debug("plan to evict page {} ", pageToEvict);
    return pageToEvict;
  }

  @Override
  public synchronized void reset() {
    mPageMap.clear();
    mBucketMap.clear();
    mMinBucket = -1;
  }
}
