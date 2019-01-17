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

package alluxio.collections;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

public class LockCache<K, V> {
  public class ValNode<V> {
    private V mValue;
    private boolean mIsAccessed;
    private AtomicInteger mRefCount;
    private boolean mIsNew;

    private ValNode(V value) {
      mValue = value;
      mIsAccessed = false;
      mRefCount = new AtomicInteger(0);
      mIsNew = true;
    }

    public AtomicInteger getRefCounter() {
      return mRefCount;
    }

    public V get() {
      return mValue;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(LockCache.class);
  private final static float DEFAULT_LOAD_FACTOR = 0.75f;
  static final float SOFT_LIMIT_RATIO = 0.9f;

  private final Map<K, ValNode<V>> mCache;
  private final int mConcurrencyLevel;
  private final int mInitSize;
  /* A suggested maximum size for the cache */
  private final int mHardLimit;
  private final int mSoftLimit;
  private final Function<? super K, ? extends V> mDefaultLoader;
  private Iterator<Map.Entry<K, ValNode<V>>> mIterator;
  private final Lock mEvictLock;

  public LockCache(@Nullable Function<? super K, ? extends V> defaultLoader, int initialSize,
      int maxSize, int concurrencyLevel) {
    mDefaultLoader = defaultLoader;
    mConcurrencyLevel = concurrencyLevel;
    mInitSize = initialSize;
    mHardLimit = maxSize;
    mSoftLimit = (int) Math.round(SOFT_LIMIT_RATIO * maxSize);
    mCache = new ConcurrentHashMap<>(mInitSize, DEFAULT_LOAD_FACTOR, concurrencyLevel);
    mIterator = mCache.entrySet().iterator();
    mEvictLock = new ReentrantLock();
  }

  private void evictIfOverLimit() {
    long numToEvict = mCache.size() - mSoftLimit;

    if (numToEvict <= 0) {
      return;
    }
    // this will block if every lock has a reference on them.
    if (mEvictLock.tryLock()) {
      try {
        // This thread is the evictor
        while (numToEvict > 0) {
          if (!mIterator.hasNext()) {
            mIterator = mCache.entrySet().iterator();
          }
          Map.Entry<K, ValNode<V>> candidateMapEntry = mIterator.next();
          ValNode<V> candidate = candidateMapEntry.getValue();
          if (candidate.mIsAccessed) {
            candidate.mIsAccessed = false;
          } else {
            if ((!candidate.mIsNew) && candidate.mRefCount.compareAndSet(0, Integer.MIN_VALUE)) {
              // the value object can be evicted, at the same time we make refCount minValue
              mIterator.remove();
              numToEvict--;
            }
          }
        }

      } finally {
        mEvictLock.unlock();
      }
    }
  }

  public ValNode<V> get(final K key) {
    Preconditions.checkNotNull(key, "key can not be null");
    ValNode<V> oldCacheEntry = null;
    ValNode<V> cacheEntry;
    while (true) {
      // repeat until we get a cacheEntry that is not in the process of being removed.
      cacheEntry = mCache.computeIfAbsent(key, k -> {
        if (mCache.size() >= mHardLimit) {
          return null;
        } else {
          return new ValNode<>(mDefaultLoader.apply(k));
        }
      });

      if (cacheEntry == null) {
        // cache is at hard limit
        try {
          Thread.sleep(100);
          evictIfOverLimit();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        continue;
      }

      if ((oldCacheEntry != cacheEntry)
          && cacheEntry.mRefCount.incrementAndGet() > 0) {
        // refCount went negative, we are evicting this entry
        cacheEntry.mIsNew = false;
        evictIfOverLimit();
        return cacheEntry;
      } else {
        oldCacheEntry = cacheEntry;
        evictIfOverLimit();
        // TODO: sleep here to prevent overloading the cache
      }
    }

  }

  public boolean contains(final K key) {
    Preconditions.checkNotNull(key, "key can not be null");
    return mCache.containsKey(key);
  }

  public int size() {
    return mCache.size();
  }
}