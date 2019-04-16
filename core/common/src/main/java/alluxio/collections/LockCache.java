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

import alluxio.concurrent.LockMode;
import alluxio.resource.LockResource;
import alluxio.resource.RefCountLockResource;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * A cache specifically designed to contain locks and will NOT evict any entries
 * that are in use.
 *
 * @param <K> key to the cache
 */
public class LockCache<K> {
  private static final Logger LOG = LoggerFactory.getLogger(LockCache.class);
  private static final float DEFAULT_LOAD_FACTOR = 0.75f;
  private static final float SOFT_LIMIT_RATIO = 0.9f;

  private final Map<K, ValNode> mCache;
  /**
   * SoftLimit = hardLimit * softLimitRatio
   * once the size reaches softlimit, eviction starts to happen,
   * once the size reaches hardlimit, blocking starts to happen.
   */
  private final int mHardLimit;
  private final int mSoftLimit;
  private final Function<? super K, ? extends ReentrantReadWriteLock> mDefaultLoader;
  private final Lock mEvictLock;

  private Iterator<Map.Entry<K, ValNode>> mIterator;
  private long mLastSizeWarningTime;

  /**
   * Constructor for a lock cache.
   *
   * @param defaultLoader specify a function to generate a value based on a key
   * @param initialSize initial size of the cache
   * @param maxSize maximum size of the cache
   * @param concurrencyLevel concurrency level of the cache
   */
  public LockCache(Function<? super K, ? extends ReentrantReadWriteLock> defaultLoader,
      int initialSize, int maxSize, int concurrencyLevel) {
    mDefaultLoader = defaultLoader;
    mHardLimit = maxSize;
    mSoftLimit = (int) Math.round(SOFT_LIMIT_RATIO * maxSize);
    mCache = new ConcurrentHashMap<>(initialSize, DEFAULT_LOAD_FACTOR, concurrencyLevel);
    mIterator = mCache.entrySet().iterator();
    mEvictLock = new ReentrantLock();
    mLastSizeWarningTime = 0;
  }

  /**
   * If the size of the cache exceeds the soft limit and no other thread is evicting entries,
   * start evicting entries.
   */
  private void evictIfOverLimit() {
    int numToEvict = mCache.size() - mSoftLimit;

    if (numToEvict <= 0) {
      return;
    }

    if (mEvictLock.tryLock()) {
      try {
        // update the total number to evict while we are holding the lock
        numToEvict = mCache.size() - mSoftLimit;
        // This thread won the race as the evictor
        while (numToEvict > 0) {
          if (!mIterator.hasNext()) {
            mIterator = mCache.entrySet().iterator();
          }
          Map.Entry<K, ValNode> candidateMapEntry = mIterator.next();
          ValNode candidate = candidateMapEntry.getValue();
          if (candidate.mIsAccessed) {
            candidate.mIsAccessed = false;
          } else {
            if (candidate.mRefCount.compareAndSet(0, Integer.MIN_VALUE)) {
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

  /**
   * Locks the specified key in the specified mode.
   *
   * @param key the key to lock
   * @param mode the mode to lock in
   * @return a lock resource which must be closed to unlock the key
   *
   */
  public LockResource get(K key, LockMode mode) {
    ValNode valNode = getValNode(key);
    ReentrantReadWriteLock lock = valNode.mValue;
    switch (mode) {
      case READ:
        return new RefCountLockResource(lock.readLock(), true, valNode.mRefCount);
      case WRITE:
        return new RefCountLockResource(lock.writeLock(), true, valNode.mRefCount);
      default:
        throw new IllegalStateException("Unknown lock mode: " + mode);
    }
  }

  /**
   * Attempts to take a lock on the given key.
   *
   * @param key the key to lock
   * @param mode lockMode to acquire
   * @return either empty or a lock resource which must be closed to unlock the key
   */
  public Optional<LockResource> tryGet(K key, LockMode mode) {
    ValNode valNode = getValNode(key);
    ReentrantReadWriteLock lock = valNode.mValue;
    Lock innerLock;
    switch (mode) {
      case READ:
        innerLock = lock.readLock();
        break;
      case WRITE:
        innerLock = lock.writeLock();
        break;
      default:
        throw new IllegalStateException("Unknown lock mode: " + mode);
    }
    if (!innerLock.tryLock()) {
      return Optional.empty();
    }
    return Optional.of(new RefCountLockResource(innerLock, false, valNode.mRefCount));
  }

  /**
   * Get the raw readwrite lock from the cache.
   *
   * @param key key to look up the value
   * @return the lock associated with the key
   */
  @VisibleForTesting
  public ReentrantReadWriteLock getRawReadWriteLock(K key) {
    return mCache.getOrDefault(key, new ValNode(new ReentrantReadWriteLock())).mValue;
  }

  private ValNode getValNode(K key) {
    Preconditions.checkNotNull(key, "key can not be null");
    ValNode cacheEntry;
    while (true) {
      // repeat until we get a cacheEntry that is not in the process of being removed.
      cacheEntry = mCache.compute(key, (k, v) -> {
        if (v != null) {
          // we want to maintain the invariant that only one thread is removing from the map.
          // Returning anything other than v would break that invariant.
          v.mRefCount.incrementAndGet();
          v.mIsAccessed = true;
          return v;
        }
        // new cache entry
        if (mCache.size() >= mHardLimit) {
          return null;
        } else {
          return new ValNode(mDefaultLoader.apply(k));
        }
      });

      if (cacheEntry == null) {
        // cache is at hard limit
        try {
          if (System.currentTimeMillis() - mLastSizeWarningTime > 60000) {
            LOG.warn("Cache at hard limit, cache size = " + mCache.size()
                + " softLimit = " + mSoftLimit + " hardLimit = " + mHardLimit);
            mLastSizeWarningTime = System.currentTimeMillis();
          }
          Thread.sleep(5);
          evictIfOverLimit();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        continue;
      }
      evictIfOverLimit();
      if (cacheEntry.mRefCount.get() > 0) {
        return cacheEntry;
      } else {
        try {
          Thread.sleep(5);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * Returns whether the cache contains a particular key.
   *
   * @param key the key to look up in the cache
   * @return true if the key is contained in the cache
   */
  @VisibleForTesting
  public boolean containsKey(K key) {
    Preconditions.checkNotNull(key, "key can not be null");
    return mCache.containsKey(key);
  }

  /**
   * Returns the size of the cache.
   *
   * @return the number of entries cached
   */
  public int size() {
    return mCache.size();
  }

  /**
   * Get the soft size limit for this cache.
   *
   * @return the soft size limit
   */
  @VisibleForTesting
  public int getSoftLimit() {
    return mSoftLimit;
  }

  /**
   * Node containing value and other information to be stored in the cache.
   */
  public class ValNode {
    private ReentrantReadWriteLock mValue;
    private boolean mIsAccessed;
    private AtomicInteger mRefCount;

    private ValNode(ReentrantReadWriteLock value) {
      mValue = value;
      mIsAccessed = false;
      mRefCount = new AtomicInteger(1);
    }
  }
}
