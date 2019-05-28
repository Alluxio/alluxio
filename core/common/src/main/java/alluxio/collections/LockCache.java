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

import java.util.HashMap;
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
 * that are in use. The cache size is unlimited, when the cache size is larger than the configured
 * max size, each {@code get} or {@code tryGet} will try to evict locks that are no longer
 * locked, but if all the locks are locked, none of them will be evicted.
 * In the worst case (e.g. deadlock), the cache size might keep growing until exhausting system
 * resources.
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
   * once the size reaches hardlimit, warnings are logged.
   */
  private final int mHardLimit;
  private final int mSoftLimit;
  private final Function<? super K, ? extends ReentrantReadWriteLock> mDefaultLoader;

  private final Lock mEvictLock = new ReentrantLock();

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
  }

  /**
   * If the size of the cache exceeds the soft limit, evict entries with zero references.
   */
  private void evictIfOverLimit() {
    if (mCache.size() <= mSoftLimit) {
      return;
    }
    if (mEvictLock.tryLock()) {
      try {
        Iterator<Map.Entry<K, ValNode>> iterator = mCache.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry<K, ValNode> candidateMapEntry = iterator.next();
          ValNode candidate = candidateMapEntry.getValue();
          if (candidate.mRefCount.compareAndSet(0, Integer.MIN_VALUE)) {
            iterator.remove();
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
    ValNode cacheEntry = mCache.compute(key, (k, v) -> {
      if (v != null && v.mRefCount.incrementAndGet() > 0) {
        // If the entry is to be removed, ref count will be INT_MIN, so incrementAndGet will < 0.
        return v;
      }
      return new ValNode(mDefaultLoader.apply(k));
    });
    evictIfOverLimit();
    if (mCache.size() >= mHardLimit) {
      LOG.warn("LockCache at hard limit, cache size = " + mCache.size()
          + " softLimit = " + mSoftLimit + " hardLimit = " + mHardLimit);
    }
    return cacheEntry;
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
   * @return all entries in the cache, for debugging purposes
   */
  @VisibleForTesting
  public Map<K, ReentrantReadWriteLock> getEntryMap() {
    Map<K, ReentrantReadWriteLock> entries = new HashMap<>();
    mCache.entrySet().forEach(entry -> {
      entries.put(entry.getKey(), entry.getValue().mValue);
    });
    return entries;
  }

  /**
   * Node containing value and other information to be stored in the cache.
   */
  private static final class ValNode {
    private final ReentrantReadWriteLock mValue;
    private AtomicInteger mRefCount;

    private ValNode(ReentrantReadWriteLock value) {
      mValue = value;
      mRefCount = new AtomicInteger(1);
    }
  }
}
