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
import alluxio.util.ThreadFactoryUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * A cache specifically designed to contain locks and will NOT evict any entries
 * that are in use. The cache size is unlimited, when the cache size is larger than the configured
 * max size, a background thread will try to evict locks that are no longer
 * locked, but if all the locks are locked, none of them will be evicted.
 * In the worst case (e.g. deadlock), the cache size might keep growing until exhausting system
 * resources.
 *
 * @param <K> key to the cache
 */
public class LockCache<K> {
  private static final Logger LOG = LoggerFactory.getLogger(LockCache.class);
  private static final long OVER_HARD_LIMIT_LOG_INTERVAL = 60000; // milliseconds
  private static final float DEFAULT_LOAD_FACTOR = 0.75f;
  private static final float SOFT_LIMIT_RATIO = 0.9f;
  /**
   * Eviction happens whenever the evictor thread is signaled,
   * or is blocked for this period of time.
   */
  private static final int EVICTION_INTERVAL = 1000; // milliseconds
  private static final String EVICTOR_THREAD_NAME = "LockCache Evictor";

  private final Map<K, ValNode> mCache;
  private Iterator<Map.Entry<K, ValNode>> mIterator;
  /**
   * SoftLimit = hardLimit * softLimitRatio
   * once the size reaches softlimit, eviction starts to happen,
   * once the size reaches hardlimit, warnings are logged.
   */
  private final int mHardLimit;
  private final int mSoftLimit;
  private final Function<? super K, ? extends ReentrantReadWriteLock> mDefaultLoader;

  private final Lock mEvictLock = new ReentrantLock();
  private final Condition mOverSoftLimit = mEvictLock.newCondition();
  private volatile long mLastOverHardLimitTime = 0;
  private final ExecutorService mEvictor;

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
    mEvictor = Executors.newSingleThreadExecutor(
        ThreadFactoryUtils.build(EVICTOR_THREAD_NAME, true));
    mEvictor.submit(() -> {
      try {
        while (!Thread.interrupted()) {
          evictIfOverLimit();
        }
      } catch (InterruptedException e) {
        // Allow thread to exit.
      }
    });
  }

  /**
   * If the size of the cache exceeds the soft limit, evicts entries with zero references until
   * cache size decreases below the soft limit or the whole cache is scanned.
   */
  private void evictIfOverLimit() throws InterruptedException {
    try (LockResource l = new LockResource(mEvictLock)) {
      while (mCache.size() <= mSoftLimit) {
        mOverSoftLimit.await(EVICTION_INTERVAL, TimeUnit.MILLISECONDS);
      }
      int numToEvict = mCache.size() - mSoftLimit;
      // The first round of scan uses the mIterator left from last eviction.
      // Then scan the cache from a new iterator for at most two round:
      // first round to mark candidate.mIsAccessed as false,
      // second round to remove the candidate from the cache.
      int roundToScan = 3;
      while (numToEvict > 0 && roundToScan > 0) {
        if (!mIterator.hasNext()) {
          mIterator = mCache.entrySet().iterator();
          roundToScan--;
        }
        Map.Entry<K, ValNode> candidateMapEntry = mIterator.next();
        ValNode candidate = candidateMapEntry.getValue();
        if (candidate.mIsAccessed) {
          candidate.mIsAccessed = false;
        } else {
          if (candidate.mRefCount.compareAndSet(0, Integer.MIN_VALUE)) {
            mIterator.remove();
            numToEvict--;
          }
        }
      }
      if (mCache.size() >= mHardLimit) {
        if (System.currentTimeMillis() - mLastOverHardLimitTime > OVER_HARD_LIMIT_LOG_INTERVAL) {
          LOG.warn("LockCache at hard limit, cache size = " + mCache.size()
              + " softLimit = " + mSoftLimit + " hardLimit = " + mHardLimit);
        }
        mLastOverHardLimitTime = System.currentTimeMillis();
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
        v.mIsAccessed = true;
        return v;
      }
      return new ValNode(mDefaultLoader.apply(k));
    });
    if (mCache.size() > mSoftLimit) {
      if (mEvictLock.tryLock()) {
        try {
          mOverSoftLimit.signal();
        } finally {
          mEvictLock.unlock();
        }
      }
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
    private volatile boolean mIsAccessed;
    private AtomicInteger mRefCount;

    private ValNode(ReentrantReadWriteLock value) {
      mValue = value;
      mIsAccessed = true;
      mRefCount = new AtomicInteger(1);
    }
  }
}
