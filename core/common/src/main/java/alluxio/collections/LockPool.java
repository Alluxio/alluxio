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
 * A resource pool specifically designed to contain locks and will NOT evict any entries
 * that are in use. The pool size is unlimited, when the pool size is larger than the configured
 * max size, a background thread will try to evict locks that are no longer
 * locked, but if all the locks are locked, none of them will be evicted.
 * In the worst case (e.g. deadlock), the pool size might keep growing until exhausting system
 * memories.
 *
 * @param <K> key for the locks
 */
public class LockPool<K> {
  private static final Logger LOG = LoggerFactory.getLogger(LockPool.class);
  private static final float DEFAULT_LOAD_FACTOR = 0.75f;
  private static final float SOFT_LIMIT_RATIO = 0.9f;
  private static final String EVICTOR_THREAD_NAME = "LockCache Evictor";

  private final Map<K, Resource> mPool;
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
  private final ExecutorService mEvictor;

  /**
   * Constructor for a lock pool.
   *
   * @param defaultLoader specify a function to generate a value based on a key
   * @param initialSize initial size of the pool
   * @param maxSize maximum size of the pool
   * @param concurrencyLevel concurrency level of the pool
   */
  public LockPool(Function<? super K, ? extends ReentrantReadWriteLock> defaultLoader,
      int initialSize, int maxSize, int concurrencyLevel) {
    mDefaultLoader = defaultLoader;
    mHardLimit = maxSize;
    mSoftLimit = (int) Math.round(SOFT_LIMIT_RATIO * maxSize);
    mPool = new ConcurrentHashMap<>(initialSize, DEFAULT_LOAD_FACTOR, concurrencyLevel);
    mEvictor = Executors.newSingleThreadExecutor(
        ThreadFactoryUtils.build(EVICTOR_THREAD_NAME, true));
    mEvictor.submit(new Evictor());
  }

  private final class Evictor implements Runnable {
    /**
     * Interval in milliseconds for logging when pool size grows over hard limit.
     */
    private static final long OVER_HARD_LIMIT_LOG_INTERVAL = 60000;
    /**
     * Eviction happens whenever the evictor thread is signaled,
     * or is blocked for this period of time in milliseconds.
     */
    private static final int EVICTION_MAX_AWAIT_TIME = 30000;

    /**
     * Creates a new instance.
     */
    public Evictor() {
      mIterator = mPool.entrySet().iterator();
    }

    /**
     * Last millisecond that the pool size grows over hard limit.
     * When size is over hard limit, a log will be printed. This time is used to limit the rate of
     * logging.
     */
    private long mLastOverHardLimitTime = 0;
    private Iterator<Map.Entry<K, Resource>> mIterator;

    @Override
    public void run() {
      try {
        while (!Thread.interrupted()) {
          awaitAndEvict();
        }
      } catch (InterruptedException e) {
        // Allow thread to exit.
      }
    }

    /**
     * Blocks until the size of the pool exceeds the soft limit, evicts entries with zero
     * references until pool size decreases below the soft limit or the whole pool is scanned.
     */
    private void awaitAndEvict() throws InterruptedException {
      try (LockResource l = new LockResource(mEvictLock)) {
        while (mPool.size() <= mSoftLimit) {
          mOverSoftLimit.await(EVICTION_MAX_AWAIT_TIME, TimeUnit.MILLISECONDS);
        }
        int numToEvict = mPool.size() - mSoftLimit;
        // The first round of scan uses the mIterator left from last eviction.
        // Then scan the pool from a new iterator for at most two round:
        // first round to mark candidate.mIsAccessed as false,
        // second round to remove the candidate from the pool.
        int roundToScan = 3;
        while (numToEvict > 0 && roundToScan > 0) {
          if (!mIterator.hasNext()) {
            mIterator = mPool.entrySet().iterator();
            roundToScan--;
          }
          Map.Entry<K, Resource> candidateMapEntry = mIterator.next();
          Resource candidate = candidateMapEntry.getValue();
          if (candidate.mIsAccessed) {
            candidate.mIsAccessed = false;
          } else {
            if (candidate.mRefCount.compareAndSet(0, Integer.MIN_VALUE)) {
              mIterator.remove();
              numToEvict--;
            }
          }
        }
        if (mPool.size() >= mHardLimit) {
          if (System.currentTimeMillis() - mLastOverHardLimitTime > OVER_HARD_LIMIT_LOG_INTERVAL) {
            LOG.warn("LockCache at hard limit, pool size = " + mPool.size()
                + " softLimit = " + mSoftLimit + " hardLimit = " + mHardLimit);
          }
          mLastOverHardLimitTime = System.currentTimeMillis();
        }
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
    Resource valNode = getResource(key);
    ReentrantReadWriteLock lock = valNode.mLock;
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
    Resource valNode = getResource(key);
    ReentrantReadWriteLock lock = valNode.mLock;
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
   * Get the raw readwrite lock from the pool.
   *
   * @param key key to look up the value
   * @return the lock associated with the key
   */
  @VisibleForTesting
  public ReentrantReadWriteLock getRawReadWriteLock(K key) {
    return mPool.getOrDefault(key, new Resource(new ReentrantReadWriteLock())).mLock;
  }

  private Resource getResource(K key) {
    Preconditions.checkNotNull(key, "key can not be null");
    Resource resource = mPool.compute(key, (k, v) -> {
      if (v != null && v.mRefCount.incrementAndGet() > 0) {
        // If the entry is to be removed, ref count will be INT_MIN, so incrementAndGet will < 0.
        v.mIsAccessed = true;
        return v;
      }
      return new Resource(mDefaultLoader.apply(k));
    });
    if (mPool.size() > mSoftLimit) {
      if (mEvictLock.tryLock()) {
        try {
          mOverSoftLimit.signal();
        } finally {
          mEvictLock.unlock();
        }
      }
    }
    return resource;
  }

  /**
   * Returns whether the pool contains a particular key.
   *
   * @param key the key to look up in the pool
   * @return true if the key is contained in the pool
   */
  @VisibleForTesting
  public boolean containsKey(K key) {
    Preconditions.checkNotNull(key, "key can not be null");
    return mPool.containsKey(key);
  }

  /**
   * Returns the size of the pool.
   *
   * @return the number of entries pool
   */
  public int size() {
    return mPool.size();
  }

  /**
   * Get the soft size limit for this pool.
   *
   * @return the soft size limit
   */
  @VisibleForTesting
  public int getSoftLimit() {
    return mSoftLimit;
  }

  /**
   * @return all entries in the pool, for debugging purposes
   */
  @VisibleForTesting
  public Map<K, ReentrantReadWriteLock> getEntryMap() {
    Map<K, ReentrantReadWriteLock> entries = new HashMap<>();
    mPool.entrySet().forEach(entry -> {
      entries.put(entry.getKey(), entry.getValue().mLock);
    });
    return entries;
  }

  /**
   * Resource containing the lock and other information to be stored in the pool.
   */
  private static final class Resource {
    private final ReentrantReadWriteLock mLock;
    private volatile boolean mIsAccessed;
    private AtomicInteger mRefCount;

    private Resource(ReentrantReadWriteLock lock) {
      mLock = lock;
      mIsAccessed = false;
      mRefCount = new AtomicInteger(1);
    }
  }
}
