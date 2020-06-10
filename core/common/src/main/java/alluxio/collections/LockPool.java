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

import alluxio.Constants;
import alluxio.concurrent.LockMode;
import alluxio.resource.LockResource;
import alluxio.resource.RWLockResource;
import alluxio.resource.RefCountLockResource;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
public class LockPool<K> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(LockPool.class);
  private static final float DEFAULT_LOAD_FACTOR = 0.75f;
  private static final String EVICTOR_THREAD_NAME = "LockPool Evictor";

  private final Map<K, Resource> mPool;
  private final Function<? super K, ? extends ReentrantReadWriteLock> mDefaultLoader;
  private final int mLowWatermark;
  private final int mHighWatermark;

  private final Lock mEvictLock = new ReentrantLock();
  private final Condition mOverHighWatermark = mEvictLock.newCondition();
  private final ExecutorService mEvictor;
  private final Future<?> mEvictorTask;

  /**
   * Constructor for a lock pool.
   *
   * @param defaultLoader specify a function to generate a value based on a key
   * @param initialSize initial size of the pool
   * @param lowWatermark low watermark of the pool size
   * @param highWatermark high watermark of the pool size
   * @param concurrencyLevel concurrency level of the pool
   */
  public LockPool(Function<? super K, ? extends ReentrantReadWriteLock> defaultLoader,
      int initialSize, int lowWatermark, int highWatermark, int concurrencyLevel) {
    mDefaultLoader = defaultLoader;
    mLowWatermark = lowWatermark;
    mHighWatermark = highWatermark;
    mPool = new ConcurrentHashMap<>(initialSize, DEFAULT_LOAD_FACTOR, concurrencyLevel);
    mEvictor = Executors.newSingleThreadExecutor(
        ThreadFactoryUtils.build(String.format("%s-%s", EVICTOR_THREAD_NAME, toString()), true));
    mEvictorTask = mEvictor.submit(new Evictor());
  }

  @Override
  public void close() throws IOException {
    mEvictorTask.cancel(true);
    mEvictor.shutdownNow(); // immediately halt the evictor thread.
    try {
      mEvictor.awaitTermination(2, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Failed to await LockPool evictor termination", e);
    }
  }

  private final class Evictor implements Runnable {
    /**
     * Interval in milliseconds for logging when pool size grows over high watermark.
     */
    private static final long OVER_HIGH_WATERMARK_LOG_INTERVAL = Constants.MINUTE_MS;
    /**
     * Eviction happens whenever the evictor thread is signaled,
     * or is blocked for this period of time in milliseconds.
     */
    private static final int EVICTION_MAX_AWAIT_TIME = 30 * Constants.SECOND_MS;

    /**
     * When size is over high watermark, a warning will be logged.
     * This represents the last time (millisecond) the warning is logged.
     * This time is used to limit the rate of logging.
     */
    private long mLastSizeWarningTime = 0;
    /**
     * Iterator for the pool, used to continue evicting from the previous iterator.
     */
    private Iterator<Map.Entry<K, Resource>> mIterator;

    /**
     * Creates a new instance.
     */
    public Evictor() {
      mIterator = mPool.entrySet().iterator();
    }

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
     * Blocks until the size of the pool exceeds the high watermark, evicts entries with zero
     * references until pool size decreases below the low watermark or the whole pool is scanned.
     */
    private void awaitAndEvict() throws InterruptedException {
      try (LockResource l = new LockResource(mEvictLock)) {
        while (mPool.size() <= mHighWatermark) {
          mOverHighWatermark.await(EVICTION_MAX_AWAIT_TIME, TimeUnit.MILLISECONDS);
        }
        int numToEvict = mPool.size() - mLowWatermark;
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
        if (mPool.size() >= mHighWatermark) {
          if (System.currentTimeMillis() - mLastSizeWarningTime
              > OVER_HIGH_WATERMARK_LOG_INTERVAL) {
            LOG.warn("LockPool size grows over high watermark: "
                + "pool size = {}, low watermark = {}, high watermark = {}",
                mPool.size(), mLowWatermark, mHighWatermark);
            mLastSizeWarningTime = System.currentTimeMillis();
          }
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
    return get(key, mode, false);
  }

  /**
   * Locks the specified key in the specified mode.
   *
   * @param key the key to lock
   * @param mode the mode to lock in
   * @param useTryLock Determines whether or not to use {@link Lock#tryLock()} or
   *                   {@link Lock#lock()} to acquire the lock. Differs from
   *                   {@link #tryGet(Object, LockMode)} in that it will block until the lock has
   *                   been acquired.
   * @return a lock resource which must be closed to unlock the key
   */
  public RWLockResource get(K key, LockMode mode, boolean useTryLock) {
    Resource resource = getResource(key);
    return new RefCountLockResource(resource.mLock, mode, true, resource.mRefCount, useTryLock);
  }

  /**
   * Attempts to take a lock on the given key.
   *
   * @param key the key to lock
   * @param mode lockMode to acquire
   * @return either empty or a lock resource which must be closed to unlock the key
   */
  public Optional<RWLockResource> tryGet(K key, LockMode mode) {
    Resource resource = getResource(key);
    ReentrantReadWriteLock lock = resource.mLock;
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
    return Optional.of(new RefCountLockResource(lock, mode, false, resource.mRefCount, false));
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
    if (mPool.size() > mHighWatermark) {
      if (mEvictLock.tryLock()) {
        try {
          mOverHighWatermark.signal();
        } finally {
          mEvictLock.unlock();
        }
      }
    }
    return resource;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("lowWatermark", mLowWatermark)
        .add("highWatermark", mHighWatermark)
        .add("size", mPool.size())
        .toString();
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
   * @return the size of the pool
   */
  public int size() {
    return mPool.size();
  }

  /**
   * @return all entries in the pool, for debugging purposes
   */
  @VisibleForTesting
  public Map<K, ReentrantReadWriteLock> getEntryMap() {
    Map<K, ReentrantReadWriteLock> entries = new HashMap<>();
    mPool.forEach((key, value) -> entries.put(key, value.mLock));
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
