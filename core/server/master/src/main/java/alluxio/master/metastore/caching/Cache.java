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

package alluxio.master.metastore.caching;

import alluxio.Constants;
import alluxio.master.metastore.ReadOption;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.logging.SamplingLogger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Base class for write-back caches which asynchronously evict entries to backing stores.
 *
 * The cache uses water mark based eviction. A dedicated thread waits for the cache to reach its
 * high water mark, then evicts entries until the cache size reaches the low water mark. All backing
 * store write operations are performed asynchronously in the eviction thread, unless the cache hits
 * maximum capacity. At maximum capacity, methods interact synchronously with the backing store. For
 * best performance, maximum capacity should never be reached. This requires that the eviction
 * thread can keep up cache writes.
 *
 * Cache hit reads are served without any locking. Writes and cache miss reads take locks on their
 * cache key.
 *
 * This class leverages the entry-level locks of ConcurrentHashMap to synchronize operations on the
 * same key.
 *
 * @param <K> the cache key type
 * @param <V> the cache value type
 */
@ThreadSafe
public abstract class Cache<K, V> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Cache.class);

  private final int mMaxSize;
  private final int mHighWaterMark;
  private final int mLowWaterMark;
  private final int mEvictBatchSize;
  private final String mName;
  @VisibleForTesting
  final ConcurrentHashMap<K, Entry> mMap;
  // TODO(andrew): Support using multiple threads to speed up backing store writes.
  // Thread for performing eviction to the backing store.
  @VisibleForTesting
  final EvictionThread mEvictionThread;

  private final StatsCounter mStatsCounter;

  /**
   * @param conf cache configuration
   * @param name a name for the cache
   * @param evictionsKey the cache evictions metric key
   * @param hitsKey the cache hits metrics key
   * @param loadTimesKey the load times metrics key
   * @param missesKey the misses metrics key
   * @param sizeKey the size metrics key
   */
  public Cache(CacheConfiguration conf, String name, MetricKey evictionsKey, MetricKey hitsKey,
               MetricKey loadTimesKey, MetricKey missesKey, MetricKey sizeKey) {
    mMaxSize = conf.getMaxSize();
    mHighWaterMark = conf.getHighWaterMark();
    mLowWaterMark = conf.getLowWaterMark();
    mEvictBatchSize = conf.getEvictBatchSize();
    mName = name;
    mMap = new ConcurrentHashMap<>(mMaxSize);
    mEvictionThread = new EvictionThread();
    mEvictionThread.setDaemon(true);
    // The eviction thread is started lazily when we first reach the high water mark.
    mStatsCounter = new StatsCounter(evictionsKey, hitsKey, loadTimesKey, missesKey);

    MetricsSystem.registerGaugeIfAbsent(sizeKey.getName(), mMap::size);
  }

  /**
   * Retrieves a value from the cache, loading it from the backing store if necessary.
   *
   * If the value needs to be loaded, concurrent calls to get(key) will block while waiting for the
   * first call to finish loading the value.
   *
   * If option.shouldSkipCache() is true, then the value loaded from the backing store will not be
   * cached and the eviction thread will not be woken up.
   *
   * @param key the key to get the value for
   * @param option the read options
   * @return the value, or empty if the key doesn't exist in the cache or in the backing store
   */
  public Optional<V> get(K key, ReadOption option) {
    if (option.shouldSkipCache() || cacheIsFull()) {
      return getSkipCache(key);
    }
    Entry result = mMap.compute(key, (k, entry) -> {
      if (entry != null) {
        mStatsCounter.recordHit();
        entry.mReferenced = true;
        return entry;
      }
      mStatsCounter.recordMiss();
      final Stopwatch stopwatch = Stopwatch.createStarted();
      Optional<V> value = load(key);
      mStatsCounter.recordLoad(stopwatch.elapsed(TimeUnit.NANOSECONDS));
      if (value.isPresent()) {
        onCacheUpdate(key, value.get());
        Entry newEntry = new Entry(key, value.get());
        newEntry.mDirty = false;
        return newEntry;
      }
      return null;
    });
    if (result == null || result.mValue == null) {
      return Optional.empty();
    }
    wakeEvictionThreadIfNecessary();
    return Optional.of(result.mValue);
  }

  /**
   * @param key the key to get the value for
   * @return the result of {@link #get(Object, ReadOption)} with default option
   */
  public Optional<V> get(K key) {
    return get(key, ReadOption.defaults());
  }

  /**
   * Retrieves a value from the cache if already cached, otherwise, loads from the backing store
   * without caching the value. Eviction is not triggered.
   *
   * @param key the key to get the value for
   * @return the value, or empty if the key doesn't exist in the cache or in the backing store
   */
  private Optional<V> getSkipCache(K key) {
    Entry entry = mMap.get(key);
    if (entry == null) {
      mStatsCounter.recordMiss();
      final Stopwatch stopwatch = Stopwatch.createStarted();
      final Optional<V> result = load(key);
      mStatsCounter.recordLoad(stopwatch.elapsed(TimeUnit.NANOSECONDS));
      return result;
    }
    mStatsCounter.recordHit();
    return Optional.ofNullable(entry.mValue);
  }

  /**
   * Writes a key/value pair to the cache.
   *
   * @param key the key
   * @param value the value
   */
  public void put(K key, V value) {
    mMap.compute(key, (k, entry) -> {
      onPut(key, value);
      if (entry == null && cacheIsFull()) {
        writeToBackingStore(key, value);
        return null;
      }
      if (entry == null || entry.mValue == null) {
        onCacheUpdate(key, value);
        return new Entry(key, value);
      }
      entry.mValue = value;
      entry.mReferenced = true;
      entry.mDirty = true;
      return entry;
    });
    wakeEvictionThreadIfNecessary();
  }

  /**
   * Removes a key from the cache.
   *
   * The key is not immediately removed from the backing store. Instead, we set the entry's value to
   * null to indicate to the eviction thread that to evict the entry, it must first remove the key
   * from the backing store. However, if the cache is full we must synchronously write to the
   * backing store instead.
   *
   * @param key the key to remove
   */
  public void remove(K key) {
    // Set the entry so that it will be removed from the backing store when it is encountered by
    // the eviction thread.
    mMap.compute(key, (k, entry) -> {
      onRemove(key);
      if (entry == null && cacheIsFull()) {
        removeFromBackingStore(k);
        return null;
      }
      onCacheUpdate(key, null);
      if (entry == null) {
        entry = new Entry(key, null);
      } else {
        entry.mValue = null;
      }
      entry.mReferenced = false;
      entry.mDirty = true;
      return entry;
    });
    wakeEvictionThreadIfNecessary();
  }

  /**
   * Flushes all data to the backing store.
   */
  public void flush() throws InterruptedException {
    List<Entry> toFlush = new ArrayList<>(mEvictBatchSize);
    Iterator<Entry> it = mMap.values().iterator();
    while (it.hasNext()) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      while (toFlush.size() < mEvictBatchSize && it.hasNext()) {
        Entry candidate = it.next();
        if (candidate.mDirty) {
          toFlush.add(candidate);
        }
      }
      flushEntries(toFlush);
      toFlush.clear();
    }
  }

  /**
   * Clears all entries from the map. This is not threadsafe, and requires external synchronization
   * to prevent concurrent modifications to the cache.
   */
  public void clear() {
    mMap.forEach((key, value) -> {
      onCacheUpdate(key, value.mValue);
      onRemove(key);
    });
    mMap.clear();
  }

  private boolean overHighWaterMark() {
    return mMap.size() >= mHighWaterMark;
  }

  private boolean cacheIsFull() {
    return mMap.size() >= mMaxSize;
  }

  private void wakeEvictionThreadIfNecessary() {
    if (mEvictionThread.mIsSleeping && mMap.size() >= mHighWaterMark) {
      kickEvictionThread();
    }
  }

  private void kickEvictionThread() {
    synchronized (mEvictionThread) {
      if (mEvictionThread.getState() == State.NEW) {
        mEvictionThread.start();
      }
      mEvictionThread.notifyAll();
    }
  }

  @Override
  public void close() {
    mEvictionThread.interrupt();
    try {
      mEvictionThread.join(10L * Constants.SECOND_MS);
      if (mEvictionThread.isAlive()) {
        LOG.warn("Failed to stop eviction thread");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  class EvictionThread extends Thread {
    @VisibleForTesting
    volatile boolean mIsSleeping = true;

    // Populated with #fillBatch, cleared with #evictBatch. We keep it around so that we don't need
    // to keep re-allocating the list.
    private final List<Entry> mEvictionCandidates = new ArrayList<>(mEvictBatchSize);
    private final List<Entry> mDirtyEvictionCandidates = new ArrayList<>(mEvictBatchSize);
    private final Logger mCacheFullLogger = new SamplingLogger(LOG, 10L * Constants.SECOND_MS);

    private Iterator<Entry> mEvictionHead = Collections.emptyIterator();

    private EvictionThread() {
      super(mName + "-eviction-thread");
    }

    @Override
    public void run() {
      while (!Thread.interrupted()) {
        // Wait for the cache to get over the high water mark.
        while (!overHighWaterMark()) {
          synchronized (mEvictionThread) {
            if (!overHighWaterMark()) {
              try {
                mIsSleeping = true;
                mEvictionThread.wait();
                mIsSleeping = false;
              } catch (InterruptedException e) {
                return;
              }
            }
          }
        }
        if (cacheIsFull()) {
          mCacheFullLogger.warn(
              "Metastore {} cache is full. Consider increasing the cache size or lowering the "
                  + "high water mark. size:{} lowWaterMark:{} highWaterMark:{} maxSize:{}",
              mName, mMap.size(), mLowWaterMark, mHighWaterMark, mMaxSize);
        }
        evictToLowWaterMark();
      }
    }

    private void evictToLowWaterMark() {
      long evictionStart = System.nanoTime();
      int toEvict = mMap.size() - mLowWaterMark;
      int evictionCount = 0;
      while (evictionCount < toEvict) {
        if (!mEvictionHead.hasNext()) {
          mEvictionHead = mMap.values().iterator();
        }
        fillBatch(toEvict - evictionCount);
        evictionCount += evictBatch();
      }
      if (evictionCount > 0) {
        mStatsCounter.recordEvictions(evictionCount);
        LOG.debug("{}: Evicted {} entries in {}ms", mName, evictionCount,
            (System.nanoTime() - evictionStart) / Constants.MS_NANO);
      }
    }

    /**
     * Attempts to fill mEvictionCandidates with up to min(count, mEvictBatchSize) candidates for
     * eviction.
     *
     * @param count maximum number of entries to store in the batch
     */
    private void fillBatch(int count) {
      int targetSize = Math.min(count, mEvictBatchSize);
      while (mEvictionCandidates.size() < targetSize && mEvictionHead.hasNext()) {
        Entry candidate = mEvictionHead.next();
        if (candidate.mReferenced) {
          candidate.mReferenced = false;
          continue;
        }
        mEvictionCandidates.add(candidate);
        if (candidate.mDirty) {
          mDirtyEvictionCandidates.add(candidate);
        }
      }
    }

    /**
     * Attempts to evict all entries in mEvictionCandidates.
     *
     * @return the number of candidates actually evicted
     */
    private int evictBatch() {
      int evicted = 0;
      if (mEvictionCandidates.isEmpty()) {
        return evicted;
      }
      flushEntries(mDirtyEvictionCandidates);
      for (Entry entry : mEvictionCandidates) {
        if (evictIfClean(entry)) {
          evicted++;
        }
      }
      mEvictionCandidates.clear();
      mDirtyEvictionCandidates.clear();
      return evicted;
    }

    /**
     * @param entry the entry to try to evict
     * @return whether the entry was successfully evicted
     */
    private boolean evictIfClean(Entry entry) {
      return null == mMap.computeIfPresent(entry.mKey, (key, e) -> {
        if (entry.mDirty) {
          return entry; // entry must have been written since we evicted.
        }
        onCacheRemove(entry.mKey);
        return null;
      });
    }
  }

  @VisibleForTesting
  protected Map<K, Entry> getCacheMap() {
    return new HashMap<>(mMap);
  }

  //
  // Callbacks so that sub-classes can listen for cache changes. All callbacks on the same key
  // happen atomically with respect to each other and other cache operations.
  //

  /**
   * Callback triggered when an update is made to a key/value pair in the cache. For removals, value
   * will be null.
   *
   * @param key the updated key
   * @param value the updated value, or null if the key is being removed
   */
  protected void onCacheUpdate(K key, @Nullable V value) {}

  /**
   * Callback triggered when a key is removed from the cache.
   *
   * This may be used in conjunction with onCacheUpdate to keep track of all changes to the cache.
   *
   * @param key the removed key
   */
  protected void onCacheRemove(K key) {}

  /**
   * Callback triggered whenever a new key/value pair is added by put(key, value).
   *
   * @param key the added key
   * @param value the added value
   */
  protected void onPut(K key, V value) {}

  /**
   * Callback triggered whenever a key is removed by remove(key).
   *
   * @param key the removed key
   */
  protected void onRemove(K key) {}

  /**
   * Loads a key from the backing store.
   *
   * @param key the key to load
   * @return the value for the key, or empty if the key doesn't exist in the backing store
   */
  protected abstract Optional<V> load(K key);

  /**
   * Writes a key/value pair to the backing store.
   *
   * @param key the key
   * @param value the value
   */
  protected abstract void writeToBackingStore(K key, V value);

  /**
   * Removes a key from the backing store.
   *
   * @param key the key
   */
  protected abstract void removeFromBackingStore(K key);

  /**
   * Attempts to flush the given entries to the backing store.
   *
   * The subclass is responsible for setting each candidate's mDirty field to false on success.
   *
   * @param candidates the candidate entries to flush
   */
  protected abstract void flushEntries(List<Entry> candidates);

  protected class Entry {
    protected K mKey;
    // null value means that the key has been removed from the cache, but still needs to be removed
    // from the backing store.
    @Nullable
    protected V mValue;

    // Whether the entry is out of sync with the backing store. If mDirty is true, the entry must be
    // flushed to the backing store before it can be evicted.
    protected volatile boolean mDirty = true;

    // Whether the entry has been recently accessed. Accesses set the bit to true, while the
    // eviction thread sets it to false. This is the same as the "referenced" bit described in the
    // CLOCK algorithm.
    private volatile boolean mReferenced = true;

    private Entry(K key, V value) {
      mKey = key;
      mValue = value;
    }
  }
}
