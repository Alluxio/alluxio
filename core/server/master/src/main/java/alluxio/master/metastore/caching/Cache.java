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
import alluxio.metrics.MetricsSystem;
import alluxio.util.logging.SamplingLogger;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.Thread.State;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

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

  /**
   * @param conf cache configuration
   * @param name a name for the cache
   */
  public Cache(CacheConfiguration conf, String name) {
    mMaxSize = conf.getMaxSize();
    mHighWaterMark = conf.getHighWaterMark();
    mLowWaterMark = conf.getLowWaterMark();
    mEvictBatchSize = conf.getEvictBatchSize();
    mName = name;
    mMap = new ConcurrentHashMap<>(mMaxSize);
    mEvictionThread = new EvictionThread();
    mEvictionThread.setDaemon(true);
    // The eviction thread is started lazily when we first reach the high water mark.

    MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(mName + "-size"),
        () -> mMap.size());
  }

  /**
   * Retrieves a value from the cache, loading it from the backing store if necessary.
   *
   * If the value needs to be loaded, concurrent calls to get(key) will block while waiting for the
   * first call to finish loading the value.
   *
   * @param key the key to get the value for
   * @return the value, or empty if the key doesn't exist in the cache or in the backing store
   */
  public Optional<V> get(K key) {
    if (cacheIsFull()) {
      Entry entry = mMap.get(key);
      if (entry == null) {
        return load(key);
      }
      return Optional.ofNullable(entry.mValue);
    }
    Entry result = mMap.compute(key, (k, entry) -> {
      if (entry != null) {
        entry.mReferenced = true;
        return entry;
      }
      Optional<V> value = load(key);
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

  private boolean overLowWaterMark() {
    return mMap.size() > mLowWaterMark;
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
      mEvictionThread.join(10 * Constants.SECOND_MS);
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

    private Iterator<Entry> mEvictionHead = Collections.emptyIterator();
    private final Logger mCacheFullLogger = new SamplingLogger(LOG, 10 * Constants.SECOND_MS);

    // This is used temporarily in each call to evictEntries. We store it as a field to avoid
    // re-allocating the array on each eviction.
    private List<Entry> mEvictionCandidates;

    private EvictionThread() {
      super(mName + "-eviction-thread");
      mEvictionCandidates = new ArrayList<>(mEvictBatchSize);
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
        evictToLowWaterMark();
      }
    }

    private void evictToLowWaterMark() {
      Instant evictionStart = Instant.now();
      int toEvict = mMap.size() - mLowWaterMark;
      int evictionCount = 0;
      while (evictionCount < toEvict) {
        if (cacheIsFull()) {
          mCacheFullLogger.warn(
              "Metastore {} cache is full. Consider increasing the cache size or lowering the "
                  + "high water mark. size:{} lowWaterMark:{} highWaterMark:{} maxSize:{}",
              mName, mMap.size(), mLowWaterMark, mHighWaterMark, mMaxSize);
        }
        evictionCount += evictBatch(Math.min(toEvict - evictionCount, mEvictBatchSize));
      }
      if (evictionCount > 0) {
        LOG.debug("{}: Evicted {} entries in {}ms", mName, evictionCount,
            Duration.between(evictionStart, Instant.now()).toMillis());
      }
    }

    /**
     * @param batchSize the target number of entries to evict. If batchSize is less than 1, no
     *        entries will be evicted
     * @return the number of entries evicted
     */
    private int evictBatch(int batchSize) {
      int evictionCount = 0;
      mEvictionCandidates.clear();
      while (mEvictionCandidates.size() < batchSize) {
        // Every iteration either sets a referenced bit from true to false or adds a new candidate.
        if (!mEvictionHead.hasNext()) {
          mEvictionHead = mMap.values().iterator();
        }
        Entry candidate = mEvictionHead.next();
        if (candidate == null) {
          return evictionCount; // cache is empty, nothing to evict
        }
        if (candidate.mReferenced) {
          candidate.mReferenced = false;
          continue;
        }
        mEvictionCandidates.add(candidate);
      }
      if (mEvictionCandidates.isEmpty()) {
        return 0;
      }
      flushEntries(mEvictionCandidates);
      for (Entry candidate : mEvictionCandidates) {
        if (null == mMap.computeIfPresent(candidate.mKey, (key, entry) -> {
          if (entry.mDirty) {
            return entry; // entry must have been written since we evicted.
          }
          onCacheRemove(candidate.mKey);
          return null;
        })) {
          evictionCount++;
        }
      }
      return evictionCount;
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
   * will be null
   *
   * @param key the updated key
   * @param value the updated value, or null if the key is being removed
   */
  protected void onCacheUpdate(K key, @Nullable V value) {}

  /**
   * Callback triggered when a key is removed from the cache.
   *
   * This may be used in conjunction with onCacheUpdate to keep track of all changes to the cache
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
