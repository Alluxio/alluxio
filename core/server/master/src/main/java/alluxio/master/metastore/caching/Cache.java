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

import alluxio.metrics.MetricsSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.Thread.State;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nullable;

/**
 * Base class for write-back caches which asynchronously evict entries to backing stores.
 *
 * The cache uses water mark based eviction. A dedicated thread waits for the cache to reach its
 * high water mark, then evicts entries until the cache size reaches the low water mark. All backing
 * store write operations are performed asynchronously in the eviction thread. Cache methods don't
 * block unless the cache reaches maximum capacity. For best performance, maximum capacity should
 * never be reached. This requires that the eviction thread can keep up cache writes.
 *
 * @param <K> the cache key type
 * @param <V> the cache value type
 */
public abstract class Cache<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(Cache.class);

  private final ConcurrentHashMap<K, Entry> mMap = new ConcurrentHashMap<>();

  private final int mMaxSize;
  private final int mHighWaterMark;
  private final int mLowWaterMark;
  private final int mEvictBatchSize;
  private final String mName;

  private final EvictionThread mEvictionThread;
  private final Object mCacheFull = new Object();

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
    MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(mName + "-size"),
        () -> mMap.size());
    mEvictionThread = new EvictionThread();
    mEvictionThread.setDaemon(true);
    mEvictionThread.setPriority(6);
  }

  /**
   * Retrieves a value from the cache, loading it from the backing store if necessary.
   *
   * @param key the key to get the value for
   * @return the value, or empty if the key doesn't exist in the cache or in the backing store
   */
  public Optional<V> get(K key) {
    blockIfCacheFull();
    Entry entry = mMap.computeIfAbsent(key, this::loadEntry);
    if (entry == null || entry.mValue == null) {
      return Optional.empty();
    }
    entry.mReferenced = true;
    wakeEvictionThreadIfNecessary();
    return Optional.of(entry.mValue);
  }

  /**
   * Writes a key/value pair to the cache.
   *
   * @param key the key
   * @param value the value
   */
  public void put(K key, V value) {
    blockIfCacheFull();
    mMap.compute(key, (prevKey, prevValue) -> {
      if (prevValue == null || prevValue.mValue == null) {
        onAdd(key, value);
        return new Entry(key, value);
      }
      prevValue.mValue = value;
      prevValue.mReferenced = true;
      prevValue.mDirty = true;
      return prevValue;
    });
    wakeEvictionThreadIfNecessary();
  }

  /**
   * Removes a key from the cache.
   *
   * The key is not immediately removed from the backing store. Instead, we set the entry's value to
   * null to indicate to the eviction thread that to evict the entry, it must first remove the key
   * from the backing store.
   *
   * @param key the key to remove
   */
  public void remove(K key) {
    // Set the entry so that it will be removed from the backing store when it is encountered by
    // the eviction thread.
    mMap.compute(key, (k, entry) -> {
      if (entry == null) {
        entry = new Entry(key, null);
      } else {
        entry.mValue = null;
      }
      onRemove(key);
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
      onRemove(key);
      onEvict(key, null);
    });
    mMap.clear();
  }

  private void kickEvictionThread() {
    synchronized (mEvictionThread) {
      if (mEvictionThread.getState() == State.NEW) {
        mEvictionThread.start();
      }
      mEvictionThread.notifyAll();
    }
  }

  private void blockIfCacheFull() {
    while (mMap.size() >= mMaxSize) {
      LOG.info("{}: map size: {}, max size: {}, high water: {}", mName, mMap.size(), mMaxSize,
          mHighWaterMark);
      kickEvictionThread();
      // Wait for the eviction thread to finish before continuing.
      synchronized (mCacheFull) {
        try {
          if (mMap.size() >= mMaxSize) {
            mCacheFull.wait();
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(e);
        }
      }
    }
  }

  private void wakeEvictionThreadIfNecessary() {
    if (mEvictionThread.mIsSleeping && mMap.size() >= mHighWaterMark) {
      kickEvictionThread();
    }
  }

  @Nullable
  private Entry loadEntry(K key) {
    Optional<V> value = load(key);
    if (value.isPresent()) {
      onAdd(key, value.get());
      Entry entry = new Entry(key, value.get());
      entry.mDirty = false;
      return entry;
    }
    return null;
  }

  private class EvictionThread extends Thread {
    private final TemporalAmount mWarnInterval = Duration.ofSeconds(30);

    private volatile boolean mIsSleeping = false;

    private Iterator<Entry> mEvictionHead = Collections.emptyIterator();
    private Instant mNextAllowedSizeWarning = Instant.EPOCH;
    private Instant mEvictionStart = Instant.EPOCH;
    private int mEvictionCount = 0;

    // This is used temporarily in each call to evictEntries. We store it as a field to avoid
    // re-allocating the array on each eviction.
    private List<Entry> mEvictionCandidates;

    private EvictionThread() {
      super("eviction-thread");
      mEvictionCandidates = new ArrayList<>(mEvictBatchSize);
    }

    @Override
    public void run() {
      while (true) {
        while (mMap.size() <= mLowWaterMark) {
          synchronized (mEvictionThread) { // Same as synchronized (this)
            if (mMap.size() <= mLowWaterMark) {
              if (mEvictionCount > 0) {
                LOG.info("{}: Evicted {} entries in {}ms", mName, mEvictionCount,
                    Duration.between(mEvictionStart, Instant.now()).toMillis());
              }
              synchronized (mCacheFull) {
                mCacheFull.notifyAll();
              }
              try {
                mEvictionThread.mIsSleeping = true;
                mEvictionThread.wait();
                mEvictionThread.mIsSleeping = false;
                mEvictionStart = Instant.now();
                mEvictionCount = 0;
              } catch (InterruptedException e) {
                return;
              }
            }
          }
        }

        evictBatch(Math.min(mMap.size(), mEvictBatchSize));
        if (mMap.size() >= mMaxSize) {
          Instant now = Instant.now();
          if (now.isAfter(mNextAllowedSizeWarning)) {
            LOG.warn(
                "Cache is full. Consider increasing the cache size or lowering the high "
                    + "water mark. size:{} maxSize:{} highWaterMark:{} lowWaterMark:{}",
                mMap.size(), mMaxSize, mHighWaterMark, mLowWaterMark);
            mNextAllowedSizeWarning = now.plus(mWarnInterval);
          }
        }
      }
    }

    private void evictBatch(int batchSize) {
      mEvictionCandidates.clear();
      while (mEvictionCandidates.size() < batchSize) {
        // Every iteration either sets a referenced bit from true to false or adds a new candidate.
        if (!mEvictionHead.hasNext()) {
          mEvictionHead = mMap.values().iterator();
        }
        Entry candidate = mEvictionHead.next();
        if (candidate == null) {
          return; // cache is empty, nothing to evict
        }
        if (candidate.mReferenced) {
          candidate.mReferenced = false;
          continue;
        }
        mEvictionCandidates.add(candidate);
      }
      flushEntries(mEvictionCandidates);
      for (Entry candidate : mEvictionCandidates) {
        if (null == mMap.computeIfPresent(candidate.mKey, (key, entry) -> {
          if (entry.mDirty) {
            return entry; // entry must have been written since we evicted.
          }
          onEvict(key, entry.mValue);
          return null;
        })) {
          mEvictionCount++;
        }
      }
    }
  }

  //
  // Callbacks so that sub-classes can listen for cache changes. All callbacks on the same key
  // happen atomically with respect to each other and other cache operations.
  //
  // Any key/value pair loaded to the cache can go through 3 type of state change
  //
  // 1. Addition: The key/value pair is added to the cache, either because the user called put(key,
  //    value) or because the user called get(key) and the key existed in the backing store. Either
  //    way, the addition synchronously triggers the onAdd callback
  // 2. Removal: The key/value pair is removed via remove(key). This immediately removes the key
  //    from the cache, and asynchronously removes it from the backing store. The onRemove()
  //    callback runs synchronously in the call to remove(key).
  // 3. Eviction: The key/value pair is asynchronously evicted from the cache. This triggers
  //    onEvict(key, value). If the key was removed, value will be null.
  //

  /**
   * Callback triggered whenever a new key/value pair is added to the cache. This does not include
   * updating an existing key.
   *
   * @param key the added key
   * @param value the added value
   */
  protected void onAdd(K key, V value) {}

  /**
   * Callback triggered whenever a key is removed from the cache. This does not include removing
   * a key that exists only in the backing store.
   *
   * @param key the removed key
   */
  protected void onRemove(K key) {}

  /**
   * Callback triggered whenever a key/value pair is evicted from the cache.
   *
   * Being evicted is different from being flushed - a key could be flushed but then immediately
   * accessed, causing it to stay in the cache. onEvict is only triggered when a key/value pair is
   * completely removed from the cache.
   *
   * Note that the cache processes removes by creating (key, null) entries and asynchronously
   * performing the remove in the backing store. onEvict is also triggered when such entries are
   * processed and removed from the cache. In such cases, value will be null.
   *
   * @param key the evicted key
   * @param value the evicted value, or null if the eviction was on a removal entry
   */
  protected void onEvict(K key, V value) {}

  /**
   * Loads a key from the backing store.
   *
   * @param key the key to load
   * @return the value for the key, or empty if the key doesn't exist in the backing store
   */
  protected abstract Optional<V> load(K key);

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
    // null value means that the key has been removed, and needs to be removed from the backing
    // store.
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
