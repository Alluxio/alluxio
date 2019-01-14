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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class LockCache<K, V> {
  static class ValNode<V> {
    private WeakReference<V> mValue;
    private volatile boolean mAccessed;

    private ValNode(V val) {
      mValue = new WeakReference<>(val);
      mAccessed = true;
    }

    public void setAccessed(boolean accessed) {
      mAccessed = accessed;
    }

    public boolean getAccessed() { return mAccessed; }

    public final boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ValNode)) {
        return false;
      }
      ValNode<V> that = (ValNode) o;
      return Objects.equal(mValue, that.mValue);

    }
  }
  private static final Logger LOG = LoggerFactory.getLogger(LockCache.class);
  private final static float DEFAULT_LOAD_FACTOR = 0.75f;

  private final ConcurrentHashMap<K, ValNode<V>> mCache;
  private final int mConcurrencyLevel;
  private final int mInitSize;
  private final long mMaxSize;
  private final Function<? super K, ? extends V> mDefaultLoader;
  private Iterator<Map.Entry<K, ValNode<V>>> mIterator;
  private final AtomicBoolean mEvictor;

  public LockCache(@Nullable Function<? super K, ? extends V> defaultLoader, int initialSize,
      long maxSize, int concurrencyLevel) {
    mDefaultLoader = defaultLoader;
    mConcurrencyLevel = concurrencyLevel;
    mInitSize = initialSize;
    mMaxSize = maxSize;
    mCache = new ConcurrentHashMap<>(mInitSize, DEFAULT_LOAD_FACTOR, concurrencyLevel);
    mIterator =  mCache.entrySet().iterator();
    mEvictor = new AtomicBoolean(false);
  }

  private void evictIfFull() {
    long numToEvict = mCache.mappingCount() - mMaxSize;
    if (numToEvict < 0) {
      return;
    }
    // this will block if every lock has a reference on them.
    if (mEvictor.compareAndSet(false, true)) {
      // This thread is the evictor
      while (numToEvict > 0) {
        if (!mIterator.hasNext()) {
          mIterator = mCache.entrySet().iterator();
        }
        Map.Entry<K, ValNode<V>> candidate = mIterator.next();

        if (candidate.getValue().mAccessed) {
          candidate.getValue().mAccessed = false;
        } else {
          if (candidate.getValue().mValue.get() == null) {
            mIterator.remove();
            numToEvict--;
          }
        }
      }
      mEvictor.set(false);
    }
  }

  public V get(final K key) {
    Preconditions.checkNotNull(key, "key can not be null");
    evictIfFull();
    ValNode<V> value = mCache.computeIfAbsent(
        key, (k) -> new ValNode<>(mDefaultLoader.apply(k)));
    V val = value.mValue.get();
    if (val == null) {
      V newVal = mDefaultLoader.apply(key);
      value.mValue = new WeakReference<>(newVal);
      return newVal;
    } else {
      return val;
    }
  }

  public void putIfAbsent(final K key, final V value) {
    Preconditions.checkNotNull(key, "key can not be null");
    evictIfFull();
    mCache.computeIfAbsent(key, (k) -> new ValNode<>(value));
  }

  public void put(final K key, final V value) {
    Preconditions.checkNotNull(key, "key can not be null");
    mCache.put(key, new ValNode<>(value));
  }
}