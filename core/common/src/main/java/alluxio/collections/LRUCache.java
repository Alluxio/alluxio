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

import com.google.common.cache.AbstractLoadingCache;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class LRUCache<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(LRUCache.class);
  private final static float DEFAULT_LOAD_FACTOR = 0.75f;

  private final ConcurrentMap<K, Future<V>> mCache;
  private final int mConcurrencyLevel;
  private final int mInitSize;
  private final int mMaxSize;
  private final Callable<V> mDefaultLoader;

  public LRUCache(@Nullable Callable<V> defaultLoader, int initialSize,
      int maxSize, int concurrencyLevel) {
    mDefaultLoader = defaultLoader;
    mConcurrencyLevel = concurrencyLevel;
    mInitSize = initialSize;
    mMaxSize = maxSize;
    mCache = new ConcurrentHashMap<>(mInitSize, DEFAULT_LOAD_FACTOR, concurrencyLevel);
  }

  private Future<V> createFutureIfAbsent(final K key, final Callable<V> callable) {
    Future<V> future = mCache.get(key);
    if (future == null) {
      final FutureTask<V> futureTask = new FutureTask<V>(callable);
      future = mCache.putIfAbsent(key, futureTask);
      if (future == null) {
        future = futureTask;
        futureTask.run();
      }
    }
    return future;
  }

  public V getUnchecked(final K key) {
    try {
      return getValue(key);
    } catch (Exception e) {
      throw new RuntimeException(e.getCause());
    }
  }

  public V getValue(final K key) throws InterruptedException, ExecutionException {
    try {
      final Future<V> future = createFutureIfAbsent(key, mDefaultLoader);
      return future.get();
    } catch (final InterruptedException e) {
      mCache.remove(key);
      throw e;
    } catch (final ExecutionException e) {
      mCache.remove(key);
      throw e;
    } catch (final RuntimeException e) {
      mCache.remove(key);
      throw e;
    }
  }

  public void putIfAbsent(final K key, final V value) {
    createFutureIfAbsent(key, () -> value);
  }

  public void put(final K key, final V value) {
    // TODO:(yuzhu) avoid calling get twice
    Future<V> future = mCache.get(key);
    if (future == null) {
      putIfAbsent(key, value);
    } else {
      future = Futures.immediateFuture(value);
      mCache.put(key, future);

    }
  }
}