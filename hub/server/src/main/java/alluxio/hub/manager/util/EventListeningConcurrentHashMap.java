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

package alluxio.hub.manager.util;

import alluxio.collections.ConcurrentHashSet;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Use this map to run runnable functions based update events to this map object. Currently, only
 * {@link #put} and {@link #remove(Object)} will trigger events.
 *
 * The event handler are run in a separate executor service thread(s).
 *
 * @param <K> the key of the map
 * @param <V> value of the map
 */
public class EventListeningConcurrentHashMap<K, V> extends ConcurrentHashMap<K, V> {
  private static final long serialVersionUID = 777227692436086087L;

  private final ConcurrentHashSet<EventListener> mListeners;
  private final ExecutorService mSvc;

  /**
   * A new instance of {@link EventListeningConcurrentHashMap}.
   *
   * @param service the executor service used to run the event listeners
   */
  public EventListeningConcurrentHashMap(ExecutorService service) {
    mSvc = service;
    mListeners = new ConcurrentHashSet<>();
  }

  /**
   * Register a runnable to be run every time this map is modified.
   *
   * @param runnable the runnable to run
   * @param minMs the minimum amount of milliseconds that must elapse between executing the
   *              runnable (set to < 0 to capture every single event)
   */
  public void registerEventListener(Runnable runnable, long minMs) {
    mListeners.add(new EventListener(runnable, minMs));
  }

  private void runEventListeners() {
    long execTime = System.currentTimeMillis();
    for (EventListener e : mListeners) {
      if (e.mLastUpdateTime.get() + e.mMinMs > execTime) {
        continue;
      }
      mSvc.submit(e.mHandler);
      e.mLastUpdateTime.set(execTime);
    }
  }

  @Override
  public V put(K key, V value) {
    V rv = super.put(key, value);
    if (!value.equals(rv)) {
      // modified, run events
      runEventListeners();
    }
    return rv;
  }

  @Override
  public V putIfAbsent(K key, V value) {
    V rv = super.putIfAbsent(key, value);
    if (rv == null) {
      // modified, run events
      runEventListeners();
    }
    return rv;
  }

  @Override
  public V remove(Object key) {
    V rv = super.remove(key);
    if (rv != null) {
      // modified, run events
      runEventListeners();
    }
    return rv;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  static class EventListener {
    private final Runnable mHandler;
    private final long mMinMs;
    private final AtomicLong mLastUpdateTime;

    public EventListener(Runnable runnable, long minMs) {
      mHandler = runnable;
      mMinMs = minMs;
      mLastUpdateTime = new AtomicLong();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof EventListener)) {
        return false;
      }

      EventListener other = (EventListener) o;
      return mHandler.equals(other.mHandler)
          && mMinMs == other.mMinMs;
    }

    public int hashCode() {
      return Objects.hash(mHandler, mMinMs);
    }
  }
}
