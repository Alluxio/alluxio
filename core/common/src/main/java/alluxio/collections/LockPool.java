package alluxio.collections;

import alluxio.concurrent.LockMode;
import alluxio.resource.LockResource;
import alluxio.resource.RWLockResource;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A Lock Pool backed by a {@link MapMaker} with values (the locks) stored as weak references.
 * @param <K> key type
 */
public class LockPool<K> implements Closeable {

  public final ConcurrentMap<K, ReentrantReadWriteLock> mCache;

  /**
   * Lock pool.
   * @param initialCap initial size of the pool
   * @param concurrency the expected concurrency of accesses to the pool
   */
  public LockPool(int initialCap, int concurrency) {
    mCache = new MapMaker().weakValues().initialCapacity(initialCap)
        .concurrencyLevel(concurrency).makeMap();
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
    return new RWLockResource(getRawReadWriteLock(key), mode, true, useTryLock);
  }

  /**
   * Attempts to take a lock on the given key.
   *
   * @param key the key to lock
   * @param mode lockMode to acquire
   * @return either empty or a lock resource which must be closed to unlock the key
   */
  public Optional<RWLockResource> tryGet(K key, LockMode mode) {
    ReentrantReadWriteLock lock = getRawReadWriteLock(key);
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
    return Optional.of(new RWLockResource(lock, mode, false, false));
  }

  /**
   * Get the raw readwrite lock from the pool.
   *
   * @param key key to look up the value
   * @return the lock associated with the key
   */
  @VisibleForTesting
  public ReentrantReadWriteLock getRawReadWriteLock(K key) {
    return mCache.computeIfAbsent(key, (K) -> new ReentrantReadWriteLock());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("size", mCache.size())
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
    return mCache.containsKey(key);
  }

  /**
   * @return the estimated size of the pool
   */
  public int size() {
    return mCache.size();
  }

  /**
   * @return all entries in the pool, for debugging purposes
   */
  @VisibleForTesting
  public Map<K, ReentrantReadWriteLock> getEntryMap() {
    return new HashMap<>(mCache);
  }

  @Override
  public void close() {
    mCache.clear();
  }
}
