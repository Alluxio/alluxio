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

package alluxio.master.file.meta;

import alluxio.concurrent.WeakSafeReentrantReadWriteLock;
import alluxio.master.file.meta.InodeTree.LockMode;
import alluxio.resource.LockResource;
import alluxio.util.interfaces.Scoped;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Class for managing inode locking. We manage locks centrally instead of embedded in the inode
 * tree. This allows us to create locks only as needed, and garbage collect locks that aren't in
 * use. As a result, we save memory when the inode tree contains many millions of files.
 *
 * We use WeakSafeReentrantReadWriteLock instead of ReentrantReadWriteLock because the read locks
 * and write locks returned by ReentrantReadWriteLock do not contain a reference to the original
 * ReentrantReadWriteLock, so the original lock can be garbage collected early.
 * WeakSafeReentrantReadWriteLock stores the reference to the original lock to avoid this problem.
 * See https://github.com/google/guava/issues/2477
 */
public class InodeLockManager {
  /**
   * Cache for supplying inode locks. To lock an inode, its inode id must be searched in this
   * cache to get the appropriate read lock.
   *
   * We use weak values so that when nothing holds a reference to
   * a lock, the garbage collector can remove the lock's entry from the cache.
   */
  public final LoadingCache<Long, WeakSafeReentrantReadWriteLock> mInodeLocks =
      CacheBuilder.<Long, WeakSafeReentrantReadWriteLock>newBuilder()
          .weakValues()
          .initialCapacity(1_000)
          .concurrencyLevel(100)
          .build(new CacheLoader<Long, WeakSafeReentrantReadWriteLock>() {
            @Override
            public WeakSafeReentrantReadWriteLock load(Long key) {
              return new WeakSafeReentrantReadWriteLock();
            }
          });

  /**
   * Cache for supplying edge locks, similar to mInodeLocks.
   */
  public final LoadingCache<Edge, WeakSafeReentrantReadWriteLock> mEdgeLocks =
      CacheBuilder.<Long, WeakSafeReentrantReadWriteLock>newBuilder()
          .weakValues()
          .initialCapacity(1_000)
          .concurrencyLevel(100)
          .build(new CacheLoader<Edge, WeakSafeReentrantReadWriteLock>() {
            @Override
            public WeakSafeReentrantReadWriteLock load(Edge key) {
              return new WeakSafeReentrantReadWriteLock();
            }
          });

  /**
   * Cache for supplying inode persistence locks. Before a thread can persist an inode, it must
   * acquire the persisting lock for the inode. The cache maps inode ids to AtomicBooleans used to
   * provide mutual exclusion for inode persisting threads.
   */
  private final LoadingCache<Long, AtomicBoolean> mPersistingLocks =
      CacheBuilder.newBuilder()
          .weakValues()
          .initialCapacity(1_000)
          .concurrencyLevel(100)
          .build(new CacheLoader<Long, AtomicBoolean>() {
            @Override
            public AtomicBoolean load(Long key) {
              return new AtomicBoolean();
            }
          });

  @VisibleForTesting
  boolean inodeReadLockedByCurrentThread(long inodeId) {
    return mInodeLocks.getUnchecked(inodeId).getReadHoldCount() > 0;
  }

  @VisibleForTesting
  boolean inodeWriteLockedByCurrentThread(long inodeId) {
    return mInodeLocks.getUnchecked(inodeId).getWriteHoldCount() > 0;
  }

  @VisibleForTesting
  boolean edgeReadLockedByCurrentThread(Edge edge) {
    return mEdgeLocks.getUnchecked(edge).getReadHoldCount() > 0;
  }

  @VisibleForTesting
  boolean edgeWriteLockedByCurrentThread(Edge edge) {
    return mEdgeLocks.getUnchecked(edge).getWriteHoldCount() > 0;
  }

  /**
   * Acquires an inode lock.
   *
   * @param inode the inode to lock
   * @param mode the mode to lock in
   * @return a lock resource which must be closed to release the lock
   */
  public LockResource lockInode(InodeView inode, LockMode mode) {
    switch (mode) {
      case READ:
        return new LockResource(mInodeLocks.getUnchecked(inode.getId()).readLock());
      case WRITE:
        return new LockResource(mInodeLocks.getUnchecked(inode.getId()).writeLock());
      default:
        throw new IllegalStateException("Unknown lock mode: " + mode);
    }
  }

  /**
   * Acquires an edge lock.
   *
   * @param edge the edge to lock
   * @param mode the mode to lock in
   * @return a lock resource which must be closed to release the lock
   */
  public LockResource lockEdge(Edge edge, LockMode mode) {
    ReadWriteLock lock = mEdgeLocks.getUnchecked(edge);
    switch (mode) {
      case READ:
        return new LockResource(lock.readLock());
      case WRITE:
        return new LockResource(lock.writeLock());
      default:
        throw new IllegalStateException("Unknown lock mode: " + mode);
    }
  }

  /**
   * Tries to acquire a lock for persisting the specified inode id.
   *
   * @param inodeId the inode to acquire the lock for
   * @return an optional wrapping a closure for releasing the lock on success, or Optional.empty if
   *         the lock is already taken
   */
  public Optional<Scoped> tryAcquirePersistingLock(long inodeId) {
    AtomicBoolean lock;
    try {
      lock = mPersistingLocks.get(inodeId);
    } catch (ExecutionException e) {
      throw new RuntimeException(e); // not possible
    }
    if (lock.compareAndSet(false, true)) {
      return Optional.of(() -> lock.set(false));
    }
    return Optional.empty();
  }
}
