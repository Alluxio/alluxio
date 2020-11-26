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

import alluxio.collections.LockPool;
import alluxio.concurrent.LockMode;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.resource.LockResource;
import alluxio.resource.RWLockResource;
import alluxio.util.interfaces.Scoped;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Striped;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
public class InodeLockManager implements Closeable {
  /**
   * Pool for supplying inode locks. To lock an inode, its inode id must be searched in this
   * pool to get the appropriate read lock.
   *
   * We use weak values so that when nothing holds a reference to
   * a lock, the garbage collector can remove the lock's entry from the pool.
   */
  private final LockPool<Long> mInodeLocks =
      new LockPool<>((key)-> new ReentrantReadWriteLock(),
          ServerConfiguration.getInt(PropertyKey.MASTER_LOCK_POOL_INITSIZE),
          ServerConfiguration.getInt(PropertyKey.MASTER_LOCK_POOL_LOW_WATERMARK),
          ServerConfiguration.getInt(PropertyKey.MASTER_LOCK_POOL_HIGH_WATERMARK),
          ServerConfiguration.getInt(PropertyKey.MASTER_LOCK_POOL_CONCURRENCY_LEVEL));
  /**
   * Cache for supplying edge locks, similar to mInodeLocks.
   */
  private final LockPool<Edge> mEdgeLocks =
      new LockPool<>((key)-> new ReentrantReadWriteLock(),
          ServerConfiguration.getInt(PropertyKey.MASTER_LOCK_POOL_INITSIZE),
          ServerConfiguration.getInt(PropertyKey.MASTER_LOCK_POOL_LOW_WATERMARK),
          ServerConfiguration.getInt(PropertyKey.MASTER_LOCK_POOL_HIGH_WATERMARK),
          ServerConfiguration.getInt(PropertyKey.MASTER_LOCK_POOL_CONCURRENCY_LEVEL));

  /**
   * Locks for guarding changes to last modified time and size on read-locked parent inodes.
   *
   * When renaming, creating, or deleting, we update the last modified time, last access time
   * and size of the parent inode while holding only a read lock. In the presence of concurrent
   * operations, this could cause the last modified time to decrease, or lead to incorrect
   * directory sizes. To avoid this, we guard the parent inode read-modify-write with this lock.
   * To avoid deadlock, a thread should never acquire more than one of these locks at the same time,
   * and no other locks should be taken while holding one of these locks.
   */
  private final Striped<Lock> mParentUpdateLocks = Striped.lock(1_000);

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

  /**
   * Creates a new instance of {@link InodeLockManager}.
   */
  public InodeLockManager() {
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_INODE_LOCK_POOL_SIZE.getName(),
        () -> mInodeLocks.size());
    MetricsSystem.registerGaugeIfAbsent(
        MetricKey.MASTER_EDGE_LOCK_POOL_SIZE.getName(),
        () -> mEdgeLocks.size());
  }

  @VisibleForTesting
  boolean inodeReadLockedByCurrentThread(long inodeId) {
    return mInodeLocks.getRawReadWriteLock(inodeId).getReadHoldCount() > 0;
  }

  @VisibleForTesting
  boolean inodeWriteLockedByCurrentThread(long inodeId) {
    return mInodeLocks.getRawReadWriteLock(inodeId).getWriteHoldCount() > 0;
  }

  @VisibleForTesting
  boolean edgeReadLockedByCurrentThread(Edge edge) {
    return mEdgeLocks.getRawReadWriteLock(edge).getReadHoldCount() > 0;
  }

  @VisibleForTesting
  boolean edgeWriteLockedByCurrentThread(Edge edge) {
    return mEdgeLocks.getRawReadWriteLock(edge).getWriteHoldCount() > 0;
  }

  /**
   * Asserts that all locks have been released, throwing an exception if any locks are still taken.
   */
  @VisibleForTesting
  public void assertAllLocksReleased() {
    assertAllLocksReleased(mEdgeLocks);
    assertAllLocksReleased(mInodeLocks);
  }

  private <T> void assertAllLocksReleased(LockPool<T> pool) {
    for (Entry<T, ReentrantReadWriteLock> entry : pool.getEntryMap().entrySet()) {
      ReentrantReadWriteLock lock = entry.getValue();
      if (lock.isWriteLocked()) {
        throw new RuntimeException(
            String.format("Found a write-locked lock for %s", entry.getKey()));
      }
      if (lock.getReadLockCount() > 0) {
        throw new RuntimeException(
            String.format("Found a read-locked lock for %s", entry.getKey()));
      }
    }
  }

  /**
   * Acquires an inode lock.
   *
   * @param inode the inode to lock
   * @param mode the mode to lock in
   * @param useTryLock whether to acquire with {@link Lock#tryLock()} or {@link Lock#lock()}. This
   *                   method differs from {@link #tryLockInode(Long, LockMode)} because it will
   *                   block until the inode has been successfully locked.
   * @return a lock resource which must be closed to release the lock
   * @see #tryLockInode(Long, LockMode)
   */
  public RWLockResource lockInode(InodeView inode, LockMode mode, boolean useTryLock) {
    return mInodeLocks.get(inode.getId(), mode, useTryLock);
  }

  /**
   * Attempts to acquire an inode lock.
   *
   * @param inodeId the inode id to try locking
   * @param mode the mode to lock in
   * @return either an empty optional, or a lock resource which must be closed to release the lock
   */
  public Optional<RWLockResource> tryLockInode(Long inodeId, LockMode mode) {
    return mInodeLocks.tryGet(inodeId, mode);
  }

  /**
   * Acquires an edge lock.
   *
   * @param edge the edge to lock
   * @param mode the mode to lock in
   * @param useTryLock whether to acquire with {@link Lock#tryLock()} or {@link Lock#lock()}. This
   *                   method differs from {@link #tryLockEdge(Edge, LockMode)} because it will
   *                   block until the edge has been successfully locked.
   * @return a lock resource which must be closed to release the lock
   * @see #tryLockEdge(Edge, LockMode)
   */
  public RWLockResource lockEdge(Edge edge, LockMode mode, boolean useTryLock) {
    return mEdgeLocks.get(edge, mode, useTryLock);
  }

  /**
   * Attempts to acquire an edge lock.
   *
   * @param edge the edge to try locking
   * @param mode the mode to lock in
   * @return either an empty optional, or a lock resource which must be closed to release the lock
   */
  public Optional<RWLockResource> tryLockEdge(Edge edge, LockMode mode) {
    return mEdgeLocks.tryGet(edge, mode);
  }

  /**
   * Tries to acquire a lock for persisting the specified inode id.
   *
   * @param inodeId the inode to acquire the lock for
   * @return an optional wrapping a closure for releasing the lock on success, or Optional.empty if
   *         the lock is already taken
   */
  public Optional<Scoped> tryAcquirePersistingLock(long inodeId) {
    AtomicBoolean lock = mPersistingLocks.getUnchecked(inodeId);
    if (lock.compareAndSet(false, true)) {
      return Optional.of(() -> lock.set(false));
    }
    return Optional.empty();
  }

  /**
   * Acquires the lock for modifying an inode's last modified time or size. As a pre-requisite, the
   * current thread should already hold a read lock on the inode.
   *
   * @param inodeId the id of the inode to lock
   * @return a lock resource which must be closed to release the lock
   */
  public LockResource lockUpdate(long inodeId) {
    return new LockResource(mParentUpdateLocks.get(inodeId));
  }

  @Override
  public void close() throws IOException {
    mInodeLocks.close();
    mEdgeLocks.close();
  }
}
