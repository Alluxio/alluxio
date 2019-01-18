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

import alluxio.collections.LockCache;
import alluxio.master.file.meta.InodeTree.LockMode;
import alluxio.resource.LockResource;
import alluxio.resource.RefCountLockResource;
import alluxio.util.interfaces.Scoped;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
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
public class InodeLockManager {
  /**
   * Cache for supplying inode locks. To lock an inode, its inode id must be searched in this
   * cache to get the appropriate read lock.
   *
   * We use weak values so that when nothing holds a reference to
   * a lock, the garbage collector can remove the lock's entry from the cache.
   */

  public final LockCache<Long, ReentrantReadWriteLock> mInodeLocks =
      new LockCache<>((key)-> new ReentrantReadWriteLock(), 1000, 10000, 100);
  /**
   * Cache for supplying edge locks, similar to mInodeLocks.
   */

  public final LockCache<Edge, ReentrantReadWriteLock> mEdgeLocks =
      new LockCache<>((key)-> new ReentrantReadWriteLock(), 1000, 10000, 100);

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
    LockCache<Long, ReentrantReadWriteLock>.ValNode
        valNode = mInodeLocks.get(inodeId);
    boolean result = valNode.get().getReadHoldCount() > 0;
    valNode.getRefCounter().decrementAndGet();
    return result;
  }

  @VisibleForTesting
  boolean inodeWriteLockedByCurrentThread(long inodeId) {
    LockCache<Long, ReentrantReadWriteLock>.ValNode
        valNode = mInodeLocks.get(inodeId);
    boolean result = valNode.get().getWriteHoldCount() > 0;
    valNode.getRefCounter().decrementAndGet();
    return result;
  }

  @VisibleForTesting
  boolean edgeReadLockedByCurrentThread(Edge edge) {
    LockCache<Edge, ReentrantReadWriteLock>.ValNode
        valNode = mEdgeLocks.get(edge);
    boolean result = valNode.get().getReadHoldCount() > 0;
    valNode.getRefCounter().decrementAndGet();
    return result;
  }

  @VisibleForTesting
  boolean edgeWriteLockedByCurrentThread(Edge edge) {
    LockCache<Edge, ReentrantReadWriteLock>.ValNode
        valNode = mEdgeLocks.get(edge);
    boolean result = valNode.get().getWriteHoldCount() > 0;
    valNode.getRefCounter().decrementAndGet();
    return result;
  }

  /**
   * Acquires an inode lock.
   *
   * @param inode the inode to lock
   * @param mode the mode to lock in
   * @return a lock resource which must be closed to release the lock
   */
  public LockResource lockInode(InodeView inode, LockMode mode) {
    LockCache<Long, ReentrantReadWriteLock>.ValNode
        valNode = mInodeLocks.get(inode.getId());
    return lock(valNode.get(), mode, valNode.getRefCounter());
  }

  /**
   * Acquires an edge lock.
   *
   * @param edge the edge to lock
   * @param mode the mode to lock in
   * @return a lock resource which must be closed to release the lock
   */
  public LockResource lockEdge(Edge edge, LockMode mode) {
    LockCache<Edge, ReentrantReadWriteLock>.ValNode
        valNode = mEdgeLocks.get(edge);
    return lock(valNode.get(), mode, valNode.getRefCounter());
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

  private LockResource lock(ReadWriteLock lock, LockMode mode, AtomicInteger refCounter) {
    switch (mode) {
      case READ:
        return new RefCountLockResource(lock.readLock(), refCounter);
      case WRITE:
        return new RefCountLockResource(lock.writeLock(), refCounter);
      default:
        throw new IllegalStateException("Unknown lock mode: " + mode);
    }
  }
}
