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

import alluxio.resource.LockResource;
import alluxio.util.interfaces.Scoped;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Class for managing inode locking.
 */
public class InodeLocker {
  private final LoadingCache<Long, ReentrantReadWriteLock> mInodeLocks =
      CacheBuilder.<Long, ReentrantReadWriteLock>newBuilder()
          .weakValues()
          .initialCapacity(1_000)
          .concurrencyLevel(100)
          .build(new CacheLoader<Long, ReentrantReadWriteLock>() {
            @Override
            public ReentrantReadWriteLock load(Long key) {
              return new ReentrantReadWriteLock();
            }
          });

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

  private ReentrantReadWriteLock getInodeLock(long id) {
    try {
      return mInodeLocks.get(id);
    } catch (ExecutionException e) {
      // This should never happen since our #load implementation doesn't throw anything.
      throw new RuntimeException(e);
    }
  }

  /**
   * @param inodeId an inode to read lock
   * @return a lock resource that must be closed to release the lock
   */
  public LockResource readLock(long inodeId) {
    return new LockResource(getInodeLock(inodeId).readLock());
  }

  /**
   * @param inodeId an inode to write lock
   * @return a lock resource that must be closed to release the lock
   */
  public LockResource writeLock(long inodeId) {
    return new LockResource(getInodeLock(inodeId).writeLock());
  }

  /**
   * @param inodeId an inode id
   * @return whether the inode is write-locked by the current thread
   */
  public boolean isWriteLockedByCurrentThread(long inodeId) {
    return getInodeLock(inodeId).isWriteLockedByCurrentThread();
  }

  /**
   * @param inodeId an inode id
   * @return whether the inode is read-locked by the current thread
   */
  public boolean isReadLockedByCurrentThread(long inodeId) {
    return getInodeLock(inodeId).getReadHoldCount() > 0;
  }

  /**
   * @param inodeId an inode id
   * @return whether the inode is read-locked or write-locked by the current thread
   */
  public boolean isLockedByCurrentThread(long inodeId) {
    return isReadLockedByCurrentThread(inodeId) || isWriteLockedByCurrentThread(inodeId);
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
