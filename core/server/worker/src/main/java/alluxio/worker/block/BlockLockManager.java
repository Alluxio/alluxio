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

package alluxio.worker.block;

import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.collections.Pair;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.resource.ResourcePool;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Handle all block locks.
 */
@ThreadSafe
public final class BlockLockManager {
  private static final Logger LOG = LoggerFactory.getLogger(BlockLockManager.class);

  /** The unique id of each lock. */
  private static final AtomicLong LOCK_ID_GEN = new AtomicLong(0);

 /** A pool of read write locks. */
  private final ResourcePool<ClientRWLock> mLockPool = new ResourcePool<ClientRWLock>(
      Configuration.getInt(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS)) {
    @Override
    public void close() {}

    @Override
    public ClientRWLock createNewResource() {
      return new ClientRWLock();
    }
  };

  /** A map from block id to the read write lock used to guard that block. */
  // todo(bowen): set concurrency level?
  private final ConcurrentHashMap<Long, ClientRWLock> mLocks = new ConcurrentHashMap<>();

  /**
   * The permits are effectively unlimited for concurrently adding new records which only
   * acquires 1 permit, but session cleanup takes all permits here to be mutually exclusive to
   * adding new records.
   */
  private static final int SESSION_SEMAPHORE_PERMITS = Integer.MAX_VALUE;

  /**
   * A semaphore used to prevent concurrent adding and removing of records when
   * session clean-up is in progress. During session cleaning, to avoid race conditions,
   * no new lock records should be added to {@link #mLockRecords}, but retrievals and removals
   * are OK since the underlying concurrent map handles that atomically.
   */
  final Semaphore mSessionCleaning = new Semaphore(SESSION_SEMAPHORE_PERMITS);

  /**
   * Records of locks currently held by clients. New entries added to the set must be protected
   * from concurrent removals due to session clean-ups.
   */
  private final IndexedSet<LockRecord> mLockRecords =
      new IndexedSet<>(INDEX_LOCK_ID, INDEX_BLOCK_ID, INDEX_SESSION_ID, INDEX_SESSION_BLOCK_ID);

  private static final IndexDefinition<LockRecord, Pair<Long, Long>> INDEX_SESSION_BLOCK_ID =
      IndexDefinition.ofNonUnique(
          lockRecord -> new Pair<>(lockRecord.getSessionId(), lockRecord.getBlockId()));

  private static final IndexDefinition<LockRecord, Long> INDEX_BLOCK_ID =
      IndexDefinition.ofNonUnique(LockRecord::getBlockId);

  private static final IndexDefinition<LockRecord, Long> INDEX_LOCK_ID =
      IndexDefinition.ofUnique(LockRecord::getLockId);

  private static final IndexDefinition<LockRecord, Long> INDEX_SESSION_ID =
      IndexDefinition.ofNonUnique(LockRecord::getSessionId);

  /**
   * Constructs a new {@link BlockLockManager}.
   */
  public BlockLockManager() {}

  /**
   * Locks a block. Note that even if this block does not exist, a lock id is still returned.
   *
   * If all {@link PropertyKey#WORKER_TIERED_STORE_BLOCK_LOCKS} are already in use and no lock has
   * been allocated for the specified block, this method will need to wait until a lock can be
   * acquired from the lock pool.
   *
   * @param sessionId the session id
   * @param blockId the block id
   * @param blockLockType {@link BlockLockType#READ} or {@link BlockLockType#WRITE}
   * @return lock id
   */
  public long lockBlock(long sessionId, long blockId, BlockLockType blockLockType) {
    OptionalLong lockId = lockBlockInternal(sessionId, blockId, blockLockType, true, null, null);
    Preconditions.checkState(lockId.isPresent(), "lockBlock should always return a lockId");
    return lockId.getAsLong();
  }

  /**
   * Tries to lock a block within the given time.
   * Note that even if this block does not exist, a lock id is still returned.
   *
   * If all {@link PropertyKey#WORKER_TIERED_STORE_BLOCK_LOCKS} are already in use and no lock has
   * been allocated for the specified block, this method will need to wait until a lock can be
   * acquired from the lock pool.
   *
   * @param sessionId the session id
   * @param blockId the block id
   * @param blockLockType {@link BlockLockType#READ} or {@link BlockLockType#WRITE}
   * @param time the maximum time to wait for the lock
   * @param unit the time unit of the {@code time} argument
   * @return lock id or INVALID_LOCK_ID if not able to lock within the given time
   */
  public OptionalLong tryLockBlock(long sessionId, long blockId, BlockLockType blockLockType,
      long time, TimeUnit unit) {
    return lockBlockInternal(sessionId, blockId, blockLockType, false, time, unit);
  }

  private OptionalLong lockBlockInternal(long sessionId, long blockId, BlockLockType blockLockType,
      boolean blocking, @Nullable Long time, @Nullable TimeUnit unit) {
    ClientRWLock blockLock = getBlockLock(blockId);
    Lock lock = blockLockType == BlockLockType.READ ? blockLock.readLock() : blockLock.writeLock();
    // Make sure the session isn't already holding the block lock.
    // todo(bowen): This is a best-effort check and is subject to race condition.
    // Remove this check and implement better error signaling or retry.
    // When the said race condition occurs, the write lock will block indefinitely until the
    // reader releases the lock. In case the reader panics while holding the lock, this will lead
    // to a deadlock. This can happen e.g. when worker tries to move a block to a new location while
    // a client is actively reading it.
    if (blockLockType == BlockLockType.WRITE && sessionHoldsLock(sessionId, blockId)) {
      throw new IllegalStateException(String
          .format("Session %s attempted to take a write lock on block %s, but the session already"
              + " holds a lock on the block", sessionId, blockId));
    }
    if (blocking) {
      lock.lock();
    } else {
      Preconditions.checkNotNull(time, "time");
      Preconditions.checkNotNull(unit, "unit");
      try {
        if (!lock.tryLock(time, unit)) {
          LOG.warn("Failed to acquire lock for block {} after {} {}.  "
                  + "session: {}, blockLockType: {}, lock reference count = {}",
              blockId, time, unit, sessionId, blockLockType,
              blockLock.getReferenceCount());
          return OptionalLong.empty();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return OptionalLong.empty();
      }
    }
    long lockId = LOCK_ID_GEN.getAndIncrement();
    LockRecord record = new LockRecord(sessionId, blockId, lockId, lock);
    try {
      try {
        mSessionCleaning.acquire();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for session clean up, sessionId={}, blockId={}",
            sessionId, blockId);
        unlock(lock, blockId);
        Thread.currentThread().interrupt();
        return OptionalLong.empty();
      }
      mLockRecords.add(record);
      mSessionCleaning.release();
      return OptionalLong.of(lockId);
    } catch (Throwable e) {
      // If an unexpected exception occurs, we should release the lock to be conservative.
      mLockRecords.remove(record);
      unlock(lock, blockId);
      throw e;
    }
  }

  /**
   * @param sessionId the session id to check
   * @param blockId the block id to check
   * @return whether the specified session holds a lock on the specified block
   */
  private boolean sessionHoldsLock(long sessionId, long blockId) {
    Set<LockRecord> sessionRecords =
        mLockRecords.getByField(INDEX_SESSION_BLOCK_ID, new Pair<>(sessionId, blockId));
    return !sessionRecords.isEmpty();
  }

  /**
   * Returns the block lock for the given block id, acquiring such a lock if it doesn't exist yet.
   *
   * If all locks have been allocated, this method will block until one can be acquired.
   *
   * @param blockId the block id to get the lock for
   * @return the block lock
   */
  private ClientRWLock getBlockLock(long blockId) {
    // Loop until we either find the block lock in the mLocks map, or successfully acquire a new
    // block lock from the lock pool.
    while (true) {
      // Check whether a lock has already been allocated for the block id.
      ClientRWLock reuseExistingLock = mLocks.computeIfPresent(
          blockId,
          (blkid, lock) -> {
            lock.addReference();
            return lock;
          }
      );
      if (reuseExistingLock != null) {
        return reuseExistingLock;
      }
      // Since a block lock hasn't already been allocated, try to acquire a new one from the pool.
      // We shouldn't wait indefinitely in acquire because the another lock for this block could be
      // allocated to another thread, in which case we could just use that lock.
      ClientRWLock newlyAcquiredLock = mLockPool.acquire(1, TimeUnit.SECONDS);
      if (newlyAcquiredLock != null) {
        int referenceCount = newlyAcquiredLock.getReferenceCount();
        if (referenceCount != 0) {
          LOG.error("A block lock was not cleanly released as newly acquired locks should have 0 "
              + "references, but got {}", referenceCount);
        }
        ClientRWLock computed = mLocks.compute(blockId, (id, lock) -> {
          // Check if someone else acquired a block lock for blockId while we were acquiring one.
          if (lock != null) {
            lock.addReference();
            return lock;
          } else {
            newlyAcquiredLock.addReference();
            return newlyAcquiredLock;
          }
        });
        if (computed != newlyAcquiredLock) {
          // reuse someone else's lock and release the unused lock
          mLockPool.release(newlyAcquiredLock);
        }
        return computed;
      }
    }
  }

  /**
   * Releases the lock with the specified lock id.
   *
   * @param lockId the id of the lock to release
   */
  public void unlockBlock(long lockId) {
    LockRecord record = mLockRecords.getFirstByField(INDEX_LOCK_ID, lockId);
    if (record == null) {
      return;
    }
    // the record may have been removed by someone else
    // after we retrieved it, so a check is necessary
    if (mLockRecords.remove(record)) {
      unlock(record.getLock(), record.getBlockId());
    }
  }

  /**
   * Validates the lock is hold by the given session for the given block.
   *
   * @param sessionId the session id
   * @param blockId the block id
   * @param lockId the lock id
   * @return hold or not
   */
  @VisibleForTesting
  public boolean checkLock(long sessionId, long blockId, long lockId) {
    LockRecord record = mLockRecords.getFirstByField(INDEX_LOCK_ID, lockId);
    return record != null && record.getSessionId() == sessionId && record.getBlockId() == blockId;
  }

  /**
   * Cleans up the locks currently hold by a specific session.
   *
   * @param sessionId the id of the session to cleanup
   */
  public void cleanupSession(long sessionId) {
    // acquire all permits of the semaphore so that no new lock records can be added
    mSessionCleaning.acquireUninterruptibly(SESSION_SEMAPHORE_PERMITS);
    try {
      Set<LockRecord> records = mLockRecords.getByField(INDEX_SESSION_ID, sessionId);
      if (records == null) {
        return;
      }
      // NOTE: iterating through an ConcurrentHashSet is not done atomically
      // this section must be protected from concurrently adding new records that belong to
      // the same session
      for (LockRecord record : records) {
        mLockRecords.remove(record);
        unlock(record.getLock(), record.getBlockId());
      }
    } finally {
      mSessionCleaning.release(SESSION_SEMAPHORE_PERMITS);
    }
  }

  /**
   * Gets a snapshot of currently locked blocks.
   *
   * @return a set of locked blocks
   */
  public Set<Long> getLockedBlocks() {
    Set<Long> set = new HashSet<>();
    // NOTE: iterating through an IndexedSet is not done atomically
    // we may end up with stale information in the resulting set
    // but the set is merely meant to be a snapshot of the locked blocks
    // and is stale as soon as this method returns
    for (LockRecord lockRecord : mLockRecords) {
      set.add(lockRecord.getBlockId());
    }
    return set;
  }

  /**
   * Unlocks the given lock and releases the block lock for the given block id if the lock no longer
   * in use.
   *
   * @param lock the lock to unlock
   * @param blockId the block id for which to potentially release the block lock
   */
  private void unlock(Lock lock, long blockId) {
    lock.unlock();
    releaseBlockLockIfUnused(blockId);
  }

  /**
   * Checks whether anyone is using the block lock for the given block id, returning the lock to
   * the lock pool if it is unused.
   *
   * @param blockId the block id for which to potentially release the block lock
   */
  private void releaseBlockLockIfUnused(long blockId) {
    mLocks.computeIfPresent(
        blockId,
        (blkid, lock) -> {
          // If we were the last worker with a reference to the lock, clean it up.
          if (lock.dropReference() == 0) {
            mLockPool.release(lock);
            return null;
          }
          return lock;
        }
    );
  }

  /**
   * Checks the internal state of the manager to make sure invariants hold.
   *
   * This method is intended for testing purposes. A runtime exception will be thrown if invalid
   * state is encountered. This method should only be called when there are no other concurrent
   * threads accessing this manager.
   */
  @VisibleForTesting
  public void validate() {
    // Compute block lock reference counts based off of lock records
    ConcurrentMap<Long, AtomicInteger> blockLockReferenceCounts = new ConcurrentHashMap<>();
    // NOTE: iterating through an IndexedSet is not done atomically
    // the counts are valid only when the caller ensures no concurrent access from other threads
    for (LockRecord record : mLockRecords) {
      blockLockReferenceCounts.putIfAbsent(record.getBlockId(), new AtomicInteger(0));
      blockLockReferenceCounts.get(record.getBlockId()).incrementAndGet();
    }

    // Check that the reference count for each block lock matches the lock record counts.
    for (Entry<Long, ClientRWLock> entry : mLocks.entrySet()) {
      long blockId = entry.getKey();
      ClientRWLock lock = entry.getValue();
      Integer recordCount = blockLockReferenceCounts.get(blockId).get();
      Integer referenceCount = lock.getReferenceCount();
      if (!Objects.equal(recordCount, referenceCount)) {
        throw new IllegalStateException("There are " + recordCount + " lock records for block"
            + " id " + blockId + ", but the reference count is " + referenceCount);
      }
    }
  }

  /**
   * Inner class to keep record of a lock.
   */
  @ThreadSafe
  private static final class LockRecord {
    private final long mSessionId;
    private final long mBlockId;
    private final long mLockId;
    private final Lock mLock;

    /** Creates a new instance of {@link LockRecord}.
     *
     * @param sessionId the session id
     * @param blockId the block id
     * @param lock the lock
     */
    LockRecord(long sessionId, long blockId, long lockId, Lock lock) {
      mSessionId = sessionId;
      mBlockId = blockId;
      mLockId = lockId;
      mLock = lock;
    }

    /**
     * @return the session id
     */
    long getSessionId() {
      return mSessionId;
    }

    /**
     * @return the block id
     */
    long getBlockId() {
      return mBlockId;
    }

    /**
     * @return the lock id
     */
    long getLockId() {
      return mLockId;
    }

    /**
     * @return the lock
     */
    Lock getLock() {
      return mLock;
    }
  }
}
