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

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.resource.LockResource;
import alluxio.resource.ResourcePool;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
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
      ServerConfiguration.getInt(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS)) {
    @Override
    public void close() {}

    @Override
    public ClientRWLock createNewResource() {
      return new ClientRWLock();
    }
  };

  /** A map from block id to the read write lock used to guard that block. */
  @GuardedBy("mSharedMapsLock")
  private final Map<Long, ClientRWLock> mLocks = new HashMap<>();

  /** A map from a session id to all the locks hold by this session. */
  @GuardedBy("mSharedMapsLock")
  private final Map<Long, Set<Long>> mSessionIdToLockIdsMap = new HashMap<>();

  /** A map from a lock id to the lock record of it. */
  @GuardedBy("mSharedMapsLock")
  private final Map<Long, LockRecord> mLockIdToRecordMap = new HashMap<>();

  /** Lock to guard metadata operations. */
  private final ReentrantReadWriteLock mSharedMapsLock = new ReentrantReadWriteLock();

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
    return lockBlockInternal(sessionId, blockId, blockLockType, true, null, null);
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
  public long tryLockBlock(long sessionId, long blockId, BlockLockType blockLockType,
      long time, TimeUnit unit) {
    return lockBlockInternal(sessionId, blockId, blockLockType, false, time, unit);
  }

  private long lockBlockInternal(long sessionId, long blockId, BlockLockType blockLockType,
      boolean blocking, @Nullable Long time, @Nullable TimeUnit unit) {
    ClientRWLock blockLock = getBlockLock(blockId);
    Lock lock = blockLockType == BlockLockType.READ ? blockLock.readLock() : blockLock.writeLock();
    // Make sure the session isn't already holding the block lock.
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
          return BlockWorker.INVALID_LOCK_ID;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return BlockWorker.INVALID_LOCK_ID;
      }
    }
    try {
      long lockId = LOCK_ID_GEN.getAndIncrement();
      try (LockResource r = new LockResource(mSharedMapsLock.writeLock())) {
        mLockIdToRecordMap.put(lockId, new LockRecord(sessionId, blockId, lock));
        Set<Long> sessionLockIds = mSessionIdToLockIdsMap.get(sessionId);
        if (sessionLockIds == null) {
          mSessionIdToLockIdsMap.put(sessionId, Sets.newHashSet(lockId));
        } else {
          sessionLockIds.add(lockId);
        }
      }
      return lockId;
    } catch (Throwable e) {
      // If an unexpected exception occurs, we should release the lock to be conservative.
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
    try (LockResource r = new LockResource(mSharedMapsLock.readLock())) {
      Set<Long> sessionLocks = mSessionIdToLockIdsMap.get(sessionId);
      if (sessionLocks == null) {
        return false;
      }
      for (Long lockId : sessionLocks) {
        LockRecord lockRecord = mLockIdToRecordMap.get(lockId);
        if (lockRecord.getBlockId() == blockId) {
          return true;
        }
      }
      return false;
    }
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
      ClientRWLock blockLock;
      // Check whether a lock has already been allocated for the block id.
      try (LockResource r = new LockResource(mSharedMapsLock.readLock())) {
        blockLock = mLocks.get(blockId);
        if (blockLock != null) {
          blockLock.addReference();
          return blockLock;
        }
      }
      // Since a block lock hasn't already been allocated, try to acquire a new one from the pool.
      // Acquire the lock outside the synchronized section because #acquire might need to block.
      // We shouldn't wait indefinitely in acquire because the another lock for this block could be
      // allocated to another thread, in which case we could just use that lock.
      blockLock = mLockPool.acquire(1, TimeUnit.SECONDS);
      if (blockLock != null) {
        try (LockResource r = new LockResource(mSharedMapsLock.writeLock())) {
          // Check if someone else acquired a block lock for blockId while we were acquiring one.
          if (mLocks.containsKey(blockId)) {
            mLockPool.release(blockLock);
            blockLock = mLocks.get(blockId);
          } else {
            mLocks.put(blockId, blockLock);
          }
          blockLock.addReference();
          return blockLock;
        }
      }
    }
  }

  /**
   * Releases the lock with the specified lock id.
   *
   * @param lockId the id of the lock to release
   * @return whether the lock corresponding the lock ID has been successfully unlocked
   */
  public boolean unlockBlockNoException(long lockId) {
    Lock lock;
    LockRecord record;
    try (LockResource r = new LockResource(mSharedMapsLock.writeLock())) {
      record = mLockIdToRecordMap.get(lockId);
      if (record == null) {
        return false;
      }
      long sessionId = record.getSessionId();
      lock = record.getLock();
      mLockIdToRecordMap.remove(lockId);
      Set<Long> sessionLockIds = mSessionIdToLockIdsMap.get(sessionId);
      sessionLockIds.remove(lockId);
      if (sessionLockIds.isEmpty()) {
        mSessionIdToLockIdsMap.remove(sessionId);
      }
    }
    unlock(lock, record.getBlockId());
    return true;
  }

  /**
   * Releases the lock with the specified lock id.
   *
   * @param lockId the id of the lock to release
   * @throws BlockDoesNotExistException if lock id cannot be found
   */
  public void unlockBlock(long lockId) throws BlockDoesNotExistException {
    Lock lock;
    LockRecord record;
    try (LockResource r = new LockResource(mSharedMapsLock.writeLock())) {
      record = mLockIdToRecordMap.get(lockId);
      if (record == null) {
        throw new BlockDoesNotExistException(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID,
            lockId);
      }
      long sessionId = record.getSessionId();
      lock = record.getLock();
      mLockIdToRecordMap.remove(lockId);
      Set<Long> sessionLockIds = mSessionIdToLockIdsMap.get(sessionId);
      sessionLockIds.remove(lockId);
      if (sessionLockIds.isEmpty()) {
        mSessionIdToLockIdsMap.remove(sessionId);
      }
    }
    unlock(lock, record.getBlockId());
  }

  /**
   * Releases the lock with the specified session and block id.
   *
   * @param sessionId the session id
   * @param blockId the block id
   * @return whether the block has been successfully unlocked
   */
  // TODO(bin): Temporary, remove me later.
  public boolean unlockBlock(long sessionId, long blockId) {
    try (LockResource r = new LockResource(mSharedMapsLock.writeLock())) {
      Set<Long> sessionLockIds = mSessionIdToLockIdsMap.get(sessionId);
      if (sessionLockIds == null) {
        return false;
      }
      for (long lockId : sessionLockIds) {
        LockRecord record = mLockIdToRecordMap.get(lockId);
        if (record == null) {
          // TODO(peis): Should this be a check failure?
          return false;
        }
        if (blockId == record.getBlockId()) {
          mLockIdToRecordMap.remove(lockId);
          sessionLockIds.remove(lockId);
          if (sessionLockIds.isEmpty()) {
            mSessionIdToLockIdsMap.remove(sessionId);
          }
          Lock lock = record.getLock();
          unlock(lock, blockId);
          return true;
        }
      }
      return false;
    }
  }

  /**
   * Validates the lock is hold by the given session for the given block.
   *
   * @param sessionId the session id
   * @param blockId the block id
   * @param lockId the lock id
   * @throws BlockDoesNotExistException when no lock record can be found for lock id
   * @throws InvalidWorkerStateException when session id or block id is not consistent with that
   *         in the lock record for lock id
   */
  public void validateLock(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    try (LockResource r = new LockResource(mSharedMapsLock.readLock())) {
      LockRecord record = mLockIdToRecordMap.get(lockId);
      if (record == null) {
        throw new BlockDoesNotExistException(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID,
            lockId);
      }
      if (sessionId != record.getSessionId()) {
        throw new InvalidWorkerStateException(ExceptionMessage.LOCK_ID_FOR_DIFFERENT_SESSION,
            lockId, record.getSessionId(), sessionId);
      }
      if (blockId != record.getBlockId()) {
        throw new InvalidWorkerStateException(ExceptionMessage.LOCK_ID_FOR_DIFFERENT_BLOCK, lockId,
            record.getBlockId(), blockId);
      }
    }
  }

  /**
   * Cleans up the locks currently hold by a specific session.
   *
   * @param sessionId the id of the session to cleanup
   */
  public void cleanupSession(long sessionId) {
    try (LockResource r = new LockResource(mSharedMapsLock.writeLock())) {
      Set<Long> sessionLockIds = mSessionIdToLockIdsMap.get(sessionId);
      if (sessionLockIds == null) {
        return;
      }
      for (long lockId : sessionLockIds) {
        LockRecord record = mLockIdToRecordMap.get(lockId);
        if (record == null) {
          LOG.error(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID.getMessage(lockId));
          continue;
        }
        Lock lock = record.getLock();
        unlock(lock, record.getBlockId());
        mLockIdToRecordMap.remove(lockId);
      }
      mSessionIdToLockIdsMap.remove(sessionId);
    }
  }

  /**
   * Gets a set of currently locked blocks.
   *
   * @return a set of locked blocks
   */
  public Set<Long> getLockedBlocks() {
    try (LockResource r = new LockResource(mSharedMapsLock.readLock())) {
      Set<Long> set = new HashSet<>();
      for (LockRecord lockRecord : mLockIdToRecordMap.values()) {
        set.add(lockRecord.getBlockId());
      }
      return set;
    }
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
    try (LockResource r = new LockResource(mSharedMapsLock.writeLock())) {
      ClientRWLock lock = mLocks.get(blockId);
      if (lock == null) {
        // Someone else probably released the block lock already.
        return;
      }
      // If we were the last worker with a reference to the lock, clean it up.
      if (lock.dropReference() == 0) {
        mLocks.remove(blockId);
        mLockPool.release(lock);
      }
    }
  }

  /**
   * Checks the internal state of the manager to make sure invariants hold.
   *
   * This method is intended for testing purposes. A runtime exception will be thrown if invalid
   * state is encountered.
   */
  public void validate() {
    try (LockResource r = new LockResource(mSharedMapsLock.readLock())) {
      // Compute block lock reference counts based off of lock records
      ConcurrentMap<Long, AtomicInteger> blockLockReferenceCounts = new ConcurrentHashMap<>();
      for (LockRecord record : mLockIdToRecordMap.values()) {
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

      // Check that if a lock id is mapped to by a session id, the lock record for that lock id
      // contains that session id.
      for (Entry<Long, Set<Long>> entry : mSessionIdToLockIdsMap.entrySet()) {
        for (Long lockId : entry.getValue()) {
          LockRecord record = mLockIdToRecordMap.get(lockId);
          if (record.getSessionId() != entry.getKey()) {
            throw new IllegalStateException("The session id map contains lock id " + lockId
                + "under session id " + entry.getKey() + ", but the record for that lock id ("
                + record + ")" + " doesn't contain that session id");
          }
        }
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
    private final Lock mLock;

    /** Creates a new instance of {@link LockRecord}.
     *
     * @param sessionId the session id
     * @param blockId the block id
     * @param lock the lock
     */
    LockRecord(long sessionId, long blockId, Lock lock) {
      mSessionId = sessionId;
      mBlockId = blockId;
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
     * @return the lock
     */
    Lock getLock() {
      return mLock;
    }
  }
}
