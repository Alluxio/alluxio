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

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.resource.LockResource;
import alluxio.resource.ResourcePool;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
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

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Handle all block locks.
 */
@ThreadSafe
public final class BlockLockManager {
  private static final Logger LOG = LoggerFactory.getLogger(BlockLockManager.class);
  private static final long LOCK_TIMEOUT_MS
      = ServerConfiguration.getMs(PropertyKey.WORKER_BLOCK_LOCK_TIMEOUT);

  /** Invalid lock ID. */
  public static final long INVALID_LOCK_ID = -1;

  /** The unique id of each lock. */
  private static final AtomicLong LOCK_ID_GEN = new AtomicLong(0);

 /** A pool of read write locks. */
  private final ResourcePool<ClientRWLock> mLockPool = new ResourcePool<ClientRWLock>(
      ServerConfiguration.getInt(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS)) {
    @Override
    public void close() {}

    @Override
    protected ClientRWLock createNewResource() {
      return new ClientRWLock();
    }
  };

  /** A map from block id to the read write lock used to guard that block. */
  @GuardedBy("mSharedMapsLock")
  private final Map<Long, ClientRWLock> mLocks = new HashMap<>();

  /** A map from a block id to all the locks of this block. */
  @GuardedBy("mSharedMapsLock")
  private final Map<Long, Set<Long>> mBlockIdToLockIdsMap = new HashMap<>();

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
    ClientRWLock blockLock = getBlockLock(blockId);
    Lock lock;
    if (blockLockType == BlockLockType.READ) {
      lock = blockLock.readLock();
    } else {
      lock = blockLock.writeLock();
      removeTimeOutBlockLocks(blockId);
    }
    lock.lock();
    try {
      long lockId = LOCK_ID_GEN.getAndIncrement();
      try (LockResource r = new LockResource(mSharedMapsLock.writeLock())) {
        mLockIdToRecordMap.put(lockId, new LockRecord(blockId, lock));
        mBlockIdToLockIdsMap.computeIfAbsent(blockId, k -> new HashSet<>()).add(lockId);
      }
      return lockId;
    } catch (RuntimeException e) {
      // If an unexpected exception occurs, we should release the lock to be conservative.
      unlock(lock, blockId);
      throw e;
    }
  }

  /**
   * Removes the timeout locks belong to the given block id.
   *
   * @param blockId the block id
   */
  private void removeTimeOutBlockLocks(long blockId) {
    long currentTime = System.currentTimeMillis();
    try (LockResource r = new LockResource(mSharedMapsLock.writeLock())) {
      if (!mBlockIdToLockIdsMap.containsKey(blockId)) {
        return;
      }
      for (long lockId : mBlockIdToLockIdsMap.get(blockId)) {
        LockRecord record = mLockIdToRecordMap.get(lockId);
        if (currentTime - record.getLastAccessTime() > LOCK_TIMEOUT_MS) {
          if (unlockBlockNoException(lockId)) {
            LOG.debug("Removed outdated lock {} of block {}",
                lockId, blockId);
          } else {
            // Should not reach here
            LOG.warn("Record of lock {} belonging to block {} is missing.",
                lockId, blockId);
          }
        }
      }
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
      long blockId = record.getBlockId();
      lock = record.getLock();
      mLockIdToRecordMap.remove(lockId);
      Set<Long> blockLockIds = mBlockIdToLockIdsMap.get(blockId);
      blockLockIds.remove(lockId);
      if (blockLockIds.isEmpty()) {
        mBlockIdToLockIdsMap.remove(blockId);
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
    if (!unlockBlockNoException(lockId)) {
      throw new BlockDoesNotExistException(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID,
          lockId);
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
      if (blockId != record.getBlockId()) {
        throw new InvalidWorkerStateException(ExceptionMessage.LOCK_ID_FOR_DIFFERENT_BLOCK, lockId,
            record.getBlockId(), blockId);
      }
      record.updateLastAccessTime();
    }
  }

  /**
   * Handles the block lock heartbeat.
   *
   * @param lockId the block lock id
   * @return true if heartbeat successfully, false if unable to find the lock id
   */
  public boolean blockLockHeartbeat(long lockId) {
    try (LockResource r = new LockResource(mSharedMapsLock.readLock())) {
      LockRecord record = mLockIdToRecordMap.get(lockId);
      if (record == null) {
        return false;
      }
      record.updateLastAccessTime();
    }
    return true;
  }

  /**
   * Gets a set of currently locked blocks.
   *
   * @return a immutable set of locked blocks
   */
  public Set<Long> getLockedBlocks() {
    try (LockResource r = new LockResource(mSharedMapsLock.readLock())) {
      return ImmutableSet.copyOf(mBlockIdToLockIdsMap.keySet());
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

      for (Entry<Long, Set<Long>> entry : mBlockIdToLockIdsMap.entrySet()) {
        for (Long lockId : entry.getValue()) {
          LockRecord record = mLockIdToRecordMap.get(lockId);
          if (record.getBlockId() != entry.getKey()) {
            throw new IllegalStateException("The block id map contains lock id " + lockId
                + "under block id " + entry.getKey() + ", but the record for that lock id ("
                + record + ")" + " doesn't contain that block id");
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
    private final long mBlockId;
    private final Lock mLock;
    private AtomicLong mLastAccessTime;

    /** Creates a new instance of {@link LockRecord}.
     *
     * @param blockId the block id
     * @param lock the lock
     */
    LockRecord(long blockId, Lock lock) {
      mBlockId = blockId;
      mLock = lock;
      mLastAccessTime = new AtomicLong(System.currentTimeMillis());
    }

    /**
     * Updates the last access time to current time.
     */
    void updateLastAccessTime() {
      mLastAccessTime.set(System.currentTimeMillis());
    }

    /**
     * @return the last access time of this lock
     */
    long getLastAccessTime() {
      return mLastAccessTime.get();
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
