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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

  /** The number of locks, larger value leads to finer locking granularity, but more space. */
  private static final int NUM_LOCKS =
      ServerConfiguration.getInt(PropertyKey.WORKER_TIERED_STORE_BLOCK_LOCKS);

  /** Invalid lock ID. */
  public static final long INVALID_LOCK_ID = -1;

  /** The unique id of each lock. */
  private static final AtomicLong LOCK_ID_GEN = new AtomicLong(0);

  /** A hashing function to map block id to one of the locks. */
  private static final HashFunction HASH_FUNC = Hashing.murmur3_32();

  /** An array of read write locks. */
  private final ClientRWLock[] mLockArray = new ClientRWLock[NUM_LOCKS];

  /** A map from a session id to all the locks hold by this session. */
  @GuardedBy("mSharedMapsLock")
  private final Map<Long, Set<Long>> mSessionIdToLockIdsMap;

  /** A map from a lock id to the lock record of it. */
  @GuardedBy("mSharedMapsLock")
  private final Map<Long, LockRecord> mLockIdToRecordMap;

  /** Lock to guard metadata operations. */
  private final ReentrantReadWriteLock mSharedMapsLock = new ReentrantReadWriteLock();

  /**
   * @param blockId the id of the block
   * @return hash index of the lock
   */
  private static int blockHashIndex(long blockId) {
    return Math.abs(HASH_FUNC.hashLong(blockId).asInt()) % NUM_LOCKS;
  }

  /**
   * Constructs a new {@link BlockLockManager}.
   */
  public BlockLockManager() {
    this(new HashMap<>(), new HashMap<>());
  }

  /**
   * Constructs a new {@link BlockLockManager}.
   */
  @VisibleForTesting
  BlockLockManager(Map<Long, Set<Long>> sessionIdToLockIdsMap,
      Map<Long, LockRecord> lockIdToRecordMap) {
    mSessionIdToLockIdsMap = sessionIdToLockIdsMap;
    mLockIdToRecordMap = lockIdToRecordMap;
    for (int i = 0; i < NUM_LOCKS; i++) {
      mLockArray[i] = new ClientRWLock();
    }
  }


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
    // hashing block id into the range of [0, NUM_LOCKS - 1]
    int index = blockHashIndex(blockId);
    ClientRWLock blockLock = mLockArray[index];
    Lock lock;
    if (blockLockType == BlockLockType.READ) {
      lock = blockLock.readLock();
    } else {
      // Make sure the session isn't already holding the block lock.
      if (sessionHoldsLock(sessionId, blockId)) {
        throw new IllegalStateException(String
            .format("Session %s attempted to take a write lock on block %s, but the session already"
                + " holds a lock on the block", sessionId, blockId));
      }
      lock = blockLock.writeLock();
    }
    lock.lock();
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
    } catch (RuntimeException e) {
      // If an unexpected exception occurs, we should release the lock to be conservative.
      lock.unlock();
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
    lock.unlock();
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
          lock.unlock();
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
        lock.unlock();
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
   * Inner class to keep record of a lock.
   */
  @ThreadSafe
  @VisibleForTesting
  static final class LockRecord {
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
