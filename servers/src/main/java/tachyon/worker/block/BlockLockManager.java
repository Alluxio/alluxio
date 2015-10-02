/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import tachyon.Constants;
import tachyon.exception.BlockDoesNotExistException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.InvalidWorkerStateException;
import tachyon.worker.WorkerContext;

/**
 * Handle all block locks.
 * <p>
 * This class is thread-safe.
 */
public final class BlockLockManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** The number of locks, larger value leads to finer locking granularity, but more space. */
  private static int sNumLocks = WorkerContext.getConf().getInt(Constants.WORKER_BLOCK_LOCK_COUNT);

  /** The unique id of each lock */
  private static final AtomicLong LOCK_ID_GEN = new AtomicLong(0);
  /** A hashing function to map blockId to one of the locks */
  private static final HashFunction HASH_FUNC = Hashing.murmur3_32();

  /** A map from a block ID to its lock */
  private final ClientRWLock[] mLockArray = new ClientRWLock[sNumLocks];
  /** A map from a session ID to all the locks hold by this session */
  private final Map<Long, Set<Long>> mSessionIdToLockIdsMap = new HashMap<Long, Set<Long>>();
  /** A map from a lock ID to the lock record of it */
  private final Map<Long, LockRecord> mLockIdToRecordMap = new HashMap<Long, LockRecord>();
  /** To guard access on mLockIdToRecordMap and mSessionIdToLockIdsMap */
  private final Object mSharedMapsLock = new Object();

  public BlockLockManager() {
    for (int i = 0; i < sNumLocks; i ++) {
      mLockArray[i] = new ClientRWLock();
    }
  }

  /**
   * Gets index of the lock that will be used to lock the block
   *
   * @param blockId the id of the block
   * @return hash index of the lock
   */
  public static int blockHashIndex(long blockId) {
    return Math.abs(HASH_FUNC.hashLong(blockId).asInt()) % sNumLocks;
  }

  /**
   * Locks a block. Note that, lock striping is used so even this block does not exist, a lock id is
   * still returned.
   *
   * @param sessionId the ID of session
   * @param blockId the ID of block
   * @param blockLockType READ or WRITE
   * @return lock ID
   */
  public long lockBlock(long sessionId, long blockId, BlockLockType blockLockType) {
    // hashing blockId into the range of [0, sNumLocks - 1]
    int index = blockHashIndex(blockId);
    ClientRWLock blockLock = mLockArray[index];
    Lock lock;
    if (blockLockType == BlockLockType.READ) {
      lock = blockLock.readLock();
    } else {
      lock = blockLock.writeLock();
    }
    lock.lock();
    long lockId = LOCK_ID_GEN.getAndIncrement();
    synchronized (mSharedMapsLock) {
      mLockIdToRecordMap.put(lockId, new LockRecord(sessionId, blockId, lock));
      Set<Long> sessionLockIds = mSessionIdToLockIdsMap.get(sessionId);
      if (sessionLockIds == null) {
        mSessionIdToLockIdsMap.put(sessionId, Sets.newHashSet(lockId));
      } else {
        sessionLockIds.add(lockId);
      }
    }
    return lockId;
  }

  /**
   * Releases a lock by its lockId or throws BlockDoesNotExistException.
   *
   * @param lockId the ID of the lock
   * @throws BlockDoesNotExistException if no lock is associated with this lock id
   */
  public void unlockBlock(long lockId) throws BlockDoesNotExistException {
    Lock lock;
    synchronized (mSharedMapsLock) {
      LockRecord record = mLockIdToRecordMap.get(lockId);
      if (record == null) {
        throw new BlockDoesNotExistException(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID,
            lockId);
      }
      long sessionId = record.sessionId();
      lock = record.lock();
      mLockIdToRecordMap.remove(lockId);
      Set<Long> sessionLockIds = mSessionIdToLockIdsMap.get(sessionId);
      sessionLockIds.remove(lockId);
      if (sessionLockIds.isEmpty()) {
        mSessionIdToLockIdsMap.remove(sessionId);
      }
    }
    lock.unlock();
  }

  // TODO(bin): Temporary, remove me later.
  public void unlockBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    synchronized (mSharedMapsLock) {
      Set<Long> sessionLockIds = mSessionIdToLockIdsMap.get(sessionId);
      for (long lockId : sessionLockIds) {
        LockRecord record = mLockIdToRecordMap.get(lockId);
        if (record == null) {
          throw new BlockDoesNotExistException(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID,
              lockId);
        }
        if (blockId == record.blockId()) {
          mLockIdToRecordMap.remove(lockId);
          sessionLockIds.remove(lockId);
          if (sessionLockIds.isEmpty()) {
            mSessionIdToLockIdsMap.remove(sessionId);
          }
          Lock lock = record.lock();
          lock.unlock();
          return;
        }
      }
      throw new BlockDoesNotExistException(
          ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_BLOCK_AND_SESSION, blockId, sessionId);
    }
  }

  /**
   * Validates the lock is hold by the given session for the given block.
   *
   * @param sessionId The ID of the session
   * @param blockId The ID of the block
   * @param lockId The ID of the lock
   * @throws BlockDoesNotExistException when no lock record can be found for lockId
   * @throws InvalidWorkerStateException when sessionId or blockId is not consistent with that in
   *         the lock record for lockId
   */
  public void validateLock(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    synchronized (mSharedMapsLock) {
      LockRecord record = mLockIdToRecordMap.get(lockId);
      if (record == null) {
        throw new BlockDoesNotExistException(ExceptionMessage.LOCK_RECORD_NOT_FOUND_FOR_LOCK_ID,
            lockId);
      }
      if (sessionId != record.sessionId()) {
        throw new InvalidWorkerStateException(ExceptionMessage.LOCK_ID_FOR_DIFFERENT_SESSION,
            lockId, record.sessionId(), sessionId);
      }
      if (blockId != record.blockId()) {
        throw new InvalidWorkerStateException(ExceptionMessage.LOCK_ID_FOR_DIFFERENT_BLOCK, lockId,
            record.blockId(), blockId);
      }
    }
  }

  /**
   * Cleans up the locks currently hold by a specific session
   *
   * @param sessionId the ID of the session to cleanup
   */
  public void cleanupSession(long sessionId) {
    synchronized (mSharedMapsLock) {
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
        Lock lock = record.lock();
        lock.unlock();
        mLockIdToRecordMap.remove(lockId);
      }
      mSessionIdToLockIdsMap.remove(sessionId);
    }
  }

  /**
   * Get a set of currently locked blocks.
   *
   * @return a set of locked blocks
   */
  public Set<Long> getLockedBlocks() {
    synchronized (mSharedMapsLock) {
      Set<Long> set = new HashSet<Long>();
      for (LockRecord lockRecord : mLockIdToRecordMap.values()) {
        set.add(lockRecord.blockId());
      }
      return set;
    }
  }

  /**
   * Inner class to keep record of a lock.
   */
  private static final class LockRecord {
    private final long mSessionId;
    private final long mBlockId;
    private final Lock mLock;

    LockRecord(long sessionId, long blockId, Lock lock) {
      mSessionId = sessionId;
      mBlockId = blockId;
      mLock = lock;
    }

    long sessionId() {
      return mSessionId;
    }

    long blockId() {
      return mBlockId;
    }

    Lock lock() {
      return mLock;
    }
  }
}
