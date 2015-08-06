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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import tachyon.Constants;

/**
 * Handle all block locks.
 * <p>
 * This class is thread-safe.
 */
public class BlockLockManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** The number of locks, larger value leads to finer locking granularity, but more space. */
  // TODO: Make this configurable
  private static final int NUM_LOCKS = 1000;
  /** Time to wait to acquire a lock */
  private static final int LOCK_ACQUIRE_TIMEOUT_MS = 5000;
  /** The unique id of each lock */
  private static final AtomicLong LOCK_ID_GEN = new AtomicLong(0);
  /** A hashing function to map blockId to one of the locks */
  private static final HashFunction HASH_FUNC = Hashing.murmur3_32();

  /** The object that serves all metadata requests for the block store */
  private final BlockMetadataManager mMetaManager;
  /** A map from a block ID to its lock */
  private final ClientRWLock[] mLockArray = new ClientRWLock[NUM_LOCKS];
  /** A map from a user ID to all the locks hold by this user */
  private final Map<Long, Set<Long>> mUserIdToLockIdsMap = new HashMap<Long, Set<Long>>();
  /** A map from a lock ID to the lock record of it */
  private final Map<Long, LockRecord> mLockIdToRecordMap = new HashMap<Long, LockRecord>();
  /** To guard access on mLockIdToRecordMap and mUserIdToLockIdsMap */
  private final Object mSharedMapsLock = new Object();

  public BlockLockManager(BlockMetadataManager metaManager) {
    mMetaManager = Preconditions.checkNotNull(metaManager);
    for (int i = 0; i < NUM_LOCKS; i ++) {
      mLockArray[i] = new ClientRWLock();
    }
  }

  /**
   * Get index of the lock that will be used to lock the block
   *
   * @param blockId the id of the block
   * @return hash index of the lock
   */
  public static int blockHashIndex(long blockId) {
    return Math.abs(HASH_FUNC.hashLong(blockId).asInt()) % NUM_LOCKS;
  }

  /**
   * Attempts to lock a block if it exists.
   *
   * @param userId the ID of user
   * @param blockId the ID of block
   * @param blockLockType READ or WRITE
   * @return lock ID
   * @throws IOException if the block does not exist or if the lock attempt exceeded the timeout
   */
  public long lockBlock(long userId, long blockId, BlockLockType blockLockType) throws IOException {
    // hashing blockId into the range of [0, NUM_LOCKS-1]
    int index = blockHashIndex(blockId);
    ClientRWLock blockLock = mLockArray[index];
    Lock lock;
    if (blockLockType == BlockLockType.READ) {
      lock = blockLock.readLock();
    } else { // blockLockType == BlockLockType.WRITE
      lock = blockLock.writeLock();
    }

    // The block lock may be busy, wait up to five seconds to obtain it.
    boolean success;
    try {
      success = lock.tryLock(LOCK_ACQUIRE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      // The UserLock implementation does not throw this exception, something is wrong if it happens
      LOG.error("Interrupted exception in tryLock, this should not occur!");
      throw new IOException(ie.getMessage(), ie.getCause());
    }
    if (!success) {
      String errMsg = "5s timeout when attempting lockBlock: " + blockId + " for user: " + userId;
      LOG.error(errMsg);
      throw new IOException(errMsg);
    }
    if (!mMetaManager.hasBlockMeta(blockId)) {
      lock.unlock();
      throw new IOException("Failed to lockBlock: no blockId " + blockId + " found");
    }
    long lockId = LOCK_ID_GEN.getAndIncrement();
    synchronized (mSharedMapsLock) {
      mLockIdToRecordMap.put(lockId, new LockRecord(userId, blockId, lock));
      Set<Long> userLockIds = mUserIdToLockIdsMap.get(userId);
      if (null == userLockIds) {
        mUserIdToLockIdsMap.put(userId, Sets.newHashSet(lockId));
      } else {
        userLockIds.add(lockId);
      }
    }
    return lockId;
  }

  /**
   * Releases a lock by its lockId or throw IOException.
   *
   * @param lockId the ID of the lock
   * @throws IOException if no lock is associated with this lock
   */
  public void unlockBlock(long lockId) throws IOException {
    Lock lock;
    synchronized (mSharedMapsLock) {
      LockRecord record = mLockIdToRecordMap.get(lockId);
      if (null == record) {
        throw new IOException("Failed to unlockBlock: lockId " + lockId + " has no lock record");
      }
      long userId = record.userId();
      lock = record.lock();
      mLockIdToRecordMap.remove(lockId);
      Set<Long> userLockIds = mUserIdToLockIdsMap.get(userId);
      userLockIds.remove(lockId);
      if (userLockIds.isEmpty()) {
        mUserIdToLockIdsMap.remove(userId);
      }
    }
    lock.unlock();
  }

  // TODO: temporary, remove me later.
  public void unlockBlock(long userId, long blockId) throws IOException {
    synchronized (mSharedMapsLock) {
      Set<Long> userLockIds = mUserIdToLockIdsMap.get(userId);
      for (long lockId : userLockIds) {
        LockRecord record = mLockIdToRecordMap.get(lockId);
        if (null == record) {
          throw new IOException("Failed to unlockBlock: lockId " + lockId + " has no lock record");
        }
        if (blockId == record.blockId()) {
          mLockIdToRecordMap.remove(lockId);
          userLockIds.remove(lockId);
          if (userLockIds.isEmpty()) {
            mUserIdToLockIdsMap.remove(userId);
          }
          Lock lock = record.lock();
          lock.unlock();
          return;
        }
      }
      throw new IOException("Failed to unlock blockId " + blockId + " for userId " + userId);
    }
  }

  /**
   * Validates the lock is hold by the given user for the given block.
   *
   * @param userId The ID of the user
   * @param blockId The ID of the block
   * @param lockId The ID of the lock
   */
  public void validateLock(long userId, long blockId, long lockId) throws IOException {
    synchronized (mSharedMapsLock) {
      LockRecord record = mLockIdToRecordMap.get(lockId);
      if (null == record) {
        throw new IOException("Failed to validateLock: lockId " + lockId + " has no lock record");
      }
      if (userId != record.userId()) {
        throw new IOException("Failed to validateLock: lockId " + lockId + " is owned by userId "
            + record.userId() + ", not " + userId);
      }
      if (blockId != record.blockId()) {
        throw new IOException("Failed to validateLock: lockId " + lockId + " is for blockId "
            + record.blockId() + ", not " + blockId);
      }
    }
  }

  /**
   * Cleans up the locks currently hold by a specific user
   *
   * @param userId the ID of the user to cleanup
   */
  public void cleanupUser(long userId) {
    synchronized (mSharedMapsLock) {
      Set<Long> userLockIds = mUserIdToLockIdsMap.get(userId);
      if (null == userLockIds) {
        return;
      }
      for (long lockId : userLockIds) {
        LockRecord record = mLockIdToRecordMap.get(lockId);
        if (null == record) {
          LOG.error("Failed to cleanup userId {}: no lock record for lockId {}", userId, lockId);
          continue;
        }
        Lock lock = record.lock();
        lock.unlock();
        mLockIdToRecordMap.remove(lockId);
      }
      mUserIdToLockIdsMap.remove(userId);
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
  private class LockRecord {
    private final long mUserId;
    private final long mBlockId;
    private final Lock mLock;

    LockRecord(long userId, long blockId, Lock lock) {
      mUserId = userId;
      mBlockId = blockId;
      mLock = lock;
    }

    long userId() {
      return mUserId;
    }

    long blockId() {
      return mBlockId;
    }

    Lock lock() {
      return mLock;
    }
  }
}
