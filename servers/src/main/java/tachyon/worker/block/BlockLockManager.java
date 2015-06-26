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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.worker.ClientRWLock;

/**
 * Handle all block locks.
 * <p>
 * This class is thread-safe.
 */
public class BlockLockManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** The number of locks, larger value leads to finer locking granularity, but more space. */
  private static final int NUM_LOCKS = 100;
  /** The unique id of each lock */
  private static final AtomicLong LOCK_ID_GEN = new AtomicLong(0);

  private final BlockMetadataManager mMetaManager;
  /** A map from a block ID to its lock */
  private final List<ClientRWLock> mLockArray = new ArrayList<ClientRWLock>(NUM_LOCKS);
  /** A map from a user ID to all the locks hold by this user */
  private final Map<Long, Set<Long>> mUserIdToLockIdsMap = new HashMap<Long, Set<Long>>();
  /** A map from a lock ID to the lock record of it */
  private final Map<Long, LockRecord> mLockIdToRecordMap = new HashMap<Long, LockRecord>();
  /** To guard access on mLockIdToRecordMap and mUserIdToLockIdsMap */
  private final Object mSharedMapsLock = new Object();

  public BlockLockManager(BlockMetadataManager metaManager) {
    mMetaManager = Preconditions.checkNotNull(metaManager);
    for (int i = 0; i < NUM_LOCKS; i ++) {
      mLockArray.add(new ClientRWLock());
    }
  }

  /**
   * Lock a block if it exists, throw IOException otherwise.
   *
   * @param userId the ID of user
   * @param blockId the ID of block
   * @param blockLockType READ or WRITE
   * @return lock id if the block exists
   * @throws IOException
   */
  public long lockBlock(long userId, long blockId, BlockLockType blockLockType) throws IOException {
    // TODO: generate real hashValue on blockID.
    int hashValue = (int) (blockId % (long) NUM_LOCKS);
    ClientRWLock blockLock = mLockArray.get(hashValue);
    Lock lock;
    if (blockLockType == BlockLockType.READ) {
      lock = blockLock.readLock();
    } else { // blockLockType == BlockLockType.WRITE
      lock = blockLock.writeLock();
    }
    lock.lock();
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

  // TODO: debug only, remove me later.
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
  public void validateLockId(long userId, long blockId, long lockId) throws IOException {
    synchronized (mSharedMapsLock) {
      LockRecord record = mLockIdToRecordMap.get(lockId);
      if (null == record) {
        throw new IOException("Failed to validateLockId: lockId " + lockId + " has no lock record");
      }
      if (userId != record.userId()) {
        throw new IOException("Failed to validateLockId: lockId " + lockId + " is owned by userId "
            + record.userId() + ", not " + userId);
      }
      if (blockId != record.blockId()) {
        throw new IOException("Failed to validateLockId: lockId " + lockId + " is for blockId "
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
