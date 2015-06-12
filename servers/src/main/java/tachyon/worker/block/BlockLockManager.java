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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.worker.ClientRWLock;

/**
 * Handle all block locks.
 * <p>
 * This class is thread-safe as all public methods are syncrhonized.
 */
public class BlockLockManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int NUM_LOCKS = 100;
  /** The unique id of each lock **/
  private static final AtomicLong LOCK_ID_GEN = new AtomicLong(0);

  /** A map from a block ID to its lock */
  private final List<ClientRWLock> mLockArray = new ArrayList<ClientRWLock>(NUM_LOCKS);
  /** A map from a user ID to all the locks hold by this user */
  private final Map<Long, Set<Long>> mUserIdToLockIdsMap = new HashMap<Long, Set<Long>>();
  /** A map from a lock ID to the user ID holding this lock */
  private final Map<Long, LockRecord> mLockIdToRecordMap = new HashMap<Long, LockRecord>();

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

  public BlockLockManager() {}

  public synchronized Optional<Long> lockBlock(long userId, long blockId,
      BlockLockType blockLockType) {
    int hashValue = (int) blockId % NUM_LOCKS;
    ClientRWLock blockLock = mLockArray.get(hashValue);
    Lock lock = null;
    if (blockLockType == BlockLockType.READ) {
      lock = blockLock.readLock();
    } else if (blockLockType == BlockLockType.WRITE) {
      lock = blockLock.writeLock();
    }
    lock.lock();
    long lockId = LOCK_ID_GEN.getAndIncrement();
    mLockIdToRecordMap.put(lockId, new LockRecord(userId, blockId, lock));
    Set<Long> userLockIds = mUserIdToLockIdsMap.get(userId);
    if (null == userLockIds) {
      mUserIdToLockIdsMap.put(userId, Sets.newHashSet(lockId));
    } else {
      userLockIds.add(lockId);
    }
    return Optional.of(lockId);
  }

  public boolean unlockBlock(long lockId) {
    LockRecord record = mLockIdToRecordMap.get(lockId);
    if (null == record) {
      return false;
    }
    long userId = record.userId();
    Lock lock = record.lock();

    mLockIdToRecordMap.remove(lockId);
    Set<Long> userLockIds = mUserIdToLockIdsMap.get(userId);
    userLockIds.remove(lockId);
    if (userLockIds.isEmpty()) {
      mUserIdToLockIdsMap.remove(userId);
    }
    lock.unlock();
    return true;
  }

  /**
   * Validates the lock is hold by the given user for the given block.
   *
   * @param userId The ID of the user
   * @param blockId The ID of the block
   * @param lockId The ID of the lock
   * @return true if validation succeeds, false otherwise
   */
  public boolean validateLockId(long userId, long blockId, long lockId) {
    LockRecord record = mLockIdToRecordMap.get(lockId);
    if (null == record) {
      return false;
    }
    return userId == record.userId() && blockId == record.blockId();
  }

  /**
   * Cleans up the locks currently hold by a specific user
   *
   * @param userId the ID of the user to cleanup
   */
  public void cleanupUser(long userId) {
    Set<Long> userLockIds = mUserIdToLockIdsMap.get(userId);
    if (null == userLockIds) {
      return;
    }
    for (long lockId : userLockIds) {
      LockRecord record = mLockIdToRecordMap.get(lockId);
      if (null == record) {
        return;
      }
      Lock lock = record.lock();
      lock.unlock();
      mLockIdToRecordMap.remove(lockId);
    }
    mUserIdToLockIdsMap.remove(userId);
  }
}
