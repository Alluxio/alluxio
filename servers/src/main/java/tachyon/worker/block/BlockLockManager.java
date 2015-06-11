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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;

import tachyon.Constants;
import tachyon.Pair;

/**
 * Handle all block locks.
 * <p>
 * This class is thread-safe as all public methods are syncrhonized.
 */
public class BlockLockManager {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /** The unique id of each lock **/
  private static final AtomicLong LOCK_ID_GEN = new AtomicLong(0);

  /** A map from a block ID to its lock */
  private final Map<Long, BlockLock> mBlockIdToLockMap = new HashMap<Long, BlockLock>();
  /** A map from a user ID to all the locks hold by this user */
  private final Map<Long, Set<Long>> mUserIdToLockIdsMap = new HashMap<Long, Set<Long>>();
  /** A map from a lock ID to the user ID holding this lock */
  private final Map<Long, Pair<Long, Lock>> mLockIdToUserIdAndLockMap =
      new HashMap<Long, Pair<Long, Lock>>();

  public BlockLockManager() {}

  public synchronized Optional<Long> lockBlock(long userId, long blockId,
      BlockLock.BlockLockType blockLockType) {
    if (!mBlockIdToLockMap.containsKey(blockId)) {
      LOG.error("Cannot get lock for block {}: not exists", blockId);
      return Optional.absent();
    }
    BlockLock blockLock = mBlockIdToLockMap.get(blockId);
    Lock lock = null;
    if (blockLockType == BlockLock.BlockLockType.READ) {
      lock = blockLock.readLock();
    } else if (blockLockType == BlockLock.BlockLockType.WRITE) {
      lock = blockLock.writeLock();
    }
    lock.lock();
    long lockId = createLockId(userId, lock);
    return Optional.of(lockId);
  }

  public synchronized boolean unlockBlock(long lockId) {
    // TODO: implement me
    // do unlock

    cleanupLockId(lockId);
    return true;
  }

  private synchronized long createLockId(long userId, Lock lock) {
    // TODO: implement me
    long lockId = LOCK_ID_GEN.getAndIncrement();
    // mUserIdToAcquiredLockIdsMap.put(userId, lockID);
    return lockId;
  }

  private synchronized boolean cleanupLockId(long lockId) {
    // TODO: implement me
    // mUserIdToAcquiredLockIdsMap.put(userId, lockID);
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
  public synchronized boolean validateLockId(long userId, long blockId, long lockId) {
    // TODO: implement me
    return true;
  }

  /**
   * Get the lock for the given block id. If there is no such a lock yet, create one.
   *
   * @param blockId The id of the block
   * @return true if success, false otherwise
   */
  public synchronized boolean addBlockLock(long blockId) {
    if (mBlockIdToLockMap.containsKey(blockId)) {
      LOG.error("Cannot add lock for block {}: already exists", blockId);
      return false;
    }
    mBlockIdToLockMap.put(blockId, new BlockLock(blockId));
    return true;
  }

  /**
   * Remove a lock for the given block id.
   *
   * @param blockId The id of the block
   * @return true if success, false otherwise
   */
  public synchronized boolean removeBlockLock(long blockId) {
    if (!mBlockIdToLockMap.containsKey(blockId)) {
      LOG.error("Cannot remove lock for block {}: not exists", blockId);
      return false;
    }
    mBlockIdToLockMap.remove(blockId);
    return true;
  }
}
