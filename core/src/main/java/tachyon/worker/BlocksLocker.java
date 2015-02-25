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

package tachyon.worker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import tachyon.worker.hierarchy.StorageDir;

/**
 * Handle local block locking.
 */
public class BlocksLocker {
  // All Blocks has been locked.
  private final Map<Long, Set<Integer>> mLockedBlockIds = new HashMap<Long, Set<Integer>>();
  // Mapping from block id to the StorageDir in which the block is locked
  private final Map<Long, StorageDir> mLockedBlockIdToStorageDir =
      new HashMap<Long, StorageDir>();
  // Each user facing block has a unique block lock id. 
  private final AtomicInteger mBlockLockId = new AtomicInteger(0);

  private final int mUserId;
  private final WorkerStorage mWorkerStorage;

  public BlocksLocker(WorkerStorage workerStorage, int userId) {
    mUserId = userId;
    mWorkerStorage = workerStorage;
  }

  /**
   * Lock a block with specified lock id.
   * 
   * @param blockId The id of the block.
   * @param blockLockId The lock id of the block
   * @return the StorageDir in which this block is locked.
   */
  public synchronized StorageDir lock(long blockId, int blockLockId) {
    if (!mLockedBlockIds.containsKey(blockId)) {
      StorageDir storageDir =  mWorkerStorage.lockBlock(blockId, mUserId);
      if (storageDir != null) {
        Set<Integer> lockIdSet = new HashSet<Integer>();
        lockIdSet.add(blockLockId);
        mLockedBlockIds.put(blockId, lockIdSet);
        mLockedBlockIdToStorageDir.put(blockId, storageDir);
        return storageDir;
      }
      return null;
    } else {
      mLockedBlockIds.get(blockId).add(blockLockId);
      return mLockedBlockIdToStorageDir.get(blockId);
    }
  }

  /**
   * Get new lock id
   * 
   * @return the lock id
   */
  public synchronized int getLockId() {
    return mBlockLockId.incrementAndGet();
  }
  /**
   * Get StorageDir in which the block is locked
   * 
   * @param blockId The id of the block
   * @return the StorageDir in which the block is locked
   */
  public synchronized StorageDir locked(long blockId) {
    return mLockedBlockIdToStorageDir.get(blockId);
  }

  /**
   * Unlock a block with a lock id.
   * 
   * @param blockId The id of the block.
   * @param lockId The lock id of the lock.
   * @return true if success, false otherwise
   */
  public synchronized boolean unlock(long blockId, int lockId) {
    Set<Integer> lockers = mLockedBlockIds.get(blockId);
    if (lockers != null) {
      lockers.remove(lockId);
      if (lockers.isEmpty()) {
        mLockedBlockIds.remove(blockId);
        return mLockedBlockIdToStorageDir.remove(blockId).unlockBlock(blockId, mUserId);
      }
      return true;
    }
    return true;
  }
}
