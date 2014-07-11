/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;
import tachyon.util.CommonUtils;

/**
 * Handle local block locking.
 */
public class BlocksLocker {
  // All Blocks has been locked.
  private Map<Long, Set<Integer>> mLockedBlockIds = new HashMap<Long, Set<Integer>>();
  // Each user facing block has a unique block lock id.
  private AtomicInteger mBlockLockId = new AtomicInteger(0);

  private int mUserId;
  private WorkerStorage mWorkerStorage;

  public BlocksLocker(WorkerStorage workerStorage, int userId) {
    mUserId = userId;
    mWorkerStorage = workerStorage;
  }

  /**
   * Lock a block.
   * 
   * @param blockId
   *          The id of the block.
   * @return The lockId of this lock.
   */
  public synchronized int lock(long blockId) {
    int locker = mBlockLockId.incrementAndGet();
    if (!mLockedBlockIds.containsKey(blockId)) {
      try {
        mWorkerStorage.lockBlock(blockId, mUserId);
      } catch (TException e) {
        CommonUtils.runtimeException(e);
      }
      mLockedBlockIds.put(blockId, new HashSet<Integer>());
    }
    mLockedBlockIds.get(blockId).add(locker);
    return locker;
  }

  /**
   * Check if the block is locked in the local memory
   * 
   * @param blockId
   *          The id of the block
   * @return true if the block is locked, false otherwise
   */
  public synchronized boolean locked(long blockId) {
    return mLockedBlockIds.containsKey(blockId);
  }

  /**
   * Unlock a block with a lock id.
   * 
   * @param blockId
   *          The id of the block.
   * @param lockId
   *          The lock id of the lock.
   */
  public synchronized void unlock(long blockId, int lockId) {
    Set<Integer> lockers = mLockedBlockIds.get(blockId);
    if (lockers != null) {
      lockers.remove(lockId);
      if (lockers.isEmpty()) {
        mLockedBlockIds.remove(blockId);
        try {
          mWorkerStorage.unlockBlock(blockId, mUserId);
        } catch (TException e) {
          CommonUtils.runtimeException(e);
        }
      }
    }
  }
}
