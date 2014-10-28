package tachyon.worker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handle local block locking.
 */
public class BlocksLocker {
  // All Blocks has been locked.
  private final Map<Long, Set<Integer>> mLockedBlockIds = new HashMap<Long, Set<Integer>>();
  // Each user facing block has a unique block lock id.
  private final AtomicInteger mBlockLockId = new AtomicInteger(0);

  private final int mUserId;
  private final WorkerStorage mWorkerStorage;

  public BlocksLocker(WorkerStorage workerStorage, int userId) {
    mUserId = userId;
    mWorkerStorage = workerStorage;
  }

  /**
   * Lock a block.
   * 
   * @param blockId The id of the block.
   * @return The lockId of this lock.
   */
  public synchronized int lock(long blockId) {
    int locker = mBlockLockId.incrementAndGet();
    if (!mLockedBlockIds.containsKey(blockId)) {
      mWorkerStorage.lockBlock(blockId, mUserId);
      mLockedBlockIds.put(blockId, new HashSet<Integer>());
    }
    mLockedBlockIds.get(blockId).add(locker);
    return locker;
  }

  /**
   * Check if the block is locked in the local memory
   * 
   * @param blockId The id of the block
   * @return true if the block is locked, false otherwise
   */
  public synchronized boolean locked(long blockId) {
    return mLockedBlockIds.containsKey(blockId);
  }

  /**
   * Unlock a block with a lock id.
   * 
   * @param blockId The id of the block.
   * @param lockId The lock id of the lock.
   */
  public synchronized void unlock(long blockId, int lockId) {
    Set<Integer> lockers = mLockedBlockIds.get(blockId);
    if (lockers != null) {
      lockers.remove(lockId);
      if (lockers.isEmpty()) {
        mLockedBlockIds.remove(blockId);
        mWorkerStorage.unlockBlock(blockId, mUserId);
      }
    }
  }
}
