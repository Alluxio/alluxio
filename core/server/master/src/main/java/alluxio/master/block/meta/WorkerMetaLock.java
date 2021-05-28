package alluxio.master.block.meta;

import com.google.common.collect.ImmutableList;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class WorkerMetaLock implements Lock {
  private final EnumSet<WorkerMetaLockType> mLockTypes;
  private final boolean mIsShared;
  private final MasterWorkerInfo mWorker;
  WorkerMetaLock(EnumSet<WorkerMetaLockType> lockTypes, boolean isShared, MasterWorkerInfo worker) {
    mLockTypes = lockTypes;
    mIsShared = isShared;
    mWorker = worker;
  }

  @Override
  public void lock() {
    for (WorkerMetaLockType t : ImmutableList.of(WorkerMetaLockType.STATUS_LOCK,
            WorkerMetaLockType.USAGE_LOCK, WorkerMetaLockType.BLOCKS_LOCK)) {
      if (mLockTypes.contains(t)) {
        if (mIsShared) {
          mWorker.getLock(t).readLock().lock();
        } else {
          mWorker.getLock(t).writeLock().lock();
        }
      }
    }
  }

  @Override
  public void unlock() {
    for (WorkerMetaLockType t : ImmutableList.of(WorkerMetaLockType.BLOCKS_LOCK,
            WorkerMetaLockType.USAGE_LOCK, WorkerMetaLockType.STATUS_LOCK)) {
      if (mLockTypes.contains(t)) {
        if (mIsShared) {
          mWorker.getLock(t).readLock().unlock();
        } else {
          mWorker.getLock(t).writeLock().unlock();
        }
      }
    }
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean tryLock() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Condition newCondition() {
    throw new UnsupportedOperationException();
  }
}