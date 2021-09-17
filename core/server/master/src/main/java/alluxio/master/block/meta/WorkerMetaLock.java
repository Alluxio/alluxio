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

package alluxio.master.block.meta;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * This class manages the logic of locking multiple sections in {@link MasterWorkerInfo} properly.
 *
 * As mentioned in javadoc of {@link MasterWorkerInfo}, there are multiple groups of metadata
 * that requires external locking. This class is used to provide a clean interface to manage
 * the locking logic and lock life cycles.
 *
 * When multiple {@link WorkerMetaLockSection} are specified, internally the locks will be
 * locked in order, and unlocked in the opposite order, so that deadlock is prevented.
 *
 * This class implements {@link Lock} so it can be managed by
 * {@link alluxio.resource.LockResource}. Callers do not need to lock and unlock explicitly.
 */
// TODO(jiacheng): Make LockResource support multiple locks so we don't need this wrapper anymore
public class WorkerMetaLock implements Lock {
  // The order for acquiring locks
  private static final List<WorkerMetaLockSection> NATURAL_ORDER =
      Arrays.asList(WorkerMetaLockSection.values());
  // The order for releasing locks
  private static final List<WorkerMetaLockSection> REVERSE_ORDER = Lists.reverse(NATURAL_ORDER);

  private final EnumSet<WorkerMetaLockSection> mLockTypes;
  private final boolean mIsShared;
  private final MasterWorkerInfo mWorker;

  /**
   * Constructor.
   *
   * @param lockTypes each {@link WorkerMetaLockSection} corresponds to one section of metadata
   * @param isShared if false, the lock is exclusive
   * @param worker the {@link MasterWorkerInfo} to lock
   */
  WorkerMetaLock(EnumSet<WorkerMetaLockSection> lockTypes, boolean isShared,
       MasterWorkerInfo worker) {
    mLockTypes = lockTypes;
    mIsShared = isShared;
    mWorker = worker;
  }

  @Override
  public void lock() {
    for (WorkerMetaLockSection t : NATURAL_ORDER) {
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
    for (WorkerMetaLockSection t : REVERSE_ORDER) {
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
    throw new UnsupportedOperationException("lockInterruptibly is not supported!");
  }

  @Override
  public boolean tryLock() {
    throw new UnsupportedOperationException("tryLock is not supported!");
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    throw new UnsupportedOperationException("tryLock is not supported!");
  }

  @Override
  public Condition newCondition() {
    throw new UnsupportedOperationException("newCondition is not supported!");
  }
}
