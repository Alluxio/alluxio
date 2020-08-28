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

package alluxio.master.file.meta;

import alluxio.concurrent.LockMode;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.concurrent.locks.Lock;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Manages the locks for a list of inodes, based off an existing lock list. This does not
 * modify the base lock list, and when this lock list is closed, the base lock list is not closed.
 * The purpose of this class is to allow lock lists to be temporarily extended and then restored.
 *
 * The base lock list can end with either an inode or edge, and can be read locked or write locked.
 *
 * Modification is only supported for the non-base part of the lock list.
 */
@NotThreadSafe
public class CompositeInodeLockList implements InodeLockList {
  /** The base lock list for this composite list. */
  private final InodeLockList mBaseLockList;
  /** An extension lock list for taking additional locks on top of the locks in mBaseLockList. */
  private final InodeLockList mSubLockList;

  /** The size of the base lock list, saved for fast lookup. */
  private final int mBaseListSize;

  /**
   * Constructs a new lock list, using an existing lock list as the base list.
   *
   * @param baseLockList the base {@link InodeLockList} to use
   * @param useTryLock whether or not use {@link Lock#tryLock()} or {@link Lock#lock()}
   */
  public CompositeInodeLockList(InodeLockList baseLockList, boolean useTryLock) {
    mBaseLockList = baseLockList;
    mBaseListSize = baseLockList.numInodes();
    mSubLockList = new SimpleInodeLockList(baseLockList.getInodeLockManager(), useTryLock);
  }

  @Override
  public void lockRootEdge(LockMode mode) {
    throw new UnsupportedOperationException(
        "lockRootEdge is not supported for composite lock lists");
  }

  @Override
  public void lockInode(Inode inode, LockMode mode) {
    mode = nextLockMode(mode);
    mSubLockList.lockInode(inode, mode);
  }

  @Override
  public void lockEdge(Inode inode, String childName, LockMode mode) {
    mode = nextLockMode(mode);
    if (mSubLockList.isEmpty()) {
      Preconditions.checkState(inode.getId() == mBaseLockList.get(mBaseListSize - 1).getId());
    }
    mSubLockList.lockEdge(inode, childName, mode);
  }

  @Override
  public void unlockLastInode() {
    if (!mSubLockList.isEmpty()) {
      mSubLockList.unlockLastInode();
    }
  }

  @Override
  public void unlockLastEdge() {
    if (!mSubLockList.isEmpty()) {
      mSubLockList.unlockLastEdge();
    }
  }

  @Override
  public void downgradeToReadLocks() {
    if (canDowngradeLast()) {
      mSubLockList.downgradeToReadLocks();
    }
  }

  @Override
  public void downgradeLastEdge() {
    if (canDowngradeLast()) {
      mSubLockList.downgradeLastEdge();
    }
  }

  @Override
  public void pushWriteLockedEdge(Inode inode, String childName) {
    if (canDowngradeLast()) {
      mSubLockList.pushWriteLockedEdge(inode, childName);
      return;
    }
    // Can't downgrade, just acquire new locks instead.
    mSubLockList.lockInode(inode, LockMode.WRITE);
    mSubLockList.lockEdge(inode, childName, LockMode.WRITE);
  }

  private boolean canDowngradeLast() {
    return !mSubLockList.isEmpty() && mBaseLockList.getLockMode() == LockMode.READ;
  }

  private LockMode nextLockMode(LockMode mode) {
    if (mBaseLockList.getLockMode() == LockMode.WRITE) {
      return LockMode.WRITE;
    }
    return mode;
  }

  @Override
  public LockMode getLockMode() {
    return mSubLockList.isEmpty() ? mBaseLockList.getLockMode() : mSubLockList.getLockMode();
  }

  @Override
  public List<Inode> getLockedInodes() {
    List<Inode> inodes = mBaseLockList.getLockedInodes();
    inodes.addAll(mSubLockList.getLockedInodes());
    return inodes;
  }

  @Override
  public Inode get(int index) {
    return index < mBaseListSize ? mBaseLockList.get(index)
        : mSubLockList.get(index - mBaseListSize);
  }

  @Override
  public int numInodes() {
    return mBaseListSize + mSubLockList.numInodes();
  }

  @Override
  public boolean endsInInode() {
    return mSubLockList.isEmpty() ? mBaseLockList.endsInInode() : mSubLockList.endsInInode();
  }

  @Override
  public boolean isEmpty() {
    return mBaseLockList.isEmpty() && mSubLockList.isEmpty();
  }

  @Override
  public InodeLockManager getInodeLockManager() {
    return mBaseLockList.getInodeLockManager();
  }

  @Override
  public void close() {
    mSubLockList.close();
  }
}
