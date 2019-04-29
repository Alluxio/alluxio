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
  private final InodeLockList mSubLockList;
  private final int mBaseListSize;

  /**
   * Constructs a new lock list, using an existing lock list as the base list.
   *
   * @param baseLockList the base {@link InodeLockList} to use
   */
  public CompositeInodeLockList(InodeLockList baseLockList) {
    mBaseLockList = baseLockList;
    mBaseListSize = baseLockList.numInodes();
    mSubLockList = new SimpleInodeLockList(baseLockList.getInodeLockManager());
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
    if (mSubLockList.isEmpty()) {
      return;
    }
    mSubLockList.unlockLastInode();
  }

  @Override
  public void unlockLastEdge() {
    if (mSubLockList.isEmpty()) {
      return;
    }
    mSubLockList.unlockLastEdge();
  }

  @Override
  public void downgradeLastInode() {
    if (!canDowngradeLast()) {
      return;
    }
    mSubLockList.downgradeLastInode();
  }

  @Override
  public void downgradeLastEdge() {
    if (!canDowngradeLast()) {
      return;
    }
    mSubLockList.downgradeLastEdge();
  }

  @Override
  public void pushWriteLockedEdge(Inode inode, String childName) {
    if (!canDowngradeLast()) {
      // Can't downgrade, just acquire new locks instead.
      mSubLockList.lockInode(inode, LockMode.WRITE);
      mSubLockList.lockEdge(inode, childName, LockMode.WRITE);
      return;
    }
    mSubLockList.pushWriteLockedEdge(inode, childName);
  }

  @Override
  public void downgradeEdgeToInode(Inode inode, LockMode mode) {
    mode = nextLockMode(mode);
    if (mSubLockList.isEmpty()) {
      // Can't downgrade, just acquire new locks instead.
      mSubLockList.lockInode(inode, LockMode.WRITE);
      return;
    }
    mSubLockList.downgradeEdgeToInode(inode, mode);
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
