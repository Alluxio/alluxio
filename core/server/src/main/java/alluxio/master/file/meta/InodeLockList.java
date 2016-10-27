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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Manages the locks for a list of {@link Inode}.
 */
@ThreadSafe
public final class InodeLockList implements AutoCloseable {
  private final List<Inode<?>> mInodes;
  private final List<InodeTree.LockMode> mLockModes;

  InodeLockList() {
    mInodes = new ArrayList<>();
    mLockModes = new ArrayList<>();
  }

  /**
   * Locks the given inode in read mode, and adds it to this lock list.
   *
   * @param inode the inode to lock
   */
  public synchronized void lockRead(Inode<?> inode) {
    inode.lockRead();
    mInodes.add(inode);
    mLockModes.add(InodeTree.LockMode.READ);
  }

  /**
   * Unlocks the last inode that was locked.
   */
  public synchronized void unlockLast() {
    if (mInodes.isEmpty()) {
      return;
    }
    Inode<?> inode = mInodes.remove(mInodes.size() - 1);
    InodeTree.LockMode lockMode = mLockModes.remove(mLockModes.size() - 1);
    if (lockMode == InodeTree.LockMode.READ) {
      inode.unlockRead();
    } else {
      inode.unlockWrite();
    }
  }

  /**
   * Locks the given inode in write mode, and adds it to this lock list.
   *
   * @param inode the inode to lock
   */
  public synchronized void lockWrite(Inode<?> inode) {
    inode.lockWrite();
    mInodes.add(inode);
    mLockModes.add(InodeTree.LockMode.WRITE);
  }

  /**
   * @return the list of inodes locked in this lock list, in order of when the inodes were locked
   */
  public synchronized List<Inode<?>> getInodes() {
    return mInodes;
  }

  @Override
  public synchronized void close() {
    for (int i = mInodes.size() - 1; i >= 0; i--) {
      Inode<?> inode = mInodes.get(i);
      InodeTree.LockMode lockMode = mLockModes.get(i);
      if (lockMode == InodeTree.LockMode.READ) {
        inode.unlockRead();
      } else {
        inode.unlockWrite();
      }
    }
    mInodes.clear();
    mLockModes.clear();
  }
}
