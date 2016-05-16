/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
 * Manages the locks for a group of {@link Inode}.
 */
@ThreadSafe
public final class InodeLockGroup implements AutoCloseable {
  private final List<Inode<?>> mReadLockedInodes;
  private final List<Inode<?>> mWriteLockedInodes;
  private final List<Inode<?>> mInodes;

  InodeLockGroup() {
    mReadLockedInodes = new ArrayList<>();
    mWriteLockedInodes = new ArrayList<>();
    mInodes = new ArrayList<>();
  }

  /**
   * Locks the given inode in read mode, and adds it to this lock group.
   *
   * @param inode the inode to lock
   */
  public synchronized void lockRead(Inode<?> inode) {
    inode.lockRead();
    mReadLockedInodes.add(inode);
    mInodes.add(inode);
  }

  /**
   * Unlocks the last inode that was locked.
   */
  public synchronized void unlockPrevious() {
    if (mInodes.isEmpty()) {
      return;
    }
    Inode<?> inode = mInodes.remove(mInodes.size() - 1);
    if (mReadLockedInodes.size() > 0 && mReadLockedInodes.get(mReadLockedInodes.size() - 1)
        .equals(inode)) {
      inode.unlockRead();
      mReadLockedInodes.remove(mReadLockedInodes.size() - 1);
      return;
    }
    if (mWriteLockedInodes.size() > 0 && mWriteLockedInodes.get(mWriteLockedInodes.size() - 1)
        .equals(inode)) {
      inode.unlockWrite();
      mWriteLockedInodes.remove(mWriteLockedInodes.size() - 1);
      return;
    }
  }

  /**
   * Locks the given inode in write mode, and adds it to this lock group.
   *
   * @param inode the inode to lock
   */
  public synchronized void lockWrite(Inode<?> inode) {
    inode.lockWrite();
    mWriteLockedInodes.add(inode);
    mInodes.add(inode);
  }

  /**
   * @return the list of inodes locked in this lock group, in order of when the inodes were locked
   */
  public synchronized List<Inode<?>> getInodes() {
    return mInodes;
  }

  @Override
  public synchronized void close() {
    for (Inode<?> inode : mReadLockedInodes) {
      inode.unlockRead();
    }
    for (Inode<?> inode : mWriteLockedInodes) {
      inode.unlockWrite();
    }
    mReadLockedInodes.clear();
    mWriteLockedInodes.clear();
    mInodes.clear();
  }
}
