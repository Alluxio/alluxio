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

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Manages the locks for a list of {@link Inode}s, based off an existing lock list. This does not
 * modify the base lock list, and when this lock list is closed, the base lock list is not closed.
 * The purpose of this class is to allow lock lists to be temporarily extended and then restored.
 *
 * The base lock list can end with either an inode or edge, and can be read locked or write locked.
 *
 * Modification is only supported for the non-base part of the lock list.
 */
@NotThreadSafe
public class CompositeInodeLockList extends InodeLockList {
  /** The base lock list for this composite list. */
  private final InodeLockList mBaseLockList;

  /**
   * Constructs a new lock list, using an existing lock list as the base list.
   *
   * @param baseLockList the base {@link InodeLockList} to use
   */
  public CompositeInodeLockList(InodeLockList baseLockList) {
    super(baseLockList.mInodeLockManager);
    mBaseLockList = baseLockList;
    mLockMode = baseLockList.mLockMode;
  }

  @Override
  public synchronized List<InodeView> getLockedInodes() {
    // Combine the base list of inodes first.
    List<InodeView> ret = new ArrayList<>(mBaseLockList.numLockedInodes() + mLockedInodes.size());
    ret.addAll(mBaseLockList.getLockedInodes());
    ret.addAll(mLockedInodes);
    return ret;
  }

  @Override
  public synchronized InodeView get(int index) {
    if (index < mBaseLockList.numLockedInodes()) {
      return mBaseLockList.get(index);
    }
    return mLockedInodes.get(index - mBaseLockList.numLockedInodes());
  }

  @Override
  public synchronized int numLockedInodes() {
    return mBaseLockList.numLockedInodes() + mLockedInodes.size();
  }

  /**
   * @return true if the locklist is empty
   */
  @Override
  public synchronized boolean isEmpty() {
    return mBaseLockList.isEmpty() && mLockedInodes.isEmpty();
  }

  @Override
  protected Entry lastEntry() {
    if (mEntries.isEmpty()) {
      return mBaseLockList.lastEntry();
    }
    return super.lastEntry();
  }
}
