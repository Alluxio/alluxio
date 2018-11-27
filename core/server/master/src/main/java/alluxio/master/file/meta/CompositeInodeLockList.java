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
 * Manages the locks for a list of {@link Inode}s, based off an existing lock list. This does not
 * modify the base lock list, and when this lock list is closed, the base lock list is not closed.
 */
@ThreadSafe
public class CompositeInodeLockList extends InodeLockList {
  /** The base lock list for this composite list. */
  private final InodeLockList mBaseLockList;

  /**
   * Constructs a new lock list, using an existing lock list as the base list.
   *
   * @param baseLockList the base {@link InodeLockList} to use
   */
  public CompositeInodeLockList(InodeLockList baseLockList) {
    super(baseLockList.mInodeLocker, baseLockList.mInodeExistsFn);
    mBaseLockList = baseLockList;
  }

  /**
   * Constructs a new lock list, using an existing lock list as the base list.
   *
   * @param baseLockList the base {@link InodeLockList} to use
   * @param descendantLockList the locklist extension
   */
  public CompositeInodeLockList(InodeLockList baseLockList, InodeLockList descendantLockList) {
    super(baseLockList.mInodeLocker, baseLockList.mInodeExistsFn);
    mBaseLockList = baseLockList;
    mInodes = descendantLockList.mInodes;
    mLocks = descendantLockList.mLocks;
    mLockModes = descendantLockList.mLockModes;
  }

  @Override
  public synchronized List<InodeView> getInodes() {
    // Combine the base list of inodes first.
    List<InodeView> ret = new ArrayList<>(mBaseLockList.size() + mInodes.size());
    ret.addAll(mBaseLockList.getInodes());
    ret.addAll(mInodes);
    return ret;
  }

  @Override
  public synchronized InodeView get(int index) {
    if (index < mBaseLockList.size()) {
      return mBaseLockList.get(index);
    }
    return mInodes.get(index - mBaseLockList.size());
  }

  @Override
  public synchronized int size() {
    return mBaseLockList.size() + mInodes.size();
  }

  /**
   * @return true if the locklist is empty
   */
  @Override
  public synchronized boolean isEmpty() {
    return mBaseLockList.isEmpty() && mInodes.isEmpty();
  }
}
