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

import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.meta.InodeTree.LockMode;
import alluxio.resource.LockResource;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Manages the locks for a list of {@link Inode}.
 */
@ThreadSafe
public class InodeLockList implements AutoCloseable {
  private static final int INITIAL_CAPACITY = 4;

  protected final InodeLocker mInodeLocker;
  protected final Predicate<Long> mInodeExistsFn;

  protected List<InodeView> mInodes;
  protected List<LockResource> mLocks;
  protected List<LockMode> mLockModes;

  /**
   * Creates a new instance of {@link InodeLockList}.
   *
   * @param inodeLocker manager for inode locks
   * @param inodeExistsFn predicate for determining whether an inode still exists
   */
  public InodeLockList(InodeLocker inodeLocker, Predicate<Long> inodeExistsFn) {
    mInodeLocker = inodeLocker;
    mInodeExistsFn = inodeExistsFn;
    mInodes = new ArrayList<>(INITIAL_CAPACITY);
    mLocks = new ArrayList<>(INITIAL_CAPACITY);
    mLockModes = new ArrayList<>(INITIAL_CAPACITY);
  }

  /**
   * Locks the given inode in read mode, and adds it to this lock list. This call should only be
   * used when locking the root or an inode by id and not path or parent.
   *
   * @param inode the inode to lock
   */
  public synchronized void lockRead(InodeView inode) {
    mInodes.add(inode);
    mLocks.add(mInodeLocker.readLock(inode.getId()));
    mLockModes.add(LockMode.READ);
  }

  /**
   * Locks the given inode in read mode, and adds it to this lock list. This method ensures the
   * parent is the expected parent inode.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param inode the inode to lock
   * @param parent the expected parent inode
   * @throws InvalidPathException if the inode is no long consistent with the caller's expectations
   */
  public synchronized void lockReadAndCheckParent(InodeView inode, InodeView parent)
      throws InvalidPathException {
    mInodes.add(inode);
    mLocks.add(mInodeLocker.readLock(inode.getId()));
    mLockModes.add(LockMode.READ);
    try {
      checkExists(inode);
      checkParent(parent, inode);
    } catch (Throwable t) {
      unlockLast();
      throw t;
    }
  }

  /**
   * Locks the given inode in read mode, and adds it to this lock list. This method ensures the
   * parent is the expected parent inode, and the name of the inode is the expected name.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param inode the inode to lock
   * @param parent the expected parent inode
   * @param name the expected name of the inode to be locked
   * @throws InvalidPathException if the inode is not consistent with the caller's expectations
   */
  public synchronized void lockReadAndCheckNameAndParent(InodeView inode, InodeView parent,
      String name) throws InvalidPathException {
    lockReadAndCheckParent(inode, parent);
    try {
      checkName(inode, name);
    } catch (Throwable t) {
      unlockLast();
      throw t;
    }
  }

  /**
   * Unlocks the last inode that was locked.
   */
  public synchronized void unlockLast() {
    if (mInodes.isEmpty()) {
      return;
    }
    mInodes.remove(mInodes.size() - 1);
    mLocks.remove(mLocks.size() - 1).close();
    mLockModes.remove(mLockModes.size() - 1);
  }

  /**
   * Downgrades the last inode that was locked, if the inode was previously WRITE locked. If the
   * inode was previously READ locked, no additional locking will occur.
   */
  public synchronized void downgradeLast() {
    if (mInodes.isEmpty()) {
      return;
    }
    if (mLockModes.get(mLockModes.size() - 1) == LockMode.WRITE) {
      // The last inode was previously WRITE locked, so downgrade the lock.
      InodeView inode = mInodes.get(mInodes.size() - 1);
      LockResource readLock = mInodeLocker.readLock(inode.getId());
      mLocks.remove(mLocks.size() - 1).close();
      mLocks.add(readLock);

      // Update the last lock mode to READ
      mLockModes.remove(mLockModes.size() - 1);
      mLockModes.add(LockMode.READ);
    }
  }

  /**
   * Locks the given inode in write mode, and adds it to this lock list. This call should only be
   * used when locking the root or an inode by id and not path or parent.
   *
   * @param inode the inode to lock
   */
  public synchronized void lockWrite(InodeView inode) {
    mInodes.add(inode);
    mLocks.add(mInodeLocker.writeLock(inode.getId()));
    mLockModes.add(LockMode.WRITE);
  }

  /**
   * Locks the given inode in write mode, and adds it to this lock list. This method ensures the
   * parent is the expected parent inode.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param inode the inode to lock
   * @param parent the expected parent inode
   * @throws InvalidPathException if the inode is not consistent with the caller's expectations
   */
  public synchronized void lockWriteAndCheckParent(InodeView inode, InodeView parent)
      throws InvalidPathException {
    mInodes.add(inode);
    mLocks.add(mInodeLocker.writeLock(inode.getId()));
    mLockModes.add(LockMode.WRITE);
    try {
      checkExists(inode);
      checkParent(parent, inode);
    } catch (Throwable t) {
      unlockLast();
      throw t;
    }
  }

  /**
   * Locks the given inode in write mode, and adds it to this lock list. This method ensures the
   * parent is the expected parent inode, and the name of the inode is the expected name.
   *
   * NOTE: This method assumes that the inode path to the parent has been read locked.
   *
   * @param inode the inode to lock
   * @param parent the expected parent inode
   * @param name the expected name of the inode to be locked
   * @throws InvalidPathException if the inode is not consistent with the caller's expectations
   */
  public synchronized void lockWriteAndCheckNameAndParent(InodeView inode, InodeView parent,
      String name) throws InvalidPathException {
    lockWriteAndCheckParent(inode, parent);
    try {
      checkName(inode, name);
    } catch (Throwable t) {
      unlockLast();
      throw t;
    }
  }

  private void checkExists(InodeView inode) throws InvalidPathException {
    if (!mInodeExistsFn.test(inode.getId())) {
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID_CONCURRENT_DELETE.getMessage());
    }
  }

  private void checkParent(InodeView parent, InodeView child) throws InvalidPathException {
    if (child.getParentId() != InodeTree.NO_PARENT && child.getParentId() != parent.getId()) {
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    }
  }

  private void checkName(InodeView inode, String name) throws InvalidPathException {
    if (!inode.getName().equals(name)) {
      throw new InvalidPathException(ExceptionMessage.PATH_INVALID_CONCURRENT_RENAME.getMessage());
    }
  }

  /**
   * @return a copy of the the list of inodes locked in this lock list, in order of when
   * the inodes were locked
   */
  // TODO(david): change this API to not return a copy
  public synchronized List<InodeView> getInodes() {
    return Lists.newArrayList(mInodes);
  }

  /**
   * @param index the index of the list
   * @return the inode at the specified index
   */
  public synchronized InodeView get(int index) {
    return mInodes.get(index);
  }

  /**
   * @return the size of the list
   */
  public synchronized int size() {
    return mInodes.size();
  }

  /**
   * @return true if the locklist is empty
   */
  public synchronized boolean isEmpty() {
    return mInodes.isEmpty();
  }

  @Override
  public synchronized void close() {
    for (int i = mInodes.size() - 1; i >= 0; i--) {
      mLocks.get(i).close();
    }
    mInodes.clear();
    mLocks.clear();
    mLockModes.clear();
  }
}
