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

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents a locked path within the inode tree. Both inodes and edges are locked along the path.
 * The path may start from either an edge or an inode.
 *
 * A lock list always contains some number of read locks (possibly zero) followed by some number of
 * write locks (possibly zero).
 *
 * This class uses list notation to describe lock lists. Nodes are single letters, edges are
 * parentName->childName, and write-locks are indicated with '*'. For example, a lock list for
 * /a/b/c/d where the c->d edge is the first write-locked element:
 *
 * [->/, /, /->a, a, a->b, b, b->c, c, c->d*, d*]
 *
 * The "->/" at the start is a pseudo-edge used to allow the WRITE_EDGE lock mode to be applied to
 * the root. This edge can only be locked by calling lockRootEdge.
 *
 * Not all lock lists need to start from the root. Another example list could be
 *
 * [b, b->c, c->d*, d*]
 *
 * This allows us to create composite lock lists out of a "normal" lock list starting from the root,
 * plus a non-root lock list.
 */
@NotThreadSafe
public interface InodeLockList extends AutoCloseable {
  /**
   * Locks the root edge in the specified mode.
   *
   * The lock list must be empty to call this method.
   *
   * @param mode the mode to lock in
   */
  void lockRootEdge(LockMode mode);

  /**
   * Locks the given inode and adds it to the lock list. This method does *not* check that the inode
   * is still a child of the previous inode, or that the inode still exists. This method should only
   * be called when the edge leading to the inode is locked.
   *
   * Example
   * Starting from [a, a->b]
   *
   * lockInode(b, LockMode.READ) results in [a, a->b, b]
   * lockInode(b, LockMode.WRITE) results in [a, a->b, b*]
   *
   * @param inode the inode to lock
   * @param mode the mode to lock in
   */
  void lockInode(Inode inode, LockMode mode);

  /**
   * Locks an edge leading out of the last inode in the list.
   *
   * Example
   * Starting from [a, a->b, b]
   *
   * lockEdge(b, c, LockMode.READ) results in [a, a->b, b, b->c]
   * lockEdge(b, c, LockMode.WRITE) results in [a, a->b, b, b->c*]
   *
   * @param inode the parent inode of the edge
   * @param childName the child name of the edge
   * @param mode the mode to lock in
   */
  void lockEdge(Inode inode, String childName, LockMode mode);

  /**
   * Unlocks the last locked inode.
   *
   * Example
   * Starting from [a, a->b, b]
   *
   * unlockLastInode() results in [a, a->b]
   */
  void unlockLastInode();

  /**
   * Unlocks the last locked edge.
   *
   * Example
   * Starting from [a, a->b]
   *
   * unlockLastEdge results in [a]
   */
  void unlockLastEdge();

  /**
   * Downgrades all locks in the current lock list to read locks.
   */
  void downgradeToReadLocks();

  /**
   * Downgrades the last edge lock in the lock list from WRITE lock to READ lock.
   *
   * Example
   * Starting from [a, a->b*]
   *
   * downgradeLastEdge() results in [a, a->b]
   */
  void downgradeLastEdge();

  /**
   * Leapfrogs the final edge write lock forward, reducing the lock list's write-locked scope.
   *
   * Example
   * Starting from [a, a->b*]
   *
   * pushWriteLockedEdge(b, c) results in [a, a->b, b, b->c*]
   *
   * The read lock on a->b is acquired before releasing the write lock. This ensures that no other
   * thread can take the write lock before the read lock is acquired.
   *
   * If this is a composite lock list and the final write lock is part of the base lock list, the
   * new locks will be acquired but no downgrade will occur.
   *
   * @param inode the inode to add to the lock list
   * @param childName the child name for the edge to add to the lock list
   */
  void pushWriteLockedEdge(Inode inode, String childName);

  /**
   * @return {@link LockMode#WRITE} if the last entry in the list is write-locked, otherwise
   *         {@link LockMode#READ}
   */
  LockMode getLockMode();

  /**
   * @return a copy of all locked inodes
   */
  List<Inode> getLockedInodes();

  /**
   * @return a copy of all locked inodes
   */
  default List<InodeView> getLockedInodeViews() {
    return new ArrayList<>(getLockedInodes());
  }

  /**
   * @param index the index of the list
   * @return the inode at the specified index
   */
  Inode get(int index);

  /**
   * @return the size of the list in terms of locked inodes
   */
  int numInodes();

  /**
   * @return whether this lock list ends in an inode (as opposed to an edge)
   */
  boolean endsInInode();

  /**
   * @return true if the locklist is empty
   */
  boolean isEmpty();

  /**
   * @return the inode lock manager for this lock list
   */
  InodeLockManager getInodeLockManager();

  /**
   * Closes the lock list, releasing all locks.
   */
  @Override
  void close();
}
