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
import alluxio.resource.LockResource;
import alluxio.resource.RWLockResource;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A simple inode lock list.
 */
@NotThreadSafe
public class SimpleInodeLockList implements InodeLockList {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleInodeLockList.class);

  /**
   * Default value for {@link #mFirstWriteLockIndex} when there is no write lock.
   */
  private static final int NO_WRITE_LOCK_INDEX = -1;
  private static final Edge ROOT_EDGE = new Edge(-1, "");

  private final InodeLockManager mInodeLockManager;

  /**
   * Inode list.
   */
  private LinkedList<Inode> mInodes;
  /**
   * If last lock in mLocks is an edge lock, then this is the edge.
   * Otherwise, null.
   */
  private Edge mLastEdge;
  /**
   * Lock list.
   * The locks always alternate between Inode lock and Edge lock.
   * The first lock can be either Inode or Edge lock.
   */
  private LinkedList<RWLockResource> mLocks;
  /**
   * The index of the first write lock entry in {@link #mLocks}.
   * If all locks are read locks, mFirstWriteLockIndex == NO_WRITE_LOCK.
   * Otherwise, all locks before this index are read locks, and all
   * locks after and including this index are write locks.
   */
  private int mFirstWriteLockIndex;

  /** Whether to use {@link Lock#tryLock()} or {@link Lock#lock()}. */
  private final boolean mUseTryLock;

  /**
   * Creates a new empty lock list.
   *
   * @param inodeLockManager manager for inode locks
   * @param useTryLock whether or not use {@link Lock#tryLock()} or {@link Lock#lock()}
   */
  public SimpleInodeLockList(InodeLockManager inodeLockManager, boolean useTryLock) {
    mInodeLockManager = inodeLockManager;
    mInodes = new LinkedList<>();
    mLocks = new LinkedList<>();
    mFirstWriteLockIndex = NO_WRITE_LOCK_INDEX;
    mUseTryLock = useTryLock;
  }

  @Override
  public void lockInode(Inode inode, LockMode mode) {
    mode = nextLockMode(mode);
    if (!mLocks.isEmpty()) {
      Preconditions.checkState(!endsInInode(),
          "Cannot lock inode %s for lock list %s because the lock list already ends in an inode",
          inode.getId(), this);
      Preconditions.checkState(inode.getName().equals(mLastEdge.getName()),
          "Expected to lock inode %s but locked inode %s", mLastEdge.getName(), inode.getName());
    }
    lockAndAddInode(inode, mode);
  }

  @Override
  public void lockEdge(Inode lastInode, String childName, LockMode mode) {
    mode = nextLockMode(mode);
    long edgeParentId = lastInode.getId();
    Edge edge = new Edge(lastInode.getId(), childName);
    if (!mLocks.isEmpty()) {
      Preconditions.checkState(endsInInode(),
          "Cannot lock edge %s when lock list %s already ends in an edge", edge, this);
      Preconditions.checkState(lastInode().getId() == edgeParentId,
          "Cannot lock edge %s when the last inode id in %s is %s", edge, this, lastInode.getId());
    }
    lockAndAddEdge(edge, mode);
  }

  @Override
  public void lockRootEdge(LockMode mode) {
    Preconditions.checkState(mLocks.isEmpty(),
        "Cannot lock root edge when lock list %s is nonempty", this);
    lockAndAddEdge(ROOT_EDGE, mode);
  }

  @Override
  public void pushWriteLockedEdge(Inode inode, String childName) {
    Edge edge = new Edge(inode.getId(), childName);
    Preconditions.checkState(!endsInInode(),
        "Cannot push edge write lock to edge %s; lock list %s ends in an inode", edge, this);
    Preconditions.checkState(endsInWriteLock(),
        "Cannot push write lock; lock list %s ends in a read lock");

    if (endsInMultipleWriteLocks()) {
      // If the lock before the edge lock is already WRITE, we can just acquire more WRITE locks.
      lockInode(inode, LockMode.WRITE);
      lockEdge(inode, childName, LockMode.WRITE);
    } else {
      Edge lastEdge = lastEdge();
      RWLockResource lastEdgeReadLock = mInodeLockManager.lockEdge(lastEdge, LockMode.READ,
          mUseTryLock);
      RWLockResource inodeLock = mInodeLockManager.lockInode(inode, LockMode.READ, mUseTryLock);
      RWLockResource nextEdgeLock = mInodeLockManager.lockEdge(edge, LockMode.WRITE, mUseTryLock);
      removeLastLock(); // Remove edge write lock
      addEdgeLock(lastEdge, LockMode.READ, lastEdgeReadLock);
      addInodeLock(inode, LockMode.READ, inodeLock);
      addEdgeLock(edge, LockMode.WRITE, nextEdgeLock);
    }
  }

  @Override
  public void unlockLastInode() {
    Preconditions.checkState(endsInInode(),
        "Cannot unlock last inode when the lock list %s ends in an edge", this);
    Preconditions.checkState(!mLocks.isEmpty(),
        "Cannot unlock last inode when the lock list is empty");
    removeLastLock();
  }

  @Override
  public void unlockLastEdge() {
    Preconditions.checkState(!endsInInode(),
        "Cannot unlock last edge when the lock list %s ends in an inode", this);
    Preconditions.checkState(!mLocks.isEmpty(),
        "Cannot unlock last edge when the lock list is empty");
    removeLastLock();
  }

  @Override
  public void downgradeToReadLocks() {
    mLocks.forEach(RWLockResource::downgrade);
    mFirstWriteLockIndex = NO_WRITE_LOCK_INDEX;
  }

  @Override
  public void downgradeLastEdge() {
    Preconditions.checkState(!endsInInode(),
        "Cannot downgrade last edge when lock list %s ends in an inode", this);
    Preconditions.checkState(!mLocks.isEmpty(),
        "Cannot downgrade last edge when the lock list is empty");
    Preconditions.checkState(endsInWriteLock(),
        "Cannot downgrade last edge when lock list %s is not write locked", this);

    if (!endsInMultipleWriteLocks()) {
      Edge lastEdge = lastEdge();
      RWLockResource newLock = mInodeLockManager.lockEdge(lastEdge, LockMode.READ, mUseTryLock);
      removeLastLock();
      addEdgeLock(lastEdge, LockMode.READ, newLock);
    }
  }

  /**
   * If mode is read but the lock list is write locked, returns LockMode.WRITE.
   *
   * This helps us preserve the invariant that there is never a READ lock following a WRITE lock.
   *
   * @param mode a lock mode
   * @return the mode
   */
  private LockMode nextLockMode(LockMode mode) {
    return endsInWriteLock() ? LockMode.WRITE : mode;
  }

  private void addLock(RWLockResource lock, LockMode mode) {
    if (!endsInWriteLock() && mode == LockMode.WRITE) {
      mFirstWriteLockIndex = mLocks.size();
    }
    mLocks.add(lock);
  }

  private void addInodeLock(Inode inode, LockMode mode, RWLockResource lock) {
    mInodes.add(inode);
    mLastEdge = null;
    addLock(lock, mode);
  }

  private void lockAndAddInode(Inode inode, LockMode mode) {
    addInodeLock(inode, mode, mInodeLockManager.lockInode(inode, mode, mUseTryLock));
  }

  private void addEdgeLock(Edge edge, LockMode mode, RWLockResource lock) {
    mLastEdge = edge;
    addLock(lock, mode);
  }

  private void lockAndAddEdge(Edge edge, LockMode mode) {
    addEdgeLock(edge, mode, mInodeLockManager.lockEdge(edge, mode, mUseTryLock));
  }

  /**
   * Removes and unlocks the last lock.
   */
  private void removeLastLock() {
    mLocks.removeLast().close();
    if (mFirstWriteLockIndex >= mLocks.size()) {
      mFirstWriteLockIndex = NO_WRITE_LOCK_INDEX;
    }
    if (mLastEdge != null) {
      mLastEdge = null;
    } else {
      Inode last = mInodes.removeLast();
      if (!mLocks.isEmpty()) {
        if (mInodes.isEmpty()) {
          mLastEdge = ROOT_EDGE;
        } else {
          mLastEdge = new Edge(mInodes.getLast().getId(), last.getName());
        }
      }
    }
  }

  @Override
  public LockMode getLockMode() {
    return endsInWriteLock() ? LockMode.WRITE : LockMode.READ;
  }

  @Override
  public List<Inode> getLockedInodes() {
    return new ArrayList<>(mInodes);
  }

  @Override
  public Inode get(int index) {
    return mInodes.get(index);
  }

  @Override
  public int numInodes() {
    return mInodes.size();
  }

  @Override
  public boolean isEmpty() {
    return mLocks.isEmpty();
  }

  @Override
  public InodeLockManager getInodeLockManager() {
    return mInodeLockManager;
  }

  @Override
  public boolean endsInInode() {
    return mLastEdge == null;
  }

  /**
   * @return the last inode
   */
  private Inode lastInode() {
    Preconditions.checkState(endsInInode(),
        "Cannot get last inode for lock list %s which does not end in an inode", this);
    return mInodes.getLast();
  }

  /**
   * @return the last edge
   */
  private Edge lastEdge() {
    Preconditions.checkState(!endsInInode(),
        "Cannot get last edge for lock list %s which does not end in an edge", this);
    return mLastEdge;
  }

  /**
   * @return whether this lock list ends in a write lock
   */
  private boolean endsInWriteLock() {
    return mFirstWriteLockIndex != NO_WRITE_LOCK_INDEX;
  }

  private boolean endsInMultipleWriteLocks() {
    return mFirstWriteLockIndex != NO_WRITE_LOCK_INDEX && mFirstWriteLockIndex < mLocks.size() - 1;
  }

  @Override
  public String toString() {
    String path = mInodes.stream()
        .map(Inode::getName)
        .collect(Collectors.joining("/"));
    StringBuilder sb = new StringBuilder("Path: " + path);
    if (mLastEdge != null) {
      sb.append(String.format(", Last edge -> %s", mLastEdge));
    }
    sb.append(String.format(", Index of first write lock: %d", mFirstWriteLockIndex));
    return sb.toString();
  }

  @Override
  public void close() {
    mInodes.clear();
    mLocks.forEach(LockResource::close);
    mLocks.clear();
  }
}
