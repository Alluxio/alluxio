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

import alluxio.master.file.meta.InodeTree.LockMode;
import alluxio.resource.LockResource;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Represents a locked path within the inode tree. All lock lists must start with the root. Both
 * inodes and edges are locked along the path.
 *
 * A lock list is either in read mode or write mode. In read mode, every lock in the list is a read
 * lock. In write mode, every lock is a read lock except for the final lock. In write mode, the
 * lock list cannot be extended (only the final lock is ever a write lock).
 *
 * For consistency across locking operations, lock lists begin with a fake edge leading to the root
 * inode. This enables the WRITE_EDGE pattern to be applied to the root.
 *
 * InodeLockLists are not thread safe and should not be passed across threads.
 */
@NotThreadSafe
public class InodeLockList implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(InodeLockList.class);

  private static final int INITIAL_CAPACITY = 4;
  private static final Edge ROOT_EDGE = new Edge(-1, "");

  protected final InodeLockManager mInodeLockManager;

  /** The set of inodes that have been locked by the lock list. */
  protected List<InodeView> mLockedInodes;
  /** Entries for each lock in the lock list, ordered from the root. */
  protected List<Entry> mEntries;
  /** The current lock mode for the lock list, either read or write. */
  protected LockMode mLockMode;

  /**
   * Creates a new empty lock list.
   *
   * @param inodeLockManager manager for inode locks
   */
  public InodeLockList(InodeLockManager inodeLockManager) {
    mInodeLockManager = inodeLockManager;
    mLockedInodes = new ArrayList<>(INITIAL_CAPACITY);
    mEntries = new ArrayList<>(INITIAL_CAPACITY);

    mLockMode = LockMode.READ;
  }

  /**
   * Locks the given inode and adds it to the lock list. This method does *not* check that the inode
   * is still a child of the previous inode, or that the inode still exists. This method should only
   * be called when the edge leading to the inode is locked.
   *
   * @param inode the inode to lock
   * @param mode the mode to lock in
   */
  public void lockInode(InodeView inode, LockMode mode) {
    Preconditions.checkState(mLockMode == LockMode.READ);

    lockInodeInternal(inode, mode);
    mLockMode = mode;
  }

  /**
   * Locks the next inode without checking or updating the mode.
   *
   * @param inode the inode to lock
   * @param mode the mode to lock in
   */
  private void lockInodeInternal(InodeView inode, LockMode mode) {
    Preconditions.checkState(!endsInInode());
    Preconditions.checkState(inode.getName().equals(((EdgeEntry) lastEntry()).getEdge().getName()));

    mLockedInodes.add(inode);
    mEntries.add(new InodeEntry(mInodeLockManager.lockInode(inode, mode), inode));
  }

  /**
   * Locks an edge leading out of the last inode in the list.
   *
   * For example, if the lock list is [a, a->b, b], lockEdge(c) will add b->c to the list, resulting
   * in [a, a->b, b, b->c].
   *
   * @param childName the child to lock
   * @param mode the mode to lock in
   */
  public void lockEdge(String childName, LockMode mode) {
    Preconditions.checkState(endsInInode());
    Preconditions.checkState(mLockMode == LockMode.READ);

    lockEdgeInternal(childName, mode);
    mLockMode = mode;
  }

  /**
   * Locks the next edge without checking or updating the mode.
   *
   * @param childName the child to lock
   * @param mode the mode to lock in
   */
  public void lockEdgeInternal(String childName, LockMode mode) {
    Preconditions.checkState(endsInInode());

    InodeView lastInode = get(numLockedInodes() - 1);
    Edge edge = new Edge(lastInode.getId(), childName);
    mEntries.add(new EdgeEntry(mInodeLockManager.lockEdge(edge, mode), edge));
  }

  /**
   * Locks the root edge in the specified mode.
   *
   * @param mode the mode to lock in
   */
  public void lockRootEdge(LockMode mode) {
    Preconditions.checkState(mEntries.isEmpty());

    mEntries.add(new EdgeEntry(mInodeLockManager.lockEdge(ROOT_EDGE, mode), ROOT_EDGE));
    mLockMode = mode;
  }

  /**
   * Leapfrogs the edge write lock forward, reducing the lock list's write-locked scope.
   *
   * For example, if the lock list is in write mode with entries [a, a->b, b, b->c],
   * pushWriteLockedEdge(c, d) will change the list to [a, a->b, b, b->c, c, c->d]. The c->d edge
   * will be write locked instead of the b->c edge. At least a read lock on b->c will be maintained
   * throughout the process so that other threads cannot interfere with creates, deletes, or
   * renames.
   *
   * For composite lock lists, this method will do nothing if the base lock is is write locked.
   *
   * @param inode the inode to add to the lock list
   * @param childName the child name for the edge to add to the lock list
   */
  public void pushWriteLockedEdge(InodeView inode, String childName) {
    Preconditions.checkState(!endsInInode());
    Preconditions.checkState(mLockMode == LockMode.WRITE);

    if (mEntries.isEmpty()) {
      // Cannot modify the base lock list, and the new inode is already implicitly locked.
      return;
    }

    lockInodeInternal(inode, LockMode.READ);
    lockEdgeInternal(childName, LockMode.WRITE);
    // downgrade the second to last edge lock.
    downgradeNthToLastEdge(2);
  }

  /**
   * @return whether this lock list ends in an inode (as opposed to an edge)
   */
  public boolean endsInInode() {
    return lastEntry() instanceof InodeEntry;
  }

  /**
   * Unlocks the last locked inode.
   */
  public void unlockLastInode() {
    Preconditions.checkState(endsInInode());
    Preconditions.checkState(!mEntries.isEmpty());

    mLockedInodes.remove(mLockedInodes.size() - 1);
    mEntries.remove(mEntries.size() - 1).mLock.close();
    mLockMode = LockMode.READ;
  }

  /**
   * Unlocks the last locked edge.
   */
  public void unlockLastEdge() {
    Preconditions.checkState(!endsInInode());
    Preconditions.checkState(!mEntries.isEmpty());

    mEntries.remove(mEntries.size() - 1).mLock.close();
    mLockMode = LockMode.READ;
  }

  /**
   * Downgrades the last inode from a write lock to a read lock. The read lock is acquired before
   * releasing the write lock.
   */
  public void downgradeLastInode() {
    Preconditions.checkState(endsInInode());
    Preconditions.checkState(!mEntries.isEmpty());
    Preconditions.checkState(mLockMode == LockMode.WRITE);

    InodeEntry last = (InodeEntry) mEntries.get(mEntries.size() - 1);
    LockResource lock = mInodeLockManager.lockInode(last.getInode(), LockMode.READ);
    last.getLock().close();
    mEntries.set(mEntries.size() - 1, new InodeEntry(lock, last.mInode));
    mLockMode = LockMode.READ;
  }

  /**
   * Downgrades the last edge lock in the lock list from WRITE lock to READ lock.
   */
  public void downgradeLastEdge() {
    Preconditions.checkNotNull(!endsInInode());
    Preconditions.checkState(!mEntries.isEmpty());
    Preconditions.checkState(mLockMode == LockMode.WRITE);

    downgradeNthToLastEdge(1);
    mLockMode = LockMode.READ;
  }

  /**
   * Downgrades from edge write-locking to inode write-locking. This reduces the scope of the write
   * lock by pushing it forward one entry.
   *
   * For example, if the lock list is in write mode with entries [a, a->b, b, b->c],
   * downgradeEdgeToInode(c, mode) will change the list to [a, a->b, b, b->c, c], with b->c
   * downgraded to a read lock. c will be locked according to the mode.
   *
   * The read lock on the final edge is taken before releasing the write lock.
   *
   * @param inode the next inode in the lock list
   * @param mode the mode to downgrade to
   */
  public void downgradeEdgeToInode(InodeView inode, LockMode mode) {
    Preconditions.checkState(!endsInInode());
    Preconditions.checkState(!mEntries.isEmpty());
    Preconditions.checkState(mLockMode == LockMode.WRITE);

    EdgeEntry last = (EdgeEntry) mEntries.get(mEntries.size() - 1);
    LockResource inodeLock = mInodeLockManager.lockInode(inode, mode);
    LockResource edgeLock = mInodeLockManager.lockEdge(last.mEdge, LockMode.READ);
    last.getLock().close();
    mEntries.set(mEntries.size() - 1, new EdgeEntry(edgeLock, last.getEdge()));
    mEntries.add(new InodeEntry(inodeLock, inode));
    mLockedInodes.add(inode);
    mLockMode = mode;
  }

  /**
   * Downgrades the nth to last edge from a write lock to a read lock. The read lock is taken before
   * releasing the write lock.
   */
  private void downgradeNthToLastEdge(int i) {
    int endOffset = (endsInInode() ? 0 : -1) + 2 * i;
    int entryIndex = mEntries.size() - endOffset;

    EdgeEntry entry = (EdgeEntry) mEntries.get(entryIndex);
    LockResource lock = mInodeLockManager.lockEdge(entry.mEdge, LockMode.READ);
    entry.getLock().close();
    mEntries.set(entryIndex, new EdgeEntry(lock, entry.getEdge()));
  }

  /**
   * @return the current lock mode for the lock list
   */
  public LockMode getLockMode() {
    return mLockMode;
  }

  /**
   * @return a copy of the the list of inodes locked in this lock list, in order of when
   * the inodes were locked
   */
  // TODO(david): change this API to not return a copy
  public List<InodeView> getInodes() {
    return Lists.newArrayList(mLockedInodes);
  }

  /**
   * @param index the index of the list
   * @return the inode at the specified index
   */
  public InodeView get(int index) {
    return mLockedInodes.get(index);
  }

  /**
   * @return the size of the list in terms of locked inodes
   */
  public int numLockedInodes() {
    return mLockedInodes.size();
  }

  /**
   * @return true if the locklist is empty
   */
  public boolean isEmpty() {
    return mEntries.isEmpty();
  }

  /**
   * @return the last lock entry in the lock list
   */
  protected Entry lastEntry() {
    return mEntries.get(mEntries.size() - 1);
  }

  @Override
  public String toString() {
    String path =
        getInodes().stream().map(inode -> inode.getName()).collect(Collectors.joining("/"));
    return String.format("Locked Inodes: <%s>%n"
        + "Entries: %s%n"
        + "Lock Mode: %s", path, mEntries, mLockMode);
  }

  @Override
  public void close() {
    mLockedInodes.clear();
    mEntries.forEach(entry -> entry.mLock.close());
    mEntries.clear();
  }

  protected abstract static class Entry {
    private final LockResource mLock;

    protected Entry(LockResource lock) {
      mLock = lock;
    }

    protected LockResource getLock() {
      return mLock;
    }
  }

  protected static class InodeEntry extends Entry {
    private final InodeView mInode;

    private InodeEntry(LockResource lock, InodeView inode) {
      super(lock);
      mInode = inode;
    }

    public InodeView getInode() {
      return mInode;
    }

    @Override
    public String toString() {
      return "\"" + mInode.getName() + "\"";
    }
  }

  protected static class EdgeEntry extends Entry {
    private final Edge mEdge;

    private EdgeEntry(LockResource lock, Edge edge) {
      super(lock);
      mEdge = edge;
    }

    public Edge getEdge() {
      return mEdge;
    }

    @Override
    public String toString() {
      return mEdge.toString();
    }
  }
}
