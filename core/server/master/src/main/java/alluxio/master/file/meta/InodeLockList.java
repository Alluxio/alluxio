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

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents a locked path within the inode tree. All lock lists must start with the root. Both
 * inodes and edges are locked along the path.
 *
 * A lock list is either in read mode or write mode. In read mode, every lock in the list is a read
 * lock. In write mode, every lock is a read lock except for the final lock.
 *
 * For consistency across locking operations, lock lists begin with a fake edge leading to the root
 * inode. This enables the WRITE_EDGE pattern to be applied to the root.
 */
@ThreadSafe
public class InodeLockList implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(InodeLockList.class);

  private static final int INITIAL_CAPACITY = 4;
  private static final Edge ROOT_EDGE = new Edge(-1, "");

  /**
   * Whether the lock list starts with an inode. For normal lock lists this is always false, but it
   * may be true for {@link CompositeInodeLockList}s if their base lock lists ends with an edge.
   */
  private final boolean mStartsWithInode;

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
    this(inodeLockManager, false);
  }

  /**
   * Creates a new lock list. This constructor is used by {@link CompositeInodeLockList} so that
   * it can start with either an inode or an edge.
   *
   * @param inodeLockManager manager for inode locks
   */
  InodeLockList(InodeLockManager inodeLockManager, boolean startsWithInode) {
    mStartsWithInode = startsWithInode;
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
    Preconditions.checkState(!endsInInode());
    Preconditions.checkState(inode.getName().equals(lastEntry().mEdge.getName()));

    mLockedInodes.add(inode);
    mEntries.add(Entry.forInode(mInodeLockManager.lockInode(inode, mode), inode));
    mLockMode = mode;
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

    InodeView lastInode = get(numLockedInodes() - 1);
    Edge edge = new Edge(lastInode.getId(), childName);
    mEntries.add(Entry.forEdge(mInodeLockManager.lockEdge(edge, mode), edge));
    mLockMode = mode;
  }

  /**
   * Locks the root edge in the specified mode.
   *
   * @param mode the mode to lock in
   */
  public void lockRootEdge(LockMode mode) {
    Preconditions.checkState(mEntries.isEmpty());

    mEntries.add(Entry.forEdge(mInodeLockManager.lockEdge(ROOT_EDGE, mode), ROOT_EDGE));
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
   * @param inode the inode to add to the lock list
   * @param pathComponent the child name for the edge to add to the lock list
   */
  public synchronized void pushWriteLockedEdge(InodeView inode, String pathComponent) {
    Preconditions.checkState(mLockMode == LockMode.WRITE);

    lockInode(inode, LockMode.READ);
    lockEdge(pathComponent, LockMode.WRITE);
    // downgrade the second to last edge lock.
    downgradeNthToLastEdge(2);
  }

  /**
   * @return whether this lock list ends in an inode (as opposed to an edge)
   */
  public synchronized boolean endsInInode() {
    if (mStartsWithInode) {
      return mEntries.size() % 2 == 1;
    } else {
      return mEntries.size() % 2 == 0;
    }
  }

  /**
   * Unlocks the last locked inode.
   */
  public synchronized void unlockLastInode() {
    Preconditions.checkState(endsInInode());

    mLockedInodes.remove(mLockedInodes.size() - 1);
    mEntries.remove(mEntries.size() - 1).mLock.close();
    mLockMode = LockMode.READ;
  }

  /**
   * Unlocks the last locked edge.
   */
  public synchronized void unlockLastEdge() {
    Preconditions.checkState(!endsInInode());

    mEntries.remove(mEntries.size() - 1).mLock.close();
    mLockMode = LockMode.READ;
  }

  /**
   * Downgrades the last inode from a write lock to a read lock. The read lock is acquired before
   * releasing the write lock.
   */
  public void downgradeLastInode() {
    Preconditions.checkState(endsInInode());
    Preconditions.checkState(mLockMode == LockMode.WRITE);

    Entry last = mEntries.get(mEntries.size() - 1);
    LockResource lock = mInodeLockManager.lockInode(last.mInode, LockMode.READ);
    last.mLock.close();
    mEntries.set(mEntries.size() - 1, Entry.forInode(lock, last.mInode));
    mLockMode = LockMode.READ;
  }

  /**
   * Downgrades the last edge lock in the lock list from WRITE lock to READ lock.
   */
  public synchronized void downgradeLastEdge() {
    Preconditions.checkNotNull(!endsInInode());
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
    Preconditions.checkState(mLockMode == LockMode.WRITE);

    Entry last = mEntries.get(mEntries.size() - 1);
    LockResource inodeLock = mInodeLockManager.lockInode(inode, mode);
    LockResource edgeLock = mInodeLockManager.lockEdge(last.mEdge, LockMode.READ);
    last.mLock.close();
    mEntries.set(mEntries.size() - 1, Entry.forEdge(edgeLock, last.mEdge));
    mEntries.add(Entry.forInode(inodeLock, inode));
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

    Entry entry = mEntries.get(entryIndex);
    LockResource lock = mInodeLockManager.lockEdge(entry.mEdge, LockMode.READ);
    entry.mLock.close();
    mEntries.set(entryIndex, Entry.forEdge(lock, entry.mEdge));
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
  public synchronized List<InodeView> getInodes() {
    return Lists.newArrayList(mLockedInodes);
  }

  /**
   * @param index the index of the list
   * @return the inode at the specified index
   */
  public synchronized InodeView get(int index) {
    return mLockedInodes.get(index);
  }

  /**
   * @return the size of the list in terms of locked inodes
   */
  public synchronized int numLockedInodes() {
    return mLockedInodes.size();
  }

  /**
   * @return true if the locklist is empty
   */
  public synchronized boolean isEmpty() {
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
  public synchronized void close() {
    mLockedInodes.clear();
    mEntries.forEach(entry -> entry.mLock.close());
  }

  protected static class Entry {
    private final LockResource mLock;
    // entries are either mInode or mEdge.
    @Nullable
    private final InodeView mInode;
    @Nullable
    private final Edge mEdge;

    private Entry(LockResource lock, InodeView inode, Edge edge) {
      mLock = lock;
      mInode = inode;
      mEdge = edge;
    }

    public static Entry forEdge(LockResource lock, Edge edge) {
      return new Entry(lock, null, edge);
    }

    public static Entry forInode(LockResource lock, InodeView inode) {
      return new Entry(lock, inode, null);
    }

    @Override
    public String toString() {
      if (mInode != null) {
        return "\"" + mInode.getName() + "\"";
      }
      return mEdge.toString();
    }
  }
}
