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

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A simple inode lock list.
 */
@NotThreadSafe
public class SimpleInodeLockList implements InodeLockList {
  private static final Logger LOG = LoggerFactory.getLogger(InodeLockList.class);

  private static final int INITIAL_CAPACITY = 8;
  private static final Edge ROOT_EDGE = new Edge(-1, "");

  private final InodeLockManager mInodeLockManager;

  /**
   * Entries for each lock in the lock list. The entries always alternate between InodeEntry and
   * EdgeEntry.
   */
  private List<Entry> mEntries;
  /**
   * The index of the first write lock entry. All locks before this index are read locks, and all
   * locks after and including this index are write locks. If all locks are read locks,
   * mFirstWriteLockIndex == mEntries.size().
   */
  private int mFirstWriteLockIndex;

  /**
   * Creates a new empty lock list.
   *
   * @param inodeLockManager manager for inode locks
   */
  public SimpleInodeLockList(InodeLockManager inodeLockManager) {
    mInodeLockManager = inodeLockManager;
    mEntries = new ArrayList<>(INITIAL_CAPACITY);
    mFirstWriteLockIndex = 0;
  }

  @Override
  public void lockInode(Inode inode, LockMode mode) {
    mode = nextLockMode(mode);
    if (!mEntries.isEmpty()) {
      Preconditions.checkState(!endsInInode());
      String lastEdgeName = ((EdgeEntry) lastEntry()).getEdge().getName();
      Preconditions.checkState(inode.getName().equals(lastEdgeName),
          "Expected to lock inode %s but locked inode %s", lastEdgeName, inode.getName());
    }
    addEntry(new InodeEntry(mInodeLockManager.lockInode(inode, mode), inode), mode);
  }

  @Override
  public void lockEdge(Inode lastInode, String childName, LockMode mode) {
    mode = nextLockMode(mode);
    long edgeParentId = lastInode.getId();
    if (!mEntries.isEmpty()) {
      Preconditions.checkState(endsInInode());
      Preconditions.checkState(lastInode().getId() == edgeParentId);
    }
    Edge edge = new Edge(edgeParentId, childName);
    addEntry(new EdgeEntry(mInodeLockManager.lockEdge(edge, mode), edge), mode);
  }

  @Override
  public void lockRootEdge(LockMode mode) {
    Preconditions.checkState(mEntries.isEmpty());
    addEntry(new EdgeEntry(mInodeLockManager.lockEdge(ROOT_EDGE, mode), ROOT_EDGE), mode);
    if (mode == LockMode.WRITE) {
      mFirstWriteLockIndex = 0;
    }
  }

  @Override
  public void pushWriteLockedEdge(Inode inode, String childName) {
    Preconditions.checkState(!endsInInode());
    Preconditions.checkState(endsInWriteLock());

    int edgeIndex = mEntries.size() - 1;
    if (mFirstWriteLockIndex < edgeIndex) {
      // If the lock before the edge lock is already WRITE, we can just acquire more WRITE locks.
      lockInode(inode, LockMode.WRITE);
      lockEdge(inode, childName, LockMode.WRITE);
    } else {
      lockInode(inode, LockMode.READ);
      lockEdge(inode, childName, LockMode.WRITE);
      downgradeEdge(edgeIndex);
    }
  }

  /**
   * If the lock list is write locked, returns LockMode.WRITE. Otherwise, returns the given mode.
   *
   * @param mode a lock mode
   * @return the mode
   */
  private LockMode nextLockMode(LockMode mode) {
    if (endsInWriteLock()) {
      return LockMode.WRITE;
    }
    return mode;
  }

  @Override
  public void unlockLastInode() {
    Preconditions.checkState(endsInInode());
    Preconditions.checkState(!mEntries.isEmpty());
    removeLastEntry();
  }

  @Override
  public void unlockLastEdge() {
    Preconditions.checkState(!endsInInode());
    Preconditions.checkState(!mEntries.isEmpty());
    removeLastEntry();
  }

  @Override
  public void downgradeLastInode() {
    Preconditions.checkState(endsInInode());
    Preconditions.checkState(!mEntries.isEmpty());
    Preconditions.checkState(endsInWriteLock());

    if (endsInMultipleWriteLocks()) {
      return; // No point in changing from write lock to read lock when the parent lock is WRITE.
    }

    Inode lastInode = lastInode();
    LockResource lock = mInodeLockManager.lockInode(lastInode, LockMode.READ);
    removeLastEntry();
    addEntry(new InodeEntry(lock, lastInode), LockMode.READ);
  }

  @Override
  public void downgradeLastEdge() {
    Preconditions.checkState(!endsInInode());
    Preconditions.checkState(!mEntries.isEmpty());
    Preconditions.checkState(endsInWriteLock());

    if (!endsInMultipleWriteLocks()) {
      downgradeEdge(mEntries.size() - 1);
    }
  }

  @Override
  public void downgradeEdgeToInode(Inode inode, LockMode mode) {
    Preconditions.checkState(!endsInInode());
    Preconditions.checkState(!mEntries.isEmpty());
    Preconditions.checkState(endsInWriteLock());

    if (endsInMultipleWriteLocks()) {
      lockInode(inode, LockMode.WRITE);
      return;
    }

    Edge lastEdge = lastEdge();
    LockResource inodeLock = mInodeLockManager.lockInode(inode, mode);
    LockResource edgeLock = mInodeLockManager.lockEdge(lastEdge, LockMode.READ);
    removeLastEntry();
    addEntry(new EdgeEntry(edgeLock, lastEdge), nextLockMode(LockMode.READ));
    addEntry(new InodeEntry(inodeLock, inode), nextLockMode(mode));
  }

  /**
   * Adds an entry to mEntries.
   *
   * @param entry the entry to add
   * @param mode the mode the entry is locked in
   */
  private void addEntry(Entry entry, LockMode mode) {
    if (!endsInWriteLock() && mode == LockMode.READ) {
      mFirstWriteLockIndex++;
    }
    mEntries.add(entry);
  }

  /**
   * Removes and unlocks the last entry from mEntries.
   */
  private void removeLastEntry() {
    mEntries.remove(mEntries.size() - 1).getLock().close();
    if (mFirstWriteLockIndex > mEntries.size()) {
      mFirstWriteLockIndex = mEntries.size();
    }
  }

  /**
   * Downgrades the edge at the specified entry index.
   */
  private void downgradeEdge(int edgeEntryIndex) {
    EdgeEntry entry = (EdgeEntry) mEntries.get(edgeEntryIndex);
    LockResource lock = mInodeLockManager.lockEdge(entry.getEdge(), LockMode.READ);
    entry.getLock().close();
    mEntries.set(edgeEntryIndex, new EdgeEntry(lock, entry.getEdge()));
    if (mFirstWriteLockIndex == edgeEntryIndex) {
      mFirstWriteLockIndex++;
    }
  }

  @Override
  public LockMode getLockMode() {
    return endsInWriteLock() ? LockMode.WRITE : LockMode.READ;
  }

  @Override
  public List<Inode> getLockedInodes() {
    return mEntries.stream()
        .filter(e -> e instanceof InodeEntry)
        .map(e -> ((InodeEntry) e).getInode())
        .collect(Collectors.toList());
  }

  @Override
  public Inode get(int index) {
    int entryIndex = mEntries.get(0) instanceof InodeEntry ? index * 2 : index * 2 + 1;
    return ((InodeEntry) mEntries.get(entryIndex)).getInode();
  }

  @Override
  public int numInodes() {
    if (mEntries.isEmpty()) {
      return 0;
    }
    if (mEntries.get(0) instanceof InodeEntry) {
      return (mEntries.size() + 1) / 2;
    }
    return mEntries.size() / 2;
  }

  @Override
  public boolean isEmpty() {
    return mEntries.isEmpty();
  }

  @Override
  public InodeLockManager getInodeLockManager() {
    return mInodeLockManager;
  }

  @Override
  public boolean endsInInode() {
    return lastEntry() instanceof InodeEntry;
  }

  /**
   * @return the last inode
   */
  private Inode lastInode() {
    Preconditions.checkState(endsInInode());
    return ((InodeEntry) lastEntry()).getInode();
  }

  /**
   * @return the last edge
   */
  private Edge lastEdge() {
    Preconditions.checkState(!endsInInode());
    return ((EdgeEntry) lastEntry()).getEdge();
  }

  /**
   * @return whether this lock list ends in a write lock
   */
  private boolean endsInWriteLock() {
    return mFirstWriteLockIndex < mEntries.size();
  }

  private boolean endsInMultipleWriteLocks() {
    return mFirstWriteLockIndex < mEntries.size() - 1;
  }

  /**
   * @return the last entry in the lock list, or null if the lock list contains no entries
   */
  @Nullable
  private Entry lastEntry() {
    return mEntries.isEmpty() ? null : mEntries.get(mEntries.size() - 1);
  }

  @Override
  public String toString() {
    String path = mEntries.stream()
        .filter(e -> e instanceof InodeEntry)
        .map(e -> ((InodeEntry) e).getInode().getName())
        .collect(Collectors.joining("/"));
    return String.format("Path: <%s>%nEntries: %s%nFirst write-locked index: %s", path, mEntries,
        mFirstWriteLockIndex);
  }

  @Override
  public void close() {
    mEntries.forEach(entry -> entry.getLock().close());
    mEntries.clear();
  }

  private abstract static class Entry {
    private final LockResource mLock;

    protected Entry(LockResource lock) {
      mLock = lock;
    }

    protected LockResource getLock() {
      return mLock;
    }
  }

  private static class InodeEntry extends Entry {
    private final Inode mInode;

    public InodeEntry(LockResource lock, Inode inode) {
      super(lock);
      mInode = inode;
    }

    public Inode getInode() {
      return mInode;
    }

    @Override
    public String toString() {
      return "\"" + mInode.getName() + "\"";
    }
  }

  private static class EdgeEntry extends Entry {
    private final Edge mEdge;

    public EdgeEntry(LockResource lock, Edge edge) {
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
