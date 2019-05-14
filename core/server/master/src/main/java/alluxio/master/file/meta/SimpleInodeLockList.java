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
  private static final Logger LOG = LoggerFactory.getLogger(SimpleInodeLockList.class);

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
      Preconditions.checkState(!endsInInode(),
          "Cannot lock inode %s for lock list %s because the lock list already ends in an inode",
          inode.getId(), this);
      String lastEdgeName = ((EdgeEntry) lastEntry()).getEdge().getName();
      Preconditions.checkState(inode.getName().equals(lastEdgeName),
          "Expected to lock inode %s but locked inode %s", lastEdgeName, inode.getName());
    }
    addEntry(createInodeEntry(inode, mode));
  }

  @Override
  public void lockEdge(Inode lastInode, String childName, LockMode mode) {
    mode = nextLockMode(mode);
    long edgeParentId = lastInode.getId();
    Edge edge = new Edge(lastInode.getId(), childName);
    if (!mEntries.isEmpty()) {
      Preconditions.checkState(endsInInode(),
          "Cannot lock edge %s when lock list %s already ends in an edge", edge, this);
      Preconditions.checkState(lastInode().getId() == edgeParentId,
          "Cannot lock edge %s when the last inode id in %s is %s", edge, this, lastInode.getId());
    }
    addEntry(createEdgeEntry(edge, mode));
  }

  @Override
  public void lockRootEdge(LockMode mode) {
    Preconditions.checkState(mEntries.isEmpty(),
        "Cannot lock root edge when lock list %s is nonempty", this);
    addEntry(createEdgeEntry(ROOT_EDGE, mode));
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
      EdgeEntry lastEdgeEntry = createEdgeEntry(lastEdge(), LockMode.READ);
      InodeEntry inodeEntry = createInodeEntry(inode, LockMode.READ);
      EdgeEntry nextEdgeEntry = createEdgeEntry(edge, LockMode.WRITE);
      removeLastEntry(); // Remove edge write lock
      addEntry(lastEdgeEntry);
      addEntry(inodeEntry);
      addEntry(nextEdgeEntry);
    }
  }

  @Override
  public void unlockLastInode() {
    Preconditions.checkState(endsInInode(),
        "Cannot unlock last inode when the lock list %s ends in an edge", this);
    Preconditions.checkState(!mEntries.isEmpty(),
        "Cannot unlock last inode when the lock list is empty");
    removeLastEntry();
  }

  @Override
  public void unlockLastEdge() {
    Preconditions.checkState(!endsInInode(),
        "Cannot unlock last edge when the lock list %s ends in an inode", this);
    Preconditions.checkState(!mEntries.isEmpty(),
        "Cannot unlock last edge when the lock list is empty");
    removeLastEntry();
  }

  @Override
  public void downgradeLastInode() {
    Preconditions.checkState(endsInInode(),
        "Cannot downgrade last inode when lock list %s ends in an edge", this);
    Preconditions.checkState(!mEntries.isEmpty(),
        "Cannot downgrade last inode when the lock list is empty");
    Preconditions.checkState(endsInWriteLock(),
        "Cannot downgrade last inode when lock list %s is not write locked", this);

    if (!endsInMultipleWriteLocks()) {
      InodeEntry newEntry = createInodeEntry(lastInode(), LockMode.READ);
      removeLastEntry();
      addEntry(newEntry);
    }
  }

  @Override
  public void downgradeLastEdge() {
    Preconditions.checkState(!endsInInode(),
        "Cannot downgrade last edge when lock list %s ends in an inode", this);
    Preconditions.checkState(!mEntries.isEmpty(),
        "Cannot downgrade last edge when the lock list is empty");
    Preconditions.checkState(endsInWriteLock(),
        "Cannot downgrade last edge when lock list %s is not write locked", this);

    if (!endsInMultipleWriteLocks()) {
      EdgeEntry newEntry = createEdgeEntry(lastEdge(), LockMode.READ);
      removeLastEntry();
      addEntry(newEntry);
    }
  }

  @Override
  public void downgradeEdgeToInode(Inode inode, LockMode mode) {
    Preconditions.checkState(!endsInInode(),
        "Cannot downgrade from an edge write lock to an inode lock when lock list %s "
            + "already ends in an inode",
        this);
    Preconditions.checkState(!mEntries.isEmpty(),
        "Cannot downgrade from an edge write lock to an inode lock when the lock list is empty");
    Preconditions.checkState(endsInWriteLock(),
        "Cannot downgrade from an edge write lock to an inode lock when lock list %s "
            + "is not write locked",
        this);

    if (endsInMultipleWriteLocks()) {
      lockInode(inode, LockMode.WRITE);
      return;
    }

    EdgeEntry edgeEntry = createEdgeEntry(lastEdge(), LockMode.READ);
    InodeEntry inodeEntry = createInodeEntry(inode, mode);
    removeLastEntry();
    addEntry(edgeEntry);
    addEntry(inodeEntry);
  }

  /**
   * Locks an edge and creates and entry without adding the entry to the lock list yet.
   *
   * @return the edge entry
   */
  private EdgeEntry createEdgeEntry(Edge edge, LockMode mode) {
    LockResource lock = mInodeLockManager.lockEdge(edge, mode);
    return new EdgeEntry(lock, edge, mode);
  }

  /**
   * Locks an inode and creates and entry without adding the entry to the lock list yet.
   *
   * @return the inode entry
   */
  private InodeEntry createInodeEntry(Inode inode, LockMode mode) {
    LockResource lock = mInodeLockManager.lockInode(inode, mode);
    return new InodeEntry(lock, inode, mode);
  }

  /**
   * If the lock list is write locked, returns LockMode.WRITE. Otherwise, returns the given mode.
   *
   * This helps us preserve the invariant that there is never a READ lock following a WRITE lock.
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

  /**
   * Adds an entry to mEntries.
   *
   * @param entry the entry to add
   */
  private void addEntry(Entry entry) {
    if (!endsInWriteLock() && entry.getMode() == LockMode.READ) {
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
    Preconditions.checkState(endsInInode(),
        "Cannot get last inode for lock list %s which does not end in an inode", this);
    return ((InodeEntry) lastEntry()).getInode();
  }

  /**
   * @return the last edge
   */
  private Edge lastEdge() {
    Preconditions.checkState(!endsInInode(),
        "Cannot get last edge for lock list %s which does not end in an edge", this);
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
    return String.format("Path: <%s>, Entries: %s, First write-locked index: %s", path, mEntries,
        mFirstWriteLockIndex);
  }

  @Override
  public void close() {
    mEntries.forEach(entry -> entry.getLock().close());
    mEntries.clear();
  }

  private abstract static class Entry {
    private final LockResource mLock;
    private final LockMode mMode;

    protected Entry(LockResource lock, LockMode mode) {
      mLock = lock;
      mMode = mode;
    }

    public LockResource getLock() {
      return mLock;
    }

    public LockMode getMode() {
      return mMode;
    }
  }

  private static class InodeEntry extends Entry {
    private final Inode mInode;

    public InodeEntry(LockResource lock, Inode inode, LockMode mode) {
      super(lock, mode);
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

    public EdgeEntry(LockResource lock, Edge edge, LockMode mode) {
      super(lock, mode);
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
