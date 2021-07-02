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

import alluxio.AlluxioURI;
import alluxio.concurrent.LockMode;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.metastore.ReadOnlyInodeStore;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.Lock;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class represents a locked path within the inode tree, starting from the root.
 *
 * The mUri and mPathComponents fields are immutable and contain the "target" path represented by
 * the LockedInodePath.
 *
 * mLockList manages the locks held by the LockedInodePath.
 *
 * The mExistingInodes list holds inodes that are locked and known to exist. The inodes might not be
 * individually locked - if one inode is write-locked, the inodes after it are implicitly
 * write-locked and may be added to mExistingInodes. The inodes in mExistingInodes are a prefix of
 * the inodes referenced by mPathComponents.
 *
 * If the full path exists, mExistingInodes will have size equal to mPathComponents.length after
 * traversal. mExistingInodes is always at least as long as the list of inodes directly locked by
 * mLockList.
 *
 * To create new inode paths from an existing path, use one of the lock*() methods. They return new
 * locked inode paths that can be modified and closed without affecting the original path. Modifying
 * the original inode path invalidates any subpaths that it has created.
 *
 * Locked inode paths are not threadsafe and should not be shared across threads.
 */
@NotThreadSafe
public class LockedInodePath implements Closeable {
  /**
   * The root inode of the inode tree. This is needed to bootstrap the inode path.
   */
  private final Inode mRoot;
  /** Inode store for looking up children. */
  private final ReadOnlyInodeStore mInodeStore;

  /** Uri for the path represented. */
  protected final AlluxioURI mUri;
  /** The components of mUri. */
  protected final String[] mPathComponents;
  /** Lock list locking some portion of the path according to mLockPattern. */
  protected final InodeLockList mLockList;
  /** The locking pattern. */
  protected LockPattern mLockPattern;
  /** Whether to use {@link Lock#tryLock()} or {@link Lock#lock()}. */
  private final boolean mUseTryLock;

  /**
   * Creates a new locked inode path.
   *
   * @param uri the uri for the path
   * @param inodeStore the inode store for looking up inode children
   * @param inodeLockManager the inode lock manager
   * @param root the root inode
   * @param lockPattern the pattern to lock in
   * @param tryLock whether or not use {@link Lock#tryLock()} or {@link Lock#lock()}
   */
  public LockedInodePath(AlluxioURI uri, ReadOnlyInodeStore inodeStore,
      InodeLockManager inodeLockManager, InodeDirectory root, LockPattern lockPattern,
      boolean tryLock)
      throws InvalidPathException {
    mUri = uri;
    mPathComponents = PathUtils.getPathComponents(uri.getPath());
    mInodeStore = inodeStore;
    mLockPattern = lockPattern;
    mRoot = root;
    mUseTryLock = tryLock;
    mLockList = new SimpleInodeLockList(inodeLockManager, mUseTryLock);
  }

  /**
   * Creates a new locked inode path, using a prefix locked inode path as a starting point.
   *
   * @param uri the uri for the new path
   * @param path the path to use as a starting point
   * @param pathComponents components of the uri
   * @param lockPattern the pattern to lock in
   */
  private LockedInodePath(AlluxioURI uri, LockedInodePath path, String[] pathComponents,
      LockPattern lockPattern, boolean tryLock) {
    Preconditions.checkState(!path.mLockList.isEmpty());
    mUri = uri;
    mPathComponents = pathComponents;
    mInodeStore = path.mInodeStore;
    mLockList = new CompositeInodeLockList(path.mLockList, tryLock);
    mLockPattern = lockPattern;
    mRoot = path.mLockList.get(0);
    mUseTryLock = tryLock;
  }

  /**
   * @return the full uri of the path
   */
  public AlluxioURI getUri() {
    return mUri;
  }

  /**
   * @return the target inode
   * @throws FileDoesNotExistException if the target inode does not exist
   */
  public Inode getInode() throws FileDoesNotExistException {
    Inode inode = getInodeOrNull();
    if (inode == null) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(mUri));
    }
    return inode;
  }

  /**
   * @return the target inode, or null if it does not exist
   */
  @Nullable
  public Inode getInodeOrNull() {
    if (!fullPathExists()) {
      return null;
    }
    return mLockList.get(mLockList.numInodes() - 1);
  }

  /**
   * @return the target inode as an {@link MutableInodeFile}
   * @throws FileDoesNotExistException if the target inode does not exist, or it is not a file
   */
  public InodeFile getInodeFile() throws FileDoesNotExistException {
    Inode inode = getInode();
    if (!inode.isFile()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(mUri));
    }
    return inode.asFile();
  }

  /**
   * @return the parent of the target inode
   * @throws InvalidPathException if the parent inode is not a directory
   * @throws FileDoesNotExistException if the parent of the target does not exist
   */
  public InodeDirectory getParentInodeDirectory()
      throws InvalidPathException, FileDoesNotExistException {
    Inode inode = getParentInodeOrNull();
    if (inode == null) {
      throw new FileDoesNotExistException(
          ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(mUri.getParent()));
    }
    if (!inode.isDirectory()) {
      throw new InvalidPathException(
          ExceptionMessage.PATH_MUST_HAVE_VALID_PARENT.getMessage(mUri));
    }
    return (InodeDirectory) inode;
  }

  /**
   * @return the parent of the target inode, or null if the parent does not exist
   */
  @Nullable
  public Inode getParentInodeOrNull() {
    if (mPathComponents.length < 2 || mLockList.numInodes() < (mPathComponents.length - 1)) {
      // The path is only the root, or the list of inodes is not long enough to contain the parent
      return null;
    }
    return mLockList.get(mPathComponents.length - 2);
  }

  /**
   * @return the last existing inode on the inode path. This could be out of date if the current
   *         thread has added or deleted inodes since the last call to traverse()
   */
  public Inode getLastExistingInode() {
    return mLockList.get(mLockList.numInodes() - 1);
  }

  /**
   * @return a copy of the list of existing inodes, from the root
   */
  public List<Inode> getInodeList() {
    return mLockList.getLockedInodes();
  }

  /**
   * @return a copy of the list of existing inodes, from the root
   */
  public List<InodeView> getInodeViewList() {
    return mLockList.getLockedInodeViews();
  }

  /**
   * @return the number of existing inodes in this path. This could be out of date if the current
   *         thread has added or deleted inodes since the last call to traverse()
   */
  public int getExistingInodeCount() {
    return mLockList.numInodes();
  }

  /**
   * @return number of components in this locked path
   */
  public int size() {
    return mPathComponents.length;
  }

  /**
   * @return true if the entire path of inodes exists, false otherwise. This could be out of date if
   *         the current thread has added or deleted inodes since the last call to traverse()
   */
  public boolean fullPathExists() {
    return mLockList.numInodes() == mPathComponents.length;
  }

  /**
   * @return the {@link LockPattern} of this path
   */
  public LockPattern getLockPattern() {
    return mLockPattern;
  }

  /**
   * Removes the last inode from the list. This is necessary when the last inode is deleted and we
   * want to continue using the inodepath. This operation is only supported when the path is
   * complete.
   */
  public void removeLastInode() {
    Preconditions.checkState(fullPathExists());

    mLockList.unlockLastInode();
  }

  /**
   * Adds the next inode to the path. This tries to reduce the scope of locking by moving the write
   * lock forward to the new final edge, downgrading the previous write lock to a read lock.
   *
   * @param inode the inode to add
   */
  public void addNextInode(Inode inode) {
    Preconditions.checkState(mLockPattern == LockPattern.WRITE_EDGE);
    Preconditions.checkState(!fullPathExists());
    Preconditions.checkState(inode.getName().equals(mPathComponents[mLockList.numInodes()]));

    int nextInodeIndex = mLockList.numInodes() + 1;
    if (nextInodeIndex < mPathComponents.length) {
      mLockList.pushWriteLockedEdge(inode, mPathComponents[nextInodeIndex]);
    } else {
      mLockList.lockInode(inode, LockMode.WRITE);
    }
  }

  /**
   * Downgrades all locks in this list to read locks.
   */
  public void downgradeToRead() {
    mLockList.downgradeToReadLocks();
    mLockPattern = LockPattern.READ;
  }

  /**
   * Returns the closest ancestor of the target inode (last inode in the full path).
   *
   * @return the closest ancestor inode
   * @throws FileDoesNotExistException if an ancestor does not exist
   */
  public Inode getAncestorInode() throws FileDoesNotExistException {
    int ancestorIndex = mPathComponents.length - 2;
    if (ancestorIndex < 0) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(mUri));
    }
    ancestorIndex = Math.min(ancestorIndex, mLockList.numInodes() - 1);
    return mLockList.get(ancestorIndex);
  }

  /**
   * Locks a descendant of the current path and returns a new locked inode path. The path is
   * traversed according to the lock pattern. Closing the new path will have no effect on the
   * current path.
   *
   * On failure, all locks taken by this method will be released.
   *
   * @param descendantUri the full descendent uri starting from the root
   * @param lockPattern the lock pattern to lock in
   * @return the new locked path
   */
  public LockedInodePath lockDescendant(AlluxioURI descendantUri, LockPattern lockPattern)
      throws InvalidPathException {
    LockedInodePath path = new LockedInodePath(descendantUri, this,
        PathUtils.getPathComponents(descendantUri.getPath()), lockPattern, mUseTryLock);
    path.traverseOrClose();
    return path;
  }

  /**
   * Returns a new locked inode path composed of the current path plus the child inode. The path is
   * traversed according to the lock pattern. The original locked inode path is unaffected.
   *
   * childComponentsHint can be used to save the work of computing path components when the path
   * components for the new path are already known.
   *
   * On failure, all locks taken by this method will be released.
   *
   * @param child the child inode
   * @param lockPattern the lock pattern
   * @return the new locked path
   */
  public LockedInodePath lockChild(Inode child, LockPattern lockPattern)
      throws InvalidPathException {
    return lockChild(child, lockPattern, addComponent(mPathComponents, child.getName()));
  }

  /**
   * Efficient version of {@link #lockChild(Inode, LockPattern)} for when the child path
   * components are already known.
   *
   * @param child the child inode
   * @param lockPattern the lock pattern
   * @param childComponentsHint path components for the new path
   * @return the new locked path
   */
  public LockedInodePath lockChild(Inode child, LockPattern lockPattern,
      String[] childComponentsHint) throws InvalidPathException {
    LockedInodePath path = new LockedInodePath(mUri.joinUnsafe(child.getName()), this,
        childComponentsHint, lockPattern, mUseTryLock);
    path.traverseOrClose();
    return path;
  }

  private static String[] addComponent(String[] components, String component) {
    String[] newComponents = new String[components.length + 1];
    System.arraycopy(components, 0, newComponents, 0, components.length);
    newComponents[components.length] = component;
    return newComponents;
  }

  /**
   * Returns a copy of the path with the final edge write locked. This requires that we haven't
   * already locked the final edge, i.e. the path is incomplete.
   *
   * @return the new locked path
   */
  public LockedInodePath lockFinalEdgeWrite() throws InvalidPathException {
    Preconditions.checkState(!fullPathExists());

    LockedInodePath newPath =
        new LockedInodePath(mUri, this, mPathComponents, LockPattern.WRITE_EDGE, mUseTryLock);
    newPath.traverse();
    return newPath;
  }

  private void traverseOrClose() throws InvalidPathException {
    try {
      traverse();
    } catch (Throwable t) {
      close();
      throw t;
    }
  }

  /**
   * Traverses the inode path according to its lock pattern. If the inode path is already partially
   * traversed, this method will pick up where the previous traversal left off.
   *
   * On return, all existing inodes in the path are added to mExistingInodes and the inodes are
   * locked according to {@link LockPattern}.
   */
  public void traverse() throws InvalidPathException {
    // This locks the root edge and inode.
    bootstrapTraversal();

    // Each iteration either locks a new inode/edge or hits a missing inode and returns.
    while (!fullPathExists()) {
      int lastInodeIndex = mLockList.numInodes() - 1;
      String nextComponent = mPathComponents[lastInodeIndex + 1];
      boolean isFinalComponent = lastInodeIndex == mPathComponents.length - 2;

      Inode lastInode = mLockList.get(lastInodeIndex);
      if (mLockList.endsInInode()) { // Lock an edge next.
        if (mLockPattern == LockPattern.WRITE_EDGE && isFinalComponent) {
          mLockList.lockEdge(lastInode, nextComponent, LockMode.WRITE);
        } else {
          mLockList.lockEdge(lastInode, nextComponent, LockMode.READ);
        }
      } else { // Lock an inode next.
        if (!lastInode.isDirectory()) {
          throw new InvalidPathException(String.format(
              "Traversal failed for path %s. Component %s(%s) is a file, not a directory.", mUri,
              lastInodeIndex, lastInode.getName()));
        }
        Optional<Inode> nextInodeOpt =
            mInodeStore.getChild(lastInode.asDirectory(), nextComponent);
        if (!nextInodeOpt.isPresent() && mLockPattern == LockPattern.WRITE_EDGE
            && !isFinalComponent) {
          // This pattern requires that we obtain a write lock on the final edge, so we must
          // upgrade to a write lock.
          mLockList.unlockLastEdge();
          mLockList.lockEdge(lastInode, nextComponent, LockMode.WRITE);
          nextInodeOpt = mInodeStore.getChild(lastInode.asDirectory(), nextComponent);
          if (nextInodeOpt.isPresent()) {
            // The component must have been created between releasing the read lock and acquiring
            // the write lock. Downgrade and continue as normal.
            mLockList.downgradeLastEdge();
          }
        }
        if (!nextInodeOpt.isPresent()) {
          if (mLockPattern == LockPattern.READ) {
            // WRITE_INODE and WRITE_EDGE should lock the last non-existing edge.
            mLockList.unlockLastEdge();
          }
          return;
        }
        Inode nextInode = nextInodeOpt.get();

        if (isFinalComponent && mLockPattern.isWrite()) {
          mLockList.lockInode(nextInode, LockMode.WRITE);
        } else {
          mLockList.lockInode(nextInode, LockMode.READ);
        }
      }
    }
  }

  private void bootstrapTraversal() {
    if (!mLockList.isEmpty()) {
      return;
    }
    LockMode edgeLock = LockMode.READ;
    LockMode inodeLock = LockMode.READ;
    if (mPathComponents.length == 1) {
      if (mLockPattern == LockPattern.WRITE_EDGE) {
        edgeLock = LockMode.WRITE;
        inodeLock = LockMode.WRITE;
      } else if (mLockPattern == LockPattern.WRITE_INODE) {
        inodeLock = LockMode.WRITE;
      }
    }
    mLockList.lockRootEdge(edgeLock);
    mLockList.lockInode(mRoot, inodeLock);
  }

  @Override
  public void close() {
    mLockList.close();
  }

  @Override
  public String toString() {
    return mUri.toString();
  }
}
