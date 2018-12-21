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
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.meta.InodeTree.LockMode;
import alluxio.master.file.meta.InodeTree.LockPattern;
import alluxio.master.metastore.ReadOnlyInodeStore;
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
   * The root inode of the inode tree. This is needed to bootstrap the inode path. After traverse(),
   * this will always be the first inode in mExistingInodes.
   */
  private final InodeView mRoot;
  /** Inode store for looking up children. */
  private final ReadOnlyInodeStore mInodeStore;

  /** Uri for the path represented. */
  protected final AlluxioURI mUri;
  /** The components of mUri. */
  protected final String[] mPathComponents;
  /**
   * The inodes of mUri which existed at the time of the last traversal. These inodes are all locked
   * either explicitly or implicitly by the current thread. An inode is locked implicitly if one of
   * its ancestor inodes or edges is write-locked.
   *
   * mLockList.getLockedInodes() is always an inclusive prefix of this list.
   */
  protected final List<InodeView> mExistingInodes;
  /** Lock list locking some portion of the path according to mLockPattern. */
  protected final InodeLockList mLockList;
  /** The locking pattern. */
  protected LockPattern mLockPattern;

  /**
   * Creates a new locked inode path.
   *
   * @param uri the uri for the path
   * @param inodeStore the inode store for looking up inode children
   * @param inodeLockManager the inode lock manager
   * @param root the root inode
   * @param lockPattern the pattern to lock in
   */
  public LockedInodePath(AlluxioURI uri, ReadOnlyInodeStore inodeStore,
      InodeLockManager inodeLockManager, InodeDirectoryView root, LockPattern lockPattern)
      throws InvalidPathException {
    mUri = uri;
    mPathComponents = PathUtils.getPathComponents(uri.getPath());
    mInodeStore = inodeStore;
    mExistingInodes = new ArrayList<>();
    mLockList = new InodeLockList(inodeLockManager);
    mLockPattern = lockPattern;
    mRoot = root;
  }

  private LockedInodePath(AlluxioURI uri, LockedInodePath path, String[] pathComponents,
      LockPattern lockPattern) {
    Preconditions.checkState(!path.mExistingInodes.isEmpty());
    Preconditions.checkState(!path.mLockList.isEmpty());
    mUri = uri;
    mPathComponents = pathComponents;
    mInodeStore = path.mInodeStore;
    mExistingInodes = new ArrayList<>(path.mExistingInodes);
    mLockList = new CompositeInodeLockList(path.mLockList);
    mLockPattern = lockPattern;
    mRoot = mExistingInodes.get(0);
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
  public InodeView getInode() throws FileDoesNotExistException {
    InodeView inode = getInodeOrNull();
    if (inode == null) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(mUri));
    }
    return inode;
  }

  /**
   * @return the target inode, or null if it does not exist
   */
  @Nullable
  public InodeView getInodeOrNull() {
    if (!fullPathExists()) {
      return null;
    }
    return mExistingInodes.get(mExistingInodes.size() - 1);
  }

  /**
   * @return the target inode as an {@link InodeFile}
   * @throws FileDoesNotExistException if the target inode does not exist, or it is not a file
   */
  public InodeFileView getInodeFile() throws FileDoesNotExistException {
    InodeView inode = getInode();
    if (!inode.isFile()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(mUri));
    }
    return (InodeFileView) inode;
  }

  /**
   * @return the parent of the target inode
   * @throws InvalidPathException if the parent inode is not a directory
   * @throws FileDoesNotExistException if the parent of the target does not exist
   */
  public InodeDirectoryView getParentInodeDirectory()
      throws InvalidPathException, FileDoesNotExistException {
    InodeView inode = getParentInodeOrNull();
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
  public InodeView getParentInodeOrNull() {
    if (mPathComponents.length < 2 || mExistingInodes.size() < (mPathComponents.length - 1)) {
      // The path is only the root, or the list of inodes is not long enough to contain the parent
      return null;
    }
    return mExistingInodes.get(mPathComponents.length - 2);
  }

  /**
   * @return the last existing inode on the inode path. This could be out of date if the current
   *         thread has added or deleted inodes since the last call to traverse()
   */
  public InodeView getLastExistingInode() {
    return mExistingInodes.get(mExistingInodes.size() - 1);
  }

  /**
   * @return a copy of the list of existing inodes, from the root
   */
  public List<InodeView> getInodeList() {
    return new ArrayList<>(mExistingInodes);
  }

  /**
   * @return the number of existing inodes in this path. This could be out of date if the current
   *         thread has added or deleted inodes since the last call to traverse()
   */
  public int getExistingInodeCount() {
    return mExistingInodes.size();
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
    return mExistingInodes.size() == mPathComponents.length;
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

    if (!isImplicitlyLocked()) {
      // WRITE_EDGE pattern always implicitly locks the list when the full path exists, so the lock
      // list must end with an inode.
      mLockList.unlockLastInode();
      mLockList.unlockLastEdge();
    }
    mExistingInodes.remove(mExistingInodes.size() - 1);
  }

  /**
   * Adds the next inode to the path. This tries to reduce the scope of locking by moving the write
   * lock forward to the new final edge, downgrading the previous write lock to a read lock. If the
   * path is implicitly locked, the inode is added but no downgrade occurs.
   *
   * @param inode the inode to add
   */
  public void addNextInode(InodeView inode) {
    Preconditions.checkState(mLockPattern == LockPattern.WRITE_EDGE);
    Preconditions.checkState(!fullPathExists());
    Preconditions.checkState(inode.getName().equals(mPathComponents[mExistingInodes.size()]));

    if (!isImplicitlyLocked() && mExistingInodes.size() < mPathComponents.length - 1) {
      mLockList.pushWriteLockedEdge(inode, mPathComponents[mExistingInodes.size() + 1]);
    }
    mExistingInodes.add(inode);
  }

  /**
   * Downgrades from the current locking scheme to the desired locking scheme.
   *
   * @param desiredLockPattern the pattern to downgrade to
   */
  public void downgradeToPattern(LockPattern desiredLockPattern) {
    switch (desiredLockPattern) {
      case READ:
        if (mLockPattern == LockPattern.WRITE_INODE) {
          Preconditions.checkState(!isImplicitlyLocked());
          mLockList.downgradeLastInode();
        } else if (mLockPattern == LockPattern.WRITE_EDGE) {
          downgradeEdgeToInode(LockMode.READ);
        }
        break;
      case WRITE_INODE:
        if (mLockPattern == LockPattern.WRITE_EDGE) {
          downgradeEdgeToInode(LockMode.WRITE);
        } else {
          Preconditions.checkState(mLockPattern == LockPattern.WRITE_INODE);
        }
        break;
      case WRITE_EDGE:
        Preconditions.checkState(mLockPattern == LockPattern.WRITE_EDGE);
        break; // Nothing to do
      default:
        throw new IllegalStateException("Unknown lock pattern: " + desiredLockPattern);
    }
    mLockPattern = desiredLockPattern;
  }

  private void downgradeEdgeToInode(LockMode lockMode) {
    if (fullPathExists()) {
      Preconditions.checkState(mLockList.numLockedInodes() == mExistingInodes.size() - 1);
      mLockList.downgradeEdgeToInode(Iterables.getLast(mExistingInodes), lockMode);
      if (lockMode == LockMode.READ) {
        // Now that we've downgraded from write to read, we can only rely on directly locked inodes
        // to still exist.
        while (mExistingInodes.size() > mLockList.numLockedInodes()) {
          mExistingInodes.remove(mExistingInodes.size() - 1);
        }
      }
    } else {
      Preconditions.checkState(mLockList.numLockedInodes() == mExistingInodes.size());
      mLockList.unlockLastEdge();
    }
  }

  /**
   * Returns the closest ancestor of the target inode (last inode in the full path).
   *
   * @return the closest ancestor inode
   * @throws FileDoesNotExistException if an ancestor does not exist
   */
  public InodeView getAncestorInode() throws FileDoesNotExistException {
    int ancestorIndex = mPathComponents.length - 2;
    if (ancestorIndex < 0) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(mUri));
    }
    ancestorIndex = Math.min(ancestorIndex, mExistingInodes.size() - 1);
    return mExistingInodes.get(ancestorIndex);
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
        PathUtils.getPathComponents(descendantUri.getPath()), lockPattern);
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
  public LockedInodePath lockChild(InodeView child, LockPattern lockPattern)
      throws InvalidPathException {
    return lockChild(child, lockPattern, addComponent(mPathComponents, child.getName()));
  }

  /**
   * Efficient version of {@link #lockChild(InodeView, LockPattern)} for when the child path
   * components are already known.
   *
   * @param child the child inode
   * @param lockPattern the lock pattern
   * @param childComponentsHint path components for the new path
   * @return the new locked path
   */
  public LockedInodePath lockChild(InodeView child, LockPattern lockPattern,
      String[] childComponentsHint) throws InvalidPathException {
    LockedInodePath path =
        new LockedInodePath(mUri.join(child.getName()), this, childComponentsHint, lockPattern);
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
        new LockedInodePath(mUri, this, mPathComponents, LockPattern.WRITE_EDGE);
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
   * traversed, this method will pick up where the previous traversal left off. If the path already
   * ends in a write lock, traverse will populate the inodes list without taking any additional
   * locks.
   *
   * On return, all existing inodes in the path are added to mExistingInodes and the inodes are
   * locked according to to {@link LockPattern}.
   */
  public void traverse() throws InvalidPathException {
    if (mLockList.getLockMode() == LockMode.WRITE) {
      traverseWithoutLocking();
      return;
    }

    // This locks the root edge and inode if necessary.
    bootstrapTraversal();

    // Each iteration either locks a new inode/edge or hits a missing inode and returns.
    while (!fullPathExists()) {
      int lastInodeIndex = mLockList.getLockedInodes().size() - 1;
      String nextComponent = mPathComponents[lastInodeIndex + 1];
      boolean isFinalComponent = lastInodeIndex == mPathComponents.length - 2;

      if (mLockList.endsInInode()) { // Lock an edge next.
        if (mLockPattern == LockPattern.WRITE_EDGE && isFinalComponent) {
          mLockList.lockEdge(nextComponent, LockMode.WRITE);
        } else {
          mLockList.lockEdge(nextComponent, LockMode.READ);
        }
      } else { // Lock an inode next.
        InodeView lastInode = mLockList.getLockedInodes().get(lastInodeIndex);
        if (!lastInode.isDirectory()) {
          throw new InvalidPathException(String.format(
              "Traversal failed for path %s. Component %s(%s) is a file, not a directory.", mUri,
              lastInodeIndex, lastInode.getName()));
        }
        Optional<InodeView> nextInodeOpt =
            mInodeStore.getChild(lastInode.asDirectory(), nextComponent);
        if (!nextInodeOpt.isPresent() && mLockPattern == LockPattern.WRITE_EDGE
            && !isFinalComponent) {
          // This pattern requires that we obtain a write lock on the final edge, so we must
          // upgrade to a write lock.
          mLockList.unlockLastEdge();
          mLockList.lockEdge(nextComponent, LockMode.WRITE);
          nextInodeOpt = mInodeStore.getChild(lastInode.asDirectory(), nextComponent);
          if (nextInodeOpt.isPresent()) {
            // The component must have been created between releasing the read lock and acquiring
            // the write lock. Downgrade and continue as normal.
            mLockList.downgradeLastEdge();
          }
        }
        if (!nextInodeOpt.isPresent()) {
          if (mLockPattern != LockPattern.WRITE_EDGE) {
            // Other lock patterns only lock up to the last existing inode.
            mLockList.unlockLastEdge();
          }
          return;
        }
        InodeView nextInode = nextInodeOpt.get();

        if (isFinalComponent) {
          if (mLockPattern == LockPattern.READ) {
            mLockList.lockInode(nextInode, LockMode.READ);
          } else if (mLockPattern == LockPattern.WRITE_INODE) {
            mLockList.lockInode(nextInode, LockMode.WRITE);
          } else if (mLockPattern == LockPattern.WRITE_EDGE) {
            if (mLockList.numLockedInodes() == mExistingInodes.size()) {
              // Add the final inode, which is not locked but does exist.
              mExistingInodes.add(nextInode);
            }
          }
        } else {
          mLockList.lockInode(nextInode, LockMode.READ);
        }

        // Avoid adding the inode if it is already in mExistingInodes.
        if (mLockList.numLockedInodes() > mExistingInodes.size()) {
          mExistingInodes.add(nextInode);
        }
      }
    }
  }

  private void bootstrapTraversal() {
    boolean lockFirstEdgeOnly =
        mPathComponents.length == 1 && mLockPattern == LockPattern.WRITE_EDGE;
    if (mLockList.isEmpty()) {
      if (lockFirstEdgeOnly) {
        mLockList.lockRootEdge(LockMode.WRITE);
      } else {
        mLockList.lockRootEdge(LockMode.READ);
      }
    }
    if (mLockList.numLockedInodes() == 0) {
      if (mPathComponents.length == 1 && mLockPattern == LockPattern.WRITE_INODE) {
        mLockList.lockInode(mRoot, LockMode.WRITE);
      } else if (!lockFirstEdgeOnly) {
        mLockList.lockInode(mRoot, LockMode.READ);
      }
    }
    if (mExistingInodes.isEmpty()) {
      mExistingInodes.add(mRoot);
    }
  }

  private void traverseWithoutLocking() throws InvalidPathException {
    if (mExistingInodes.isEmpty()) {
      mExistingInodes.add(mRoot);
    }

    for (int i = mExistingInodes.size(); i < mPathComponents.length; i++) {
      InodeView lastInode = mExistingInodes.get(i - 1);
      if (lastInode.isFile()) {
        throw new InvalidPathException(String.format(
            "Traversal failed for path %s. Component %s(%s) is a file, not a directory.", mUri,
            i - 1, lastInode.getName()));
      }
      Optional<InodeView> nextInode =
          mInodeStore.getChild(lastInode.asDirectory(), mPathComponents[i]);
      if (!nextInode.isPresent()) {
        return;
      }
      mExistingInodes.add(nextInode.get());
    }
  }

  /**
   * Returns whether the path is implicitly locked.
   *
   * A path is implicitly locked if it ends with an inode that is not locked by the lock list. This
   * can only happen when one of the inodes or edges leading to that inode is write-locked.
   *
   * @return whether the path is implicitly locked
   */
  private boolean isImplicitlyLocked() {
    return mLockList.getLockMode() == LockMode.WRITE
        && mExistingInodes.size() > mLockList.numLockedInodes();
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
