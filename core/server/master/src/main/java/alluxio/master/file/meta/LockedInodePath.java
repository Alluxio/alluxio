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
import alluxio.util.io.PathUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a path of locked {@link Inode}, starting from the root.
 */
@ThreadSafe
public abstract class LockedInodePath implements AutoCloseable {
  protected final AlluxioURI mUri;
  protected final String[] mPathComponents;
  protected final ArrayList<Inode<?>> mInodes;
  protected final InodeLockList mLockList;
  protected final InodeTree.LockMode mLockMode;

  LockedInodePath(AlluxioURI uri, List<Inode<?>> inodes, InodeLockList lockList,
      InodeTree.LockMode lockMode)
      throws InvalidPathException {
    Preconditions.checkArgument(!inodes.isEmpty());
    mUri = uri;
    mPathComponents = PathUtils.getPathComponents(mUri.getPath());
    mInodes = new ArrayList<>(inodes);
    mLockList = lockList;
    mLockMode = lockMode;
  }

  LockedInodePath(LockedInodePath inodePath) {
    Preconditions.checkArgument(!inodePath.mInodes.isEmpty());
    mUri = inodePath.mUri;
    mPathComponents = inodePath.mPathComponents;
    mInodes = inodePath.mInodes;
    mLockList = inodePath.mLockList;
    mLockMode = inodePath.mLockMode;
  }

  /**
   * @return the full uri of the path
   */
  public synchronized AlluxioURI getUri() {
    return mUri;
  }

  /**
   * @return the target inode
   * @throws FileDoesNotExistException if the target inode does not exist
   */
  public synchronized Inode<?> getInode() throws FileDoesNotExistException {
    if (!fullPathExists()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(mUri));
    }
    return mInodes.get(mInodes.size() - 1);
  }

  /**
   * @return the target inode as an {@link InodeFile}
   * @throws FileDoesNotExistException if the target inode does not exist, or it is not a file
   */
  public synchronized  InodeFile getInodeFile() throws FileDoesNotExistException {
    Inode<?> inode = getInode();
    if (!inode.isFile()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(mUri));
    }
    return (InodeFile) inode;
  }

  /**
   * @return the parent of the target inode
   * @throws InvalidPathException if the parent inode is not a directory
   * @throws FileDoesNotExistException if the parent of the target does not exist
   */
  public synchronized  InodeDirectory getParentInodeDirectory()
      throws InvalidPathException, FileDoesNotExistException {
    if (mPathComponents.length < 2 || mInodes.size() < (mPathComponents.length - 1)) {
      // The path is only the root, or the list of inodes is not long enough to contain the parent
      throw new FileDoesNotExistException(
          ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(mUri.getParent()));
    }
    Inode<?> inode = mInodes.get(mPathComponents.length - 2);
    if (!inode.isDirectory()) {
      throw new InvalidPathException(ExceptionMessage.PATH_MUST_HAVE_VALID_PARENT.getMessage(mUri));
    }
    return (InodeDirectory) inode;
  }

  /**
   * @return a copy of the list of existing inodes, from the root
   */
  public synchronized List<Inode<?>> getInodeList() {
    return Lists.newArrayList(mInodes);
  }

  /**
   * @return true if the entire path of inodes exists, false otherwise
   */
  public synchronized boolean fullPathExists() {
    return mInodes.size() == mPathComponents.length;
  }

  /**
   * @return the {@link InodeTree.LockMode} of this path
   */
  public synchronized InodeTree.LockMode getLockMode() {
    return mLockMode;
  }

  @Override
  public synchronized void close() {
    mLockList.close();
  }
}
