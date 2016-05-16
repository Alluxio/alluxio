/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
public abstract class InodePath implements AutoCloseable {
  protected final AlluxioURI mUri;
  protected final String[] mPathComponents;
  protected final ArrayList<Inode<?>> mInodes;
  protected final InodeLockGroup mLockGroup;

  InodePath(AlluxioURI uri, List<Inode<?>> inodes, InodeLockGroup lockGroup)
      throws InvalidPathException {
    Preconditions.checkArgument(!inodes.isEmpty());
    mUri = uri;
    mPathComponents = PathUtils.getPathComponents(mUri.getPath());
    mInodes = new ArrayList<>(inodes);
    mLockGroup = lockGroup;
  }

  InodePath(InodePath inodePath) {
    Preconditions.checkArgument(!inodePath.mInodes.isEmpty());
    mUri = inodePath.mUri;
    mPathComponents = inodePath.mPathComponents;
    mInodes = inodePath.mInodes;
    mLockGroup = inodePath.mLockGroup;
  }

  /**
   * @return the full uri of the path
   */
  public synchronized AlluxioURI getUri() {
    return mUri;
  }

  /**
   * Returns the target inode. If the target inode does not exist, an exception will be thrown.
   *
   * @return the target inode
   * @throws FileDoesNotExistException if the target inode does not exist
   */
  public synchronized Inode getInode() throws FileDoesNotExistException {
    if (!fullPathExists()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(mUri));
    }
    return mInodes.get(mInodes.size() - 1);
  }

  /**
   * Returns the target inode, as an {@link InodeFile}. If the target inode does not exist, or it
   * is not a file, an exception will be thrown.
   *
   * @return the target inode as an {@link InodeFile}
   * @throws FileDoesNotExistException if the target inode does not exist, or it is not a file
   */
  public synchronized  InodeFile getInodeFile() throws FileDoesNotExistException {
    Inode inode = getInode();
    if (!inode.isFile()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(mUri));
    }
    return (InodeFile) inode;
  }

  /**
   * Returns the parent of the target inode. If the parent of the target inode does not exist, an
   * exception will be thrown.
   *
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
    Inode inode = mInodes.get(mPathComponents.length - 2);
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

  @Override
  public synchronized void close() {
    mLockGroup.close();
  }
}
