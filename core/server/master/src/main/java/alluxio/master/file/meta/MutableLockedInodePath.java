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

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents an {@link LockedInodePath}, where the list of inodes can be extended to
 * gather additional inodes along the path.
 */
@ThreadSafe
public class MutableLockedInodePath extends LockedInodePath {
  /**
   * Creates an instance of {@link MutableLockedInodePath}.
   *
   * @param uri the URI
   * @param inodes the inodes
   * @param lockList the lock list of the inodes
   * @param lockMode the lock mode for the path
   * @throws InvalidPathException if the path passed is invalid
   */
  // TODO(gpang): restructure class hierarchy, rename class
  public MutableLockedInodePath(AlluxioURI uri, List<Inode<?>> inodes, InodeLockList lockList,
      InodeTree.LockMode lockMode)
      throws InvalidPathException {
    super(uri, inodes, lockList, lockMode);
  }

  /**
   * Returns the closest ancestor of the target inode (last inode in the full path).
   *
   * @return the closest ancestor inode
   * @throws FileDoesNotExistException if an ancestor does not exist
   */
  public synchronized Inode<?> getAncestorInode() throws FileDoesNotExistException {
    int ancestorIndex = mPathComponents.length - 2;
    if (ancestorIndex < 0) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage(mUri));
    }
    ancestorIndex = Math.min(ancestorIndex, mInodes.size() - 1);
    return mInodes.get(ancestorIndex);
  }

  String[] getPathComponents() {
    return mPathComponents;
  }

  List<Inode<?>> getInodes() {
    return mInodes;
  }

  InodeLockList getLockList() {
    return mLockList;
  }
}
