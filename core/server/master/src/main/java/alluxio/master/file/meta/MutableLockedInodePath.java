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
import alluxio.exception.InvalidPathException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents an {@link LockedInodePath}, where the list of inodes can be extended to
 * gather additional inodes along the path.
 */
@ThreadSafe
public final class MutableLockedInodePath extends LockedInodePath {
  /**
   * Creates an instance of {@link MutableLockedInodePath}.
   *
   * @param uri the URI
   * @param lockList the lock list of the inodes
   * @param lockMode the lock mode for the path
   * @throws InvalidPathException if the path passed is invalid
   */
  public MutableLockedInodePath(AlluxioURI uri, InodeLockList lockList, InodeTree.LockMode lockMode)
      throws InvalidPathException {
    super(uri, lockList, lockMode);
  }

  synchronized InodeLockList getLockList() {
    return mLockList;
  }

  synchronized String[] getPathComponents() {
    return mPathComponents;
  }
}
