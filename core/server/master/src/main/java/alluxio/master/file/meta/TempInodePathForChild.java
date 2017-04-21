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

import alluxio.exception.InvalidPathException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents a temporary {@link LockedInodePath}, with a child component joined to the
 * existing path. This {@link LockedInodePath} will not unlock the inodes on close.
 *
 * This is useful for being able to pass in a new child path based on an existing
 * {@link LockedInodePath}, without having to re-traverse the inode tree, and re-acquire locks.
 * This allows methods to operate on a child path component extending from an existing
 * {@link LockedInodePath}.
 */
@ThreadSafe
public final class TempInodePathForChild extends MutableLockedInodePath {

  /**
   * Constructs a temporary {@link LockedInodePath} from an existing {@link LockedInodePath}.
   *
   * @param inodePath the {@link LockedInodePath} to create the temporary path from
   * @param childComponent the child component
   * @throws InvalidPathException if the path is invalid
   */
  public TempInodePathForChild(LockedInodePath inodePath, String childComponent)
      throws InvalidPathException {
    super(inodePath.mUri.join(childComponent), inodePath.mInodes, inodePath.mLockList,
        inodePath.mLockMode);
  }

  @Override
  public synchronized void close() {
    // nothing to close
  }
}
