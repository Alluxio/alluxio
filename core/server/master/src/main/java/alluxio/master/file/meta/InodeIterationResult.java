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

/**
 * The inode, its full path, and the locked path.
 */
public class InodeIterationResult {
  private final Inode mInode;
  private final LockedInodePath mLockedPath;

  /**
   * Creates an instance.
   * @param inode the inode
   * @param lockedPath the locked path
   */
  public InodeIterationResult(
      Inode inode, LockedInodePath lockedPath) {
    mInode = inode;
    mLockedPath = lockedPath;
  }

  /**
   * @return the inode
   */
  public Inode getInode() {
    return mInode;
  }

  /**
   * @return the locked path
   */
  public LockedInodePath getLockedPath() {
    return mLockedPath;
  }

  @Override
  public String toString() {
    return mLockedPath.getUri().toString();
  }
}
