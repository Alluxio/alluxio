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
import alluxio.master.file.meta.InodeTree.LockPattern;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents information about how a path should be locked.
 */
@ThreadSafe
public final class LockingScheme {
  private final AlluxioURI mPath;
  private final LockPattern mDesiredLockPattern;
  private final boolean mShouldSync;

  /**
   * Constructs a {@link LockingScheme}.
   *
   * @param path the path to lock
   * @param desiredLockPattern the desired lock mode
   * @param shouldSync true if the path should be synced
   */
  public LockingScheme(AlluxioURI path, LockPattern desiredLockPattern, boolean shouldSync) {
    mPath = path;
    mDesiredLockPattern = desiredLockPattern;
    mShouldSync = shouldSync;
  }

  /**
   * @return the desired mode for the locking
   */
  public LockPattern getDesiredPattern() {
    return mDesiredLockPattern;
  }

  /**
   * @return the mode that should be used to lock the path, considering if ufs sync should occur
   */
  public LockPattern getPattern() {
    if (mShouldSync) {
      // Syncing needs to be able to delete the inode if it was deleted in the UFS.
      return LockPattern.WRITE_EDGE;
    }
    return mDesiredLockPattern;
  }

  /**
   * @return the path for this locking scheme
   */
  public AlluxioURI getPath() {
    return mPath;
  }

  /**
   * @return true if the path should be synced with ufs
   */
  public boolean shouldSync() {
    return mShouldSync;
  }
}
