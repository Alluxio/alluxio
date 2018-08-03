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

import javax.annotation.concurrent.ThreadSafe;

/**
 * This class represents information about how a path should be locked.
 */
@ThreadSafe
public final class LockingScheme {
  private final AlluxioURI mPath;
  private final InodeTree.LockMode mDesiredLockMode;
  private final boolean mShouldSync;

  /**
   * Constructs a {@link LockingScheme}.
   *
   * @param path the path to lock
   * @param desiredLockMode the desired lock mode
   * @param shouldSync true if the path should be synced
   */
  public LockingScheme(AlluxioURI path, InodeTree.LockMode desiredLockMode, boolean shouldSync) {
    mPath = path;
    mDesiredLockMode = desiredLockMode;
    mShouldSync = shouldSync;
  }

  /**
   * @return the desired mode for the locking
   */
  InodeTree.LockMode getDesiredMode() {
    return mDesiredLockMode;
  }

  /**
   * @return the mode that should be used to lock the path, considering if ufs sync should occur
   */
  public InodeTree.LockMode getMode() {
    if (mShouldSync) {
      // Syncing requires write.
      return InodeTree.LockMode.WRITE;
    }
    return mDesiredLockMode;
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
