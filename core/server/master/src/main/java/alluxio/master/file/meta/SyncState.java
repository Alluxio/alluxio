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

import alluxio.file.options.DescendantType;

/**
 * This class is used to keep the state of the items in {@link UfsSyncPathCache},
 * it contains information about the last time a path was synced (validated)
 * or marked as out of sync (invalidated).
 *
 * There is a most one writer at a time to a sync state.
 * But multiple readers may be reading the values while they
 * are being updated, thus the values are volatile.
 */
class SyncState {
  /** The last time a direct child was invalidated. */
  volatile long mDirectChildrenInvalidation = 0;
  /** The last time a (non-direct) child was invalidated. */
  volatile long mRecursiveChildrenInvalidation = 0;
  /** The last time this exact path was invalidated. */
  volatile long mInvalidationTime = 0;
  /** The last time this path was synced. */
  volatile long mSyncTime = 0;
  /** The last time this path and its direct children were synced. */
  volatile long mDirectChildrenSyncTime = 0;
  /** The last time this path and all is children were synced recursively. */
  volatile long mRecursiveSyncTime = 0;
  /** True iff this path is a file.
   * Note that being marked as a file means that any
   * sync performed on this path will be considered a recursive sync.
   * If it is not known if this path is a file, then this value will be false. */
  volatile boolean mIsFile;

  /**
   * Create a new SyncState object.
   * @param isFile if the path is a file
   */
  SyncState(boolean isFile) {
    mIsFile = isFile;
  }

  /**
   * @param time the time of the most recent invalidation
   */
  void setInvalidationTime(long time) {
    if (time > mInvalidationTime) {
      mInvalidationTime = time;
    }
  }

  /**
   * @param time the time of the most recent invalidation
   *             of a direct child
   */
  void setDirectChildInvalidation(long time) {
    if (time > mDirectChildrenInvalidation) {
      mDirectChildrenInvalidation = time;
    }
  }

  /**
   * @param time the time of the most recent invalidation
   *             of a recursive child
   */
  void setRecursiveChildInvalidation(long time) {
    if (time > mRecursiveChildrenInvalidation) {
      mRecursiveChildrenInvalidation = time;
    }
  }

  /**
   * @param isFile if the path is a file
   */
  void setIsFile(boolean isFile) {
    // only transition from file to directory
    // and not the other way around
    if (!isFile && mIsFile) {
      mIsFile = false;
    }
  }

  /**
   * @param time the most recent validation (sync) time
   * @param descendantType the type of sync performed
   */
  void setValidationTime(long time, DescendantType descendantType) {
    if (time > mSyncTime) {
      mSyncTime = time;
    }
    if (descendantType == DescendantType.ALL) {
      if (time > mRecursiveSyncTime) {
        mRecursiveSyncTime = time;
      }
      if (time > mDirectChildrenSyncTime) {
        mDirectChildrenSyncTime = time;
      }
    } else if (descendantType == DescendantType.ONE) {
      if (time > mDirectChildrenSyncTime) {
        mDirectChildrenSyncTime = time;
      }
    }
  }
}
