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
  volatile long mDirectChildrenInvalidation = 0;
  volatile long mRecursiveChildrenInvalidation = 0;
  volatile long mInvalidationTime = 0;
  volatile long mValidationTime = 0;
  volatile long mDirectValidationTime = 0;
  volatile long mRecursiveValidationTime = 0;
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
    if (time > mValidationTime) {
      mValidationTime = time;
    }
    if (descendantType == DescendantType.ALL) {
      if (time > mRecursiveValidationTime) {
        mRecursiveValidationTime = time;
      }
      if (time > mDirectValidationTime) {
        mDirectValidationTime = time;
      }
    } else if (descendantType == DescendantType.ONE) {
      if (time > mDirectValidationTime) {
        mDirectValidationTime = time;
      }
    }
  }
}
