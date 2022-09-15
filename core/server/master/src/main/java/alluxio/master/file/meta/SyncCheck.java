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

import com.google.common.base.Preconditions;

/**
 * Returned by {@link UfsSyncPathCache#shouldSyncPath} indicating whether
 * a sync is needed as well as the time of the last sync if one is not needed.
 * After the sync is performed, calling {@link SyncCheck#syncSuccess()} or
 * {@link SyncCheck#skippedSync()} will result in creating an object containing
 * information pertaining to the result of the sync.
 */
public class SyncCheck {
  private final boolean mShouldSync;
  private final long mLastSyncTime;

  /** Sync is not needed, with no last sync time. **/
  public static final SyncCheck SHOULD_NOT_SYNC = new SyncCheck(
      false, 0);
  /** Sync is needed. **/
  public static final SyncCheck SHOULD_SYNC = new SyncCheck(
      true, 0);

  /**
   * Create a SyncCheck object indicating a sync is not needed due to
   * a recent sync.
   * @param lastSyncTime the time of the last sync
   * @return the result object
   */
  public static SyncCheck shouldNotSyncWithTime(long lastSyncTime) {
    return new SyncCheck(false, lastSyncTime);
  }

  private SyncCheck(boolean shouldSync, long lastSyncTime) {
    mShouldSync = shouldSync;
    mLastSyncTime = lastSyncTime;
  }

  @Override
  public String toString() {
    return String.format("{Should sync: %b, Last sync time: %d}", mShouldSync, mLastSyncTime);
  }

  /**
   * @return true if a sync is necessary
   */
  public boolean isShouldSync() {
    return mShouldSync;
  }

  /**
   * This method should be called if the sync was performed successfully.
   * @return the {@link SyncResult} object
   */
  public SyncResult syncSuccess() {
    return SyncResult.SYNC_SUCCESS;
  }

  /**
   * This method should be called if the sync skipped due to the existance
   * of a recent sync.
   * @return the {@link SyncResult} object
   */
  public SyncResult skippedSync() {
    return SyncResult.skippedWithTime(mLastSyncTime);
  }

  /**
   * Information about the result of a synchronization, created by calling
   * {@link SyncCheck#syncSuccess()} or {@link SyncCheck#skippedSync()},
   * or using {@link SyncResult#INVALID_RESULT} if the sync failed
   * due to an external reason.
   */
  public static class SyncResult {
    private final boolean mPerformedSync;
    private final boolean mResultValid;
    private final long mLastSyncTime;

    public static final SyncResult INVALID_RESULT = new SyncResult(
        false, false, 0);

    private static final SyncResult SYNC_SUCCESS = new SyncResult(
        true, true, 0);

    private static SyncResult skippedWithTime(long lastSyncTime) {
      return new SyncResult(false, true, lastSyncTime);
    }

    private SyncResult(boolean syncPerformed, boolean resultValid, long lastSyncTime) {
      mPerformedSync = syncPerformed;
      mResultValid = resultValid;
      mLastSyncTime = lastSyncTime;
    }

    /**
     * @return true if the result was due to a valid sync (eiter a successful sync,
     * or a sync skipped due to a recent existing sync)
     */
    public boolean isResultValid() {
      return mResultValid;
    }

    /**
     * @return true if a synchronization was performed
     */
    public boolean wasSyncPerformed() {
      return mPerformedSync;
    }

    /**
     * @return the time of the last synchronization of this path
     */
    public long getLastSyncTime() {
      Preconditions.checkState(isResultValid() && !wasSyncPerformed(),
          "last sync time is only valid if the sync result is valid and a sync was not performed");
      return mLastSyncTime;
    }
  }
}
