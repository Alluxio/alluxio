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

package alluxio.master.file.metasync;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import com.google.common.base.Joiner;

/**
 * Metadata sync results.
 */
public class SyncResult {
  public SyncResult(boolean success, long syncDuration,
                    Map<SyncOperation, Long> successOperationCount,
                    Map<SyncOperation, Long> failedOperationCount,
                    @Nullable SyncFailReason failReason, long numUfsFileScanned) {
    mSuccess = success;
    mSyncDuration = syncDuration;
    mSuccessOperationCount = successOperationCount;
    mFailedOperationCount = failedOperationCount;
    mSyncFailReason = failReason;
    mNumUfsFileScanned = numUfsFileScanned;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("{ SyncResult ");
    builder.append("success: ").append(mSuccess);
    builder.append(", sync duration: ").append(mSyncDuration).append("ms");
    builder.append(", fail reason: ").append(mSyncFailReason);
    builder.append(", num ufs files scanned: ").append(mNumUfsFileScanned);
    builder.append(", success op count: ");
    builder.append(Joiner.on(",").withKeyValueSeparator(":").join(mSuccessOperationCount));
    builder.append(", fail op count: ");
    builder.append(Joiner.on(",").withKeyValueSeparator(":").join(mFailedOperationCount));
    builder.append(" }");
    return builder.toString();
  }

  public static SyncResult merge(SyncResult r1, SyncResult r2) {
    boolean success = true;
    if (!r1.mSuccess || !r2.mSuccess) {
      success = false;
    }
    long syncDuration = r1.mSyncDuration + r2.mSyncDuration;
    SyncFailReason failReason = null;
    if (r1.mSyncFailReason != null) {
      failReason = r1.mSyncFailReason;
    }
    if (r2.mSyncFailReason != null) {
      failReason = r2.mSyncFailReason;
    }
    long numUfsFileScanned = r1.mNumUfsFileScanned + r2.mNumUfsFileScanned;
    HashMap<SyncOperation, Long> successOperationCount = new HashMap<>(r1.mSuccessOperationCount);
    for (Map.Entry<SyncOperation, Long> nxt : r2.mSuccessOperationCount.entrySet()) {
      successOperationCount.merge(nxt.getKey(), nxt.getValue(), Long::sum);
    }
    HashMap<SyncOperation, Long> failedOperationCount = new HashMap<>(r1.mFailedOperationCount);
    for (Map.Entry<SyncOperation, Long> nxt : r2.mFailedOperationCount.entrySet()) {
      failedOperationCount.merge(nxt.getKey(), nxt.getValue(), Long::sum);
    }
    return new SyncResult(success, syncDuration, successOperationCount,
        failedOperationCount, failReason, numUfsFileScanned);
  }

  private final boolean mSuccess;
  private final long mSyncDuration;

  private final Map<SyncOperation, Long> mSuccessOperationCount;
  private final Map<SyncOperation, Long> mFailedOperationCount;

  private @Nullable final SyncFailReason mSyncFailReason;
  private final long mNumUfsFileScanned;

  /**
   * @return the sync duration in ms
   */
  public long getSyncDuration() {
    return mSyncDuration;
  }

  /**
   * @return the success operation count map
   */
  public Map<SyncOperation, Long> getSuccessOperationCount() {
    return mSuccessOperationCount;
  }

  /**
   * @return # of ufs files scanned
   */
  public long getNumUfsFileScanned() {
    return mNumUfsFileScanned;
  }

  /**
   * @return the failed operation count map
   */
  public Map<SyncOperation, Long> getFailedOperationCount() {
    return mFailedOperationCount;
  }

  /**
   * @return the fail reason if the sync failed
   */
  public @Nullable SyncFailReason getFailReason() {
    return mSyncFailReason;
  }

  /**
   * @return if the metadata sync succeeded
   */
  public boolean getSuccess() {
    return mSuccess;
  }
}
