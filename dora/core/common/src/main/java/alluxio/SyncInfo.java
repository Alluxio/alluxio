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

package alluxio;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to represent what the active syncing process should sync.
 */
public class SyncInfo {
  public static final long INVALID_TXID = -1;
  private static final SyncInfo EMPTY_INFO = new SyncInfo(Collections.emptyMap(),
      false, INVALID_TXID);

  // A map mapping syncpoints to files changed in those sync points
  private final Map<AlluxioURI, Set<AlluxioURI>> mChangedFilesMap;

  // Ignore the changed files and simply sync the entire directory
  private final boolean mForceSync;

  // transaction id that we can restart from once this sync is complete
  private final long mTxId;

  /**
   * Constructs a SyncInfo.
   *
   * @param changedFiles a map mapping syncpoint to changed files
   * @param forceSync force sync the entire directory
   * @param txId the transaction id  that is synced in this sync
   */
  public SyncInfo(Map<AlluxioURI, Set<AlluxioURI>> changedFiles, boolean forceSync, long txId) {
    mChangedFilesMap = changedFiles;
    mForceSync = forceSync;
    mTxId = txId;
  }

  /**
   * Returns an empty SyncInfo object.
   *
   * @return emptyInfo object
   */
  public static SyncInfo emptyInfo() {
    return EMPTY_INFO;
  }

  /**
   * Returns a list of sync points.
   *
   * @return a list of sync points
   */
  public Set<AlluxioURI> getSyncPoints() {
    return mChangedFilesMap.keySet();
  }

  /**
   * REturns a set of changed files.
   *
   * @param syncPoint the syncPoint that we are monitoring
   * @return a set of sync points
   */
  public Set<AlluxioURI> getChangedFiles(AlluxioURI syncPoint) {
    return mChangedFilesMap.get(syncPoint);
  }

  /**
   * returns true if this sync should happen on the entire directory.
   *
   * @return true if this sync should happen on the entire dir
   */
  public boolean isForceSync() {
    return mForceSync;
  }

  /**
   * returns the transaction id that is synced in this sync period.
   *
   * @return a transaction id that is synced in this sync period
   */
  public long getTxId() {
    return mTxId;
  }
}
