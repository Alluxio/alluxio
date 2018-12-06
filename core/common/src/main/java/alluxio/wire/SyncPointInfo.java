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

package alluxio.wire;

import alluxio.AlluxioURI;

/**
 * This class represents the state of a sync point, whether the intial syncing is done,
 * in progress or not going to be done.
 */
public class SyncPointInfo {
  /**
   * Indicates the sync status of the active sync point
   */
  public enum SyncStatus {
    NOT_IN_SYNC,
    SYNCING,
    SYNCED
  }

  private final AlluxioURI mSyncPointUri;
  private final SyncStatus mSyncStatus;

  public SyncPointInfo(AlluxioURI syncPoint, SyncStatus syncStatus) {
    mSyncPointUri = syncPoint;
    mSyncStatus = syncStatus;
  }

  public AlluxioURI getSyncPointUri() {
    return mSyncPointUri;
  }

  public SyncStatus getSyncStatus() {
    return mSyncStatus;
  }

  /**
   * @return thrift representation of the file information
   */
  public alluxio.thrift.SyncPointInfo toThrift() {
    alluxio.thrift.SyncPointStatus status =  alluxio.thrift.SyncPointStatus.findByValue(mSyncStatus.ordinal());

    alluxio.thrift.SyncPointInfo info = new alluxio.thrift.SyncPointInfo(
        mSyncPointUri.getPath(), status);
    return info;
  }

  public static SyncPointInfo fromThrift(alluxio.thrift.SyncPointInfo syncPointInfo) {
    SyncStatus syncStatus;
    switch (syncPointInfo.getSyncStatus()) {
      case Not_In_Sync:
        syncStatus = SyncStatus.NOT_IN_SYNC;
        break;
      case Syncing:
        syncStatus = SyncStatus.SYNCING;
        break;
      case Synced:
        syncStatus = SyncStatus.SYNCED;
        break;
      default:
        syncStatus = SyncStatus.NOT_IN_SYNC;
    }
    return new SyncPointInfo(new AlluxioURI(syncPointInfo.getSyncPointUri()), syncStatus);
  }

}
