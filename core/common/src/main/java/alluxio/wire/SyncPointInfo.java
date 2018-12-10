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
   * Indicates the status of the initial sync of the active sync point.
   */
  public enum SyncStatus {
    // This state is possible when initial syncing is turned off
    NOT_INITIALLY_SYNCED,
    SYNCING,
    INITIALLY_SYNCED
  }

  private final AlluxioURI mSyncPointUri;
  private final SyncStatus mSyncStatus;

  /**
   * Constructs a SyncPointInfo object.
   *
   * @param syncPoint path to the sync point
   * @param syncStatus current syncing status
   */
  public SyncPointInfo(AlluxioURI syncPoint, SyncStatus syncStatus) {
    mSyncPointUri = syncPoint;
    mSyncStatus = syncStatus;
  }

  /**
   * Get the uri of the sync point.
   * @return uri of the sync point
   */
  public AlluxioURI getSyncPointUri() {
    return mSyncPointUri;
  }

  /**
   * Get the initial sync status.
   * @return the initial sync status
   */
  public SyncStatus getSyncStatus() {
    return mSyncStatus;
  }

  /**
   * @return thrift representation of the sync point information
   */
  public alluxio.thrift.SyncPointInfo toThrift() {
    alluxio.thrift.SyncPointStatus status =
        alluxio.thrift.SyncPointStatus.findByValue(mSyncStatus.ordinal());

    alluxio.thrift.SyncPointInfo info = new alluxio.thrift.SyncPointInfo(
        mSyncPointUri.getPath(), status);
    return info;
  }

  /**
   * Generate sync point information from the thrift representation.
   * @param syncPointInfo the thrift representation
   * @return sync point info object
   */
  public static SyncPointInfo fromThrift(alluxio.thrift.SyncPointInfo syncPointInfo) {
    SyncStatus syncStatus;
    switch (syncPointInfo.getSyncStatus()) {
      case Not_Initially_Synced:
        syncStatus = SyncStatus.NOT_INITIALLY_SYNCED;
        break;
      case Syncing:
        syncStatus = SyncStatus.SYNCING;
        break;
      case Initially_Synced:
        syncStatus = SyncStatus.INITIALLY_SYNCED;
        break;
      default:
        syncStatus = SyncStatus.NOT_INITIALLY_SYNCED;
    }
    return new SyncPointInfo(new AlluxioURI(syncPointInfo.getSyncPointUri()), syncStatus);
  }
}
