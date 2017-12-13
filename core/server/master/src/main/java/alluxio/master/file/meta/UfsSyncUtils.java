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

import alluxio.Constants;
import alluxio.underfs.UfsStatus;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Util methods for UFS sync.
 */
@NotThreadSafe
public final class UfsSyncUtils {

  private UfsSyncUtils() {} // prevent instantiation

  /**
   * Given an {@link Inode} and {@link UfsStatus}, returns a {@link SyncPlan} describing how to
   * sync the inode with the ufs.
   *
   * @param inode the inode to sync
   * @param ufsFingerprint the ufs fingerprint to check for the sync
   * @return a {@link SyncPlan} describing how to sync the inode with the ufs
   */
  public static SyncPlan computeSyncPlan(Inode inode, String ufsFingerprint) {
    boolean isSynced = inodeUfsIsSynced(inode, ufsFingerprint);

    UfsSyncUtils.SyncPlan syncPlan = new UfsSyncUtils.SyncPlan();
    if (!isSynced) {
      // Alluxio inode is not synced with UFS.
      if (inode.getParentId() != InodeTree.NO_PARENT) {
        // Do not delete the root.
        syncPlan.setDelete();
      }
      if (!Constants.INVALID_UFS_FINGERPRINT.equals(ufsFingerprint)) {
        // UFS exists, so load metadata later.
        syncPlan.setLoadMetadata();
      }
    }

    if (inode.isDirectory() && inode.isPersisted() && isSynced) {
      // Both Alluxio and UFS are directories, so sync the children of the directory.
      syncPlan.setSyncChildren();
    }

    return syncPlan;
  }

  /**
   * Returns true if the given inode is synced with the ufs status. This is a single inode check,
   * so for directory inodes, this does not consider the children inodes.
   *
   * @param inode the inode to check for sync
   * @param ufsFingerprint the ufs fingerprint to check for the sync
   * @return true of the inode is synced with the ufs status
   */
  public static boolean inodeUfsIsSynced(Inode inode, String ufsFingerprint) {
    boolean isSyncedUnpersisted =
        !inode.isPersisted() && Constants.INVALID_UFS_FINGERPRINT.equals(ufsFingerprint);

    boolean isSyncedPersisted;
    if (inode instanceof InodeFile) {
      // check the file fingerprint.
      InodeFile inodeFile = (InodeFile) inode;
      isSyncedPersisted = inodeFile.isPersisted()
          && inodeFile.getUfsFingerprint().equals(ufsFingerprint)
          && !inodeFile.getUfsFingerprint().equals(Constants.INVALID_UFS_FINGERPRINT);
    } else {
      // ufs fingerprint must exist.
      // TODO(gpang): Currently, directory fingerprints are not considered, because directories are
      // created/modified more frequently than files.
      isSyncedPersisted =
          inode.isPersisted() && !Constants.INVALID_UFS_FINGERPRINT.equals(ufsFingerprint);
    }
    return isSyncedPersisted || isSyncedUnpersisted;
  }

  /**
   * A class describing how to sync an inode with the ufs.
   * A sync plan has several steps:
   * 1. delete: the inode should be deleted
   * 2. syncChildren: the inode is a directory, and the children should be synced
   * 3. loadMetadata: the inode metadata should be loaded from UFS
   */
  public static final class SyncPlan {
    private boolean mDelete;
    private boolean mLoadMetadata;
    private boolean mSyncChildren;

    SyncPlan() {
      mDelete = false;
      mLoadMetadata = false;
      mSyncChildren = false;
    }

    void setDelete() {
      mDelete = true;
    }

    void setLoadMetadata() {
      mLoadMetadata = true;
    }

    void setSyncChildren() {
      mSyncChildren = true;
    }

    /**
     * @return true if the inode should be deleted for the sync plan
     */
    public boolean toDelete() {
      return mDelete;
    }

    /**
     * @return true if the inode should load metadata from ufs
     */
    public boolean toLoadMetadata() {
      return mLoadMetadata;
    }

    /**
     * @return true if the children of the directory inode should be synced
     */
    public boolean toSyncChildren() {
      return mSyncChildren;
    }
  }
}
