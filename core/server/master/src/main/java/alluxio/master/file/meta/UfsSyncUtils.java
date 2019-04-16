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

import alluxio.underfs.Fingerprint;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Util methods for UFS sync.
 */
@NotThreadSafe
public final class UfsSyncUtils {

  private UfsSyncUtils() {} // prevent instantiation

  /**
   * Given an inode and ufs status, returns a sync plan describing how to
   * sync the inode with the ufs.
   *
   * @param inode the inode to sync
   * @param ufsFingerprint the ufs fingerprint to check for the sync
   * @param containsMountPoint true if this inode contains a mount point, false otherwise
   * @return a {@link SyncPlan} describing how to sync the inode with the ufs
   */
  public static SyncPlan computeSyncPlan(Inode inode, Fingerprint ufsFingerprint,
      boolean containsMountPoint) {
    Fingerprint inodeFingerprint =  Fingerprint.parse(inode.getUfsFingerprint());
    boolean isContentSynced = inodeUfsIsContentSynced(inode, inodeFingerprint, ufsFingerprint);
    boolean isMetadataSynced = inodeUfsIsMetadataSynced(inode, inodeFingerprint, ufsFingerprint);
    boolean ufsExists = ufsFingerprint.isValid();
    boolean ufsIsDir = ufsFingerprint != null
        && Fingerprint.Type.DIRECTORY.name().equals(ufsFingerprint.getTag(Fingerprint.Tag.TYPE));

    UfsSyncUtils.SyncPlan syncPlan = new UfsSyncUtils.SyncPlan();

    if (isContentSynced && isMetadataSynced) {
      // Inode is already synced.
      if (inode.isDirectory() && inode.isPersisted()) {
        // Both Alluxio and UFS are directories, so sync the children of the directory.
        syncPlan.setSyncChildren();
      }
      return syncPlan;
    }

    // One of the metadata or content is not in sync
    if (inode.isDirectory() && (containsMountPoint || ufsIsDir)) {
      // Instead of deleting and then loading metadata to update, try to update directly
      // - mount points (or paths with mount point descendants) should not be deleted
      // - directory permissions can be updated without removing the inode
      if (inode.getParentId() != InodeTree.NO_PARENT) {
        // Only update the inode if it is not the root directory. The root directory is a special
        // case, since it is expected to be owned by the process that starts the master, and not
        // the owner on UFS.
        syncPlan.setUpdateMetadata();
      }
      syncPlan.setSyncChildren();
      return syncPlan;
    }

    // One of metadata or content is not in sync and it is a file
    // The only way for a directory to reach this point, is that the ufs with the same path is not
    // a directory. That requires a deletion and reload as well.
    if (!isContentSynced) {
      // update inode, by deleting and then optionally loading metadata
      syncPlan.setDelete();
      if (ufsExists) {
        // UFS exists, so load metadata later.
        syncPlan.setLoadMetadata();
      }
    } else {
      syncPlan.setUpdateMetadata();
    }
    return syncPlan;
  }

  /**
   * Returns true if the given inode's content is synced with the ufs status. This is a single inode
   * check, so for directory inodes, this does not consider the children inodes.
   *
   * @param inode the inode to check for sync
   * @param inodeFingerprint the inode's parsed fingerprint
   * @param ufsFingerprint the ufs fingerprint to check for the sync
   * @return true of the inode is synced with the ufs status
   */
  public static boolean inodeUfsIsContentSynced(Inode inode, Fingerprint inodeFingerprint,
      Fingerprint ufsFingerprint) {
    boolean isSyncedUnpersisted =
        !inode.isPersisted() && !ufsFingerprint.isValid();
    boolean isSyncedPersisted;

    isSyncedPersisted = inode.isPersisted()
        && inodeFingerprint.matchContent(ufsFingerprint)
        && inodeFingerprint.isValid();

    return isSyncedPersisted || isSyncedUnpersisted;
  }

  /**
   * Returns true if the given inode's metadata matches the ufs fingerprint.
   *
   * @param inode the inode to check for sync
   * @param inodeFingerprint the inode's parsed fingerprint
   * @param ufsFingerprint the ufs fingerprint to check for the sync
   * @return true of the inode is synced with the ufs status
   */
  public static boolean inodeUfsIsMetadataSynced(Inode inode, Fingerprint inodeFingerprint,
      Fingerprint ufsFingerprint) {
    return inodeFingerprint.isValid() && inodeFingerprint.matchMetadata(ufsFingerprint);
  }

  /**
   * A class describing how to sync an inode with the ufs.
   * A sync plan has several steps:
   * 1. updateMetadata: the file or directory should update its metadata from UFS
   * 2. delete: the inode should be deleted
   * 3. syncChildren: the inode is a directory, and the children should be synced
   * 4. loadMetadata: the inode metadata should be loaded from UFS
   */
  public static final class SyncPlan {
    private boolean mUpdateMetadata;
    private boolean mDelete;
    private boolean mLoadMetadata;
    private boolean mSyncChildren;

    SyncPlan() {
      mDelete = false;
      mUpdateMetadata = false;
      mLoadMetadata = false;
      mSyncChildren = false;
    }

    void setUpdateMetadata() {
      mUpdateMetadata = true;
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
     * @return true if the inode should update metadata from ufs fingerprint
     */
    public boolean toUpdateMetaData() {
      return mUpdateMetadata;
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
