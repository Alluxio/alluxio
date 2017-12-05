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

import alluxio.underfs.UfsFileStatus;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Util methods for UFS sync.
 */
@NotThreadSafe
public final class UfsSyncUtils {

  /**
   * Returns the {@link UfsStatus} of the given path.
   *
   * @param ufs the ufs object
   * @param path the path to get the status for, can be a file or directory
   * @return returns the {@link UfsStatus} of the given path
   */
  public static UfsStatus getUfsStatus(UnderFileSystem ufs, String path) {
    UfsStatus ufsStatus = null;
    try {
      if (ufs.isFile(path)) {
        ufsStatus = ufs.getFileStatus(path);
      }
    } catch (IOException e) {
      // ignore error, since ufs path may not exist, or may be a directory.
    }

    if (ufsStatus == null) {
      try {
        if (ufs.isDirectory(path)) {
          ufsStatus = ufs.getDirectoryStatus(path);
        }
      } catch (IOException e) {
        // ignore error, since ufs path may not exist, or may be a directory.
      }
    }
    return ufsStatus;
  }

  /**
   * Given an {@link Inode} and {@link UfsStatus}, returns a {@link SyncPlan} describing how to
   * sync the inode with the ufs.
   *
   * @param inode the inode to sync
   * @param ufsStatus the ufs status to check for the sync
   * @return a {@link SyncPlan} describing how to sync the inode with the ufs
   */
  public static SyncPlan computeSyncPlan(Inode inode, UfsStatus ufsStatus) {
    boolean matches = inodeUfsMatch(inode, ufsStatus);

    UfsSyncUtils.SyncPlan syncPlan = new UfsSyncUtils.SyncPlan();
    if (!matches) {
      // UFS does not match with Alluxio inode.
      syncPlan.setDelete();
      if (ufsStatus != null) {
        // UFS exists, so load metadata later.
        syncPlan.setLoadMetadata();
      }
    }

    if (inode.isDirectory() && inode.isPersisted() && matches) {
      // Both Alluxio and UFS are directories, so sync the children of the directory.
      syncPlan.setSyncChildren();
    }

    return syncPlan;
  }

  /**
   * Returns true if the given inode matches the ufs status. This is a single inode check, so for
   * directory inodes, this does not consider the children inodes.
   *
   * @param inode the inode to check for sync
   * @param ufsStatus the ufs status to check for the sync
   * @return true of the inode matches with the ufs status
   */
  public static boolean inodeUfsMatch(Inode inode, UfsStatus ufsStatus) {
    boolean matchPersisted = false;
    boolean matchUnpersisted = !inode.isPersisted() && ufsStatus == null;
    if (inode.isFile()) {
      // Alluxio path is a file.
      InodeFile inodeFile = (InodeFile) inode;
      matchPersisted = inodeFile.isPersisted() && ufsStatus != null && ufsStatus.isFile()
          && ((UfsFileStatus) ufsStatus).getContentLength() == inodeFile.getLength();
    } else {
      // Alluxio path is a directory.
      matchPersisted = inode.isPersisted() && ufsStatus != null && ufsStatus.isDirectory();
    }
    return matchPersisted || matchUnpersisted;
  }

  private UfsSyncUtils() {} // prevent instantiation

  /**
   * A class describing how to sync an inode with the ufs.
   * A sync plan has several steps:
   * 1. delete: the inode should be deleted
   * 2. syncChildren: the inode is a directory, and the children should be synced
   * 3. loadMetadata: the inode metadata should loaded from UFS
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
