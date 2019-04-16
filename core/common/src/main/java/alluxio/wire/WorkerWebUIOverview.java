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

import alluxio.util.webui.UIStorageDir;
import alluxio.util.webui.UIUsageOnTier;
import alluxio.util.webui.UIWorkerInfo;

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI overview information.
 */
@NotThreadSafe
public final class WorkerWebUIOverview implements Serializable {
  private static final long serialVersionUID = -518535767688463473L;

  private List<UIStorageDir> mStorageDirs;
  private List<UIUsageOnTier> mUsageOnTiers;
  private String mCapacityBytes;
  private String mUsedBytes;
  private String mVersion;
  private UIWorkerInfo mWorkerInfo;

  /**
   * Creates a new instance of {@link WorkerWebUIOverview}.
   */
  public WorkerWebUIOverview() {
  }

  /**
   * Gets capacity bytes.
   *
   * @return the capacity bytes
   */
  public String getCapacityBytes() {
    return mCapacityBytes;
  }

  /**
   * Gets storage dirs.
   *
   * @return the storage dirs
   */
  public List<UIStorageDir> getStorageDirs() {
    return mStorageDirs;
  }

  /**
   * Gets usage on tiers.
   *
   * @return the usage on tiers
   */
  public List<UIUsageOnTier> getUsageOnTiers() {
    return mUsageOnTiers;
  }

  /**
   * Gets used bytes.
   *
   * @return the used bytes
   */
  public String getUsedBytes() {
    return mUsedBytes;
  }

  /**
   * Gets version.
   *
   * @return the version
   */
  public String getVersion() {
    return mVersion;
  }

  /**
   * Gets worker info.
   *
   * @return the worker info
   */
  public UIWorkerInfo getWorkerInfo() {
    return mWorkerInfo;
  }

  /**
   * Sets capacity bytes.
   *
   * @param CapacityBytes the capacity bytes
   * @return the capacity bytes
   */
  public WorkerWebUIOverview setCapacityBytes(String CapacityBytes) {
    mCapacityBytes = CapacityBytes;
    return this;
  }

  /**
   * Sets storage dirs.
   *
   * @param StorageDirs the storage dirs
   * @return the storage dirs
   */
  public WorkerWebUIOverview setStorageDirs(List<UIStorageDir> StorageDirs) {
    mStorageDirs = StorageDirs;
    return this;
  }

  /**
   * Sets usage on tiers.
   *
   * @param UsageOnTiers the usage on tiers
   * @return the usage on tiers
   */
  public WorkerWebUIOverview setUsageOnTiers(List<UIUsageOnTier> UsageOnTiers) {
    mUsageOnTiers = UsageOnTiers;
    return this;
  }

  /**
   * Sets used bytes.
   *
   * @param UsedBytes the used bytes
   * @return the used bytes
   */
  public WorkerWebUIOverview setUsedBytes(String UsedBytes) {
    mUsedBytes = UsedBytes;
    return this;
  }

  /**
   * Sets version.
   *
   * @param Version the version
   * @return the version
   */
  public WorkerWebUIOverview setVersion(String Version) {
    mVersion = Version;
    return this;
  }

  /**
   * Sets worker info.
   *
   * @param WorkerInfo the worker info
   * @return the worker info
   */
  public WorkerWebUIOverview setWorkerInfo(UIWorkerInfo WorkerInfo) {
    mWorkerInfo = WorkerInfo;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("capacityBytes", mCapacityBytes)
        .add("storageDirs", mStorageDirs).add("usageOnTiers", mUsageOnTiers)
        .add("usedBytes", mUsedBytes).add("version", mVersion).add("workerInfo", mWorkerInfo)
        .toString();
  }
}
