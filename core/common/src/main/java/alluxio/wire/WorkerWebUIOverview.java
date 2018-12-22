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

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI overview information.
 */
@NotThreadSafe
public final class WorkerWebUIOverview implements Serializable {
  private String mCapacityBytes;
  private List<UIStorageDir> mStorageDirs;
  private List<UIUsageOnTier> mUsageOnTiers;
  private String mUsedBytes;
  private String mVersion;
  private UIWorkerInfo mWorkerInfo;

  /**
   * Creates a new instance of {@link WorkerWebUIOverview}.
   */
  public WorkerWebUIOverview() {
  }

  public String getCapacityBytes() {
    return mCapacityBytes;
  }

  public List<UIStorageDir> getStorageDirs() {
    return mStorageDirs;
  }

  public List<UIUsageOnTier> getUsageOnTiers() {
    return mUsageOnTiers;
  }

  public String getUsedBytes() {
    return mUsedBytes;
  }

  public String getVersion() {
    return mVersion;
  }

  public UIWorkerInfo getWorkerInfo() {
    return mWorkerInfo;
  }

  public WorkerWebUIOverview setCapacityBytes(String CapacityBytes) {
    mCapacityBytes = CapacityBytes;
    return this;
  }

  public WorkerWebUIOverview setStorageDirs(List<UIStorageDir> StorageDirs) {
    mStorageDirs = StorageDirs;
    return this;
  }

  public WorkerWebUIOverview setUsageOnTiers(List<UIUsageOnTier> UsageOnTiers) {
    mUsageOnTiers = UsageOnTiers;
    return this;
  }

  public WorkerWebUIOverview setUsedBytes(String UsedBytes) {
    mUsedBytes = UsedBytes;
    return this;
  }

  public WorkerWebUIOverview setVersion(String Version) {
    mVersion = Version;
    return this;
  }

  public WorkerWebUIOverview setWorkerInfo(UIWorkerInfo WorkerInfo) {
    mWorkerInfo = WorkerInfo;
    return this;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("mCapacityBytes", mCapacityBytes)
        .add("mStorageDirs", mStorageDirs).add("mUsageOnTiers", mUsageOnTiers)
        .add("mUsedBytes", mUsedBytes).add("mVersion", mVersion).add("mWorkerInfo", mWorkerInfo)
        .toString();
  }
}
