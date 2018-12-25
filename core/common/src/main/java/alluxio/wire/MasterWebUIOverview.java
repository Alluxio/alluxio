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
import alluxio.util.webui.StorageTierInfo;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI overview information.
 */
@NotThreadSafe
public final class MasterWebUIOverview implements Serializable {
  private boolean mDebug;
  private String mCapacity;
  private ConfigCheckReport.ConfigStatus mConfigCheckStatus;
  private int mComfigCheckWarnNum;
  private int mConfigCheckErrorNum;
  private int mInconsistentPaths;
  private String mLiveWorkerNodes;
  private List<StorageTierInfo> mStorageTierInfos;
  private List<AlluxioURI> mInconsistentPathItems;
  private Map<Scope, List<InconsistentProperty>> mConfigCheckErrors;
  private Map<Scope, List<InconsistentProperty>> mConfigCheckWarns;
  private String mConsistencyCheckStatus;
  private String mDiskCapacity;
  private String mDiskFreeCapacity;
  private String mDiskUsedCapacity;
  private String mFreeCapacity;
  private String mMasterNodeAddress;
  private String mStartTime;
  private String mUptime;
  private String mUsedCapacity;
  private String mVersion;

  /**
   * Creates a new instance of {@link MasterWebUIOverview}.
   */
  public MasterWebUIOverview() {
  }

  /**
   * @return the capacity
   */
  public String getCapacity() {
    return mCapacity;
  }

  /**
   * @return the number of config check errors
   */
  public int getConfigCheckErrorNum() {
    return mConfigCheckErrorNum;
  }

  /**
   * @return the config check errors
   */
  public Map<Scope, List<InconsistentProperty>> getConfigCheckErrors() {
    return mConfigCheckErrors;
  }

  /**
   * @return the config check status
   */
  public ConfigCheckReport.ConfigStatus getConfigCheckStatus() {
    return mConfigCheckStatus;
  }

  /**
   * @return the config check warnings
   */
  public Map<Scope, List<InconsistentProperty>> getConfigCheckWarns() {
    return mConfigCheckWarns;
  }

  /**
   * @return the consistency check status
   */
  public String getConsistencyCheckStatus() {
    return mConsistencyCheckStatus;
  }

  /**
   * @return the debug value
   */
  public boolean getDebug() {
    return mDebug;
  }

  /**
   * @return the disk capacity
   */
  public String getDiskCapacity() {
    return mDiskCapacity;
  }

  /**
   * @return the free disk capacity
   */
  public String getDiskFreeCapacity() {
    return mDiskFreeCapacity;
  }

  /**
   * @return the used disk capacity
   */
  public String getDiskUsedCapacity() {
    return mDiskUsedCapacity;
  }

  /**
   * @return the free capacity
   */
  public String getFreeCapacity() {
    return mFreeCapacity;
  }

  /**
   * @return inconsistent path items
   */
  public List<AlluxioURI> getInconsistentPathItems() {
    return mInconsistentPathItems;
  }

  /**
   * @return live worker nodes
   */
  public String getLiveWorkerNodes() {
    return mLiveWorkerNodes;
  }

  /**
   * @return the master node address
   */
  public String getMasterNodeAddress() {
    return mMasterNodeAddress;
  }

  /**
   * @return the start time
   */
  public String getStartTime() {
    return mStartTime;
  }

  /**
   * @return the storage tier infos
   */
  public List<StorageTierInfo> getStorageTierInfos() {
    return mStorageTierInfos;
  }

  /**
   * @return the uptime
   */
  public String getUptime() {
    return mUptime;
  }

  /**
   * @return used capacity
   */
  public String getUsedCapacity() {
    return mUsedCapacity;
  }

  /**
   * @return the version
   */
  public String getVersion() {
    return mVersion;
  }

  /**
   * @return
   */
  public int getInconsistentPaths() {
    return mInconsistentPaths;
  }

  public int getComfigCheckWarnNum() {
    return mComfigCheckWarnNum;
  }

  /**
   * @param capacity
   * @return
   */
  public MasterWebUIOverview setCapacity(String capacity) {
    mCapacity = capacity;
    return this;
  }

  /**
   * @param configCheckErrorNum
   * @return
   */
  public MasterWebUIOverview setConfigCheckErrorNum(int configCheckErrorNum) {
    mConfigCheckErrorNum = configCheckErrorNum;
    return this;
  }

  /**
   * @param configCheckErrors
   * @return
   */
  public MasterWebUIOverview setConfigCheckErrors(
      Map<Scope, List<InconsistentProperty>> configCheckErrors) {
    mConfigCheckErrors = configCheckErrors;
    return this;
  }

  /**
   * @param configCheckStatus
   * @return
   */
  public MasterWebUIOverview setConfigCheckStatus(
      ConfigCheckReport.ConfigStatus configCheckStatus) {
    mConfigCheckStatus = configCheckStatus;
    return this;
  }

  /**
   * @param configCheckWarns
   * @return
   */
  public MasterWebUIOverview setConfigCheckWarns(
      Map<Scope, List<InconsistentProperty>> configCheckWarns) {
    mConfigCheckWarns = configCheckWarns;
    return this;
  }

  /**
   * @param consistencyCheckStatus
   * @return
   */
  public MasterWebUIOverview setConsistencyCheckStatus(String consistencyCheckStatus) {
    mConsistencyCheckStatus = consistencyCheckStatus;
    return this;
  }

  /**
   * @param debug
   * @return
   */
  public MasterWebUIOverview setDebug(boolean debug) {
    mDebug = debug;
    return this;
  }

  /**
   * @param diskCapacity
   * @return
   */
  public MasterWebUIOverview setDiskCapacity(String diskCapacity) {
    mDiskCapacity = diskCapacity;
    return this;
  }

  /**
   * @param diskFreeCapacity
   * @return
   */
  public MasterWebUIOverview setDiskFreeCapacity(String diskFreeCapacity) {
    mDiskFreeCapacity = diskFreeCapacity;
    return this;
  }

  /**
   * @param diskUsedCapacity
   * @return
   */
  public MasterWebUIOverview setDiskUsedCapacity(String diskUsedCapacity) {
    mDiskUsedCapacity = diskUsedCapacity;
    return this;
  }

  /**
   * @param freeCapacity
   * @return
   */
  public MasterWebUIOverview setFreeCapacity(String freeCapacity) {
    mFreeCapacity = freeCapacity;
    return this;
  }

  /**
   * @param inconsistentPathItems
   * @return
   */
  public MasterWebUIOverview setInconsistentPathItems(List<AlluxioURI> inconsistentPathItems) {
    mInconsistentPathItems = inconsistentPathItems;
    return this;
  }

  /**
   * @param liveWorkerNodes
   * @return
   */
  public MasterWebUIOverview setLiveWorkerNodes(String liveWorkerNodes) {
    mLiveWorkerNodes = liveWorkerNodes;
    return this;
  }

  /**
   * @param masterNodeAddress
   * @return
   */
  public MasterWebUIOverview setMasterNodeAddress(String masterNodeAddress) {
    mMasterNodeAddress = masterNodeAddress;
    return this;
  }

  /**
   * @param startTime
   * @return
   */
  public MasterWebUIOverview setStartTime(String startTime) {
    mStartTime = startTime;
    return this;
  }

  /**
   * @param storageTierInfos
   * @return
   */
  public MasterWebUIOverview setStorageTierInfos(List<StorageTierInfo> storageTierInfos) {
    mStorageTierInfos = storageTierInfos;
    return this;
  }

  /**
   * @param uptime
   * @return
   */
  public MasterWebUIOverview setUptime(String uptime) {
    mUptime = uptime;
    return this;
  }

  /**
   * @param usedCapacity
   * @return
   */
  public MasterWebUIOverview setUsedCapacity(String usedCapacity) {
    mUsedCapacity = usedCapacity;
    return this;
  }

  /**
   * @param version
   * @return
   */
  public MasterWebUIOverview setVersion(String version) {
    mVersion = version;
    return this;
  }

  /**
   * @param size
   * @return
   */
  public MasterWebUIOverview setInconsistentPaths(int size) {
    mInconsistentPaths = size;
    return this;
  }

  /**
   * @param configCheckWarnNum
   * @return
   */
  public MasterWebUIOverview setConfigCheckWarnNum(int configCheckWarnNum) {
    mComfigCheckWarnNum = configCheckWarnNum;
    return this;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("capacity", mCapacity)
        .add("configCheckErrorNum", mConfigCheckErrorNum)
        .add("configCheckErrors", mConfigCheckErrors).add("configCheckStatus", mConfigCheckStatus)
        .add("configCheckWarns", mConfigCheckWarns)
        .add("consistencyCheckStatus", mConsistencyCheckStatus).add("debug", mDebug)
        .add("diskCapacity", mDiskCapacity).add("diskFreeCapacity", mDiskFreeCapacity)
        .add("diskUsedCapacity", mDiskUsedCapacity).add("freeCapacity", mFreeCapacity)
        .add("inconsistentPathItems", mInconsistentPathItems)
        .add("liveWorkerNodes", mLiveWorkerNodes).add("masterNodeAddress", mMasterNodeAddress)
        .add("startTime", mStartTime).add("storageTierInfos", mStorageTierInfos)
        .add("uptime", mUptime).add("usedCapacity", mUsedCapacity).add("version", mVersion)
        .add("inconsistentPaths", mInconsistentPaths).toString();
  }
}
