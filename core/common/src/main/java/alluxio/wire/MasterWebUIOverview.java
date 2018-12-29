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
  private static final long serialVersionUID = 6589358568781503724L;

  private boolean mDebug;
  private ConfigCheckReport.ConfigStatus mConfigCheckStatus;
  private int mComfigCheckWarnNum;
  private int mConfigCheckErrorNum;
  private int mInconsistentPaths;
  private List<StorageTierInfo> mStorageTierInfos;
  private List<String> mInconsistentPathItems;
  private Map<Scope, List<InconsistentProperty>> mConfigCheckErrors;
  private Map<Scope, List<InconsistentProperty>> mConfigCheckWarns;
  private String mCapacity;
  private String mConsistencyCheckStatus;
  private String mDiskCapacity;
  private String mDiskFreeCapacity;
  private String mDiskUsedCapacity;
  private String mFreeCapacity;
  private String mLiveWorkerNodes;
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
   * Gets capacity.
   *
   * @return the capacity
   */
  public String getCapacity() {
    return mCapacity;
  }

  /**
   * Gets config check error num.
   *
   * @return the number of config check errors
   */
  public int getConfigCheckErrorNum() {
    return mConfigCheckErrorNum;
  }

  /**
   * Gets config check errors.
   *
   * @return the config check errors
   */
  public Map<Scope, List<InconsistentProperty>> getConfigCheckErrors() {
    return mConfigCheckErrors;
  }

  /**
   * Gets config check status.
   *
   * @return the config check status
   */
  public ConfigCheckReport.ConfigStatus getConfigCheckStatus() {
    return mConfigCheckStatus;
  }

  /**
   * Gets config check warns.
   *
   * @return the config check warnings
   */
  public Map<Scope, List<InconsistentProperty>> getConfigCheckWarns() {
    return mConfigCheckWarns;
  }

  /**
   * Gets consistency check status.
   *
   * @return the consistency check status
   */
  public String getConsistencyCheckStatus() {
    return mConsistencyCheckStatus;
  }

  /**
   * Gets debug.
   *
   * @return the debug value
   */
  public boolean getDebug() {
    return mDebug;
  }

  /**
   * Gets disk capacity.
   *
   * @return the disk capacity
   */
  public String getDiskCapacity() {
    return mDiskCapacity;
  }

  /**
   * Gets disk free capacity.
   *
   * @return the free disk capacity
   */
  public String getDiskFreeCapacity() {
    return mDiskFreeCapacity;
  }

  /**
   * Gets disk used capacity.
   *
   * @return the used disk capacity
   */
  public String getDiskUsedCapacity() {
    return mDiskUsedCapacity;
  }

  /**
   * Gets free capacity.
   *
   * @return the free capacity
   */
  public String getFreeCapacity() {
    return mFreeCapacity;
  }

  /**
   * Gets inconsistent path items.
   *
   * @return inconsistent path items
   */
  public List<String> getInconsistentPathItems() {
    return mInconsistentPathItems;
  }

  /**
   * Gets live worker nodes.
   *
   * @return live worker nodes
   */
  public String getLiveWorkerNodes() {
    return mLiveWorkerNodes;
  }

  /**
   * Gets master node address.
   *
   * @return the master node address
   */
  public String getMasterNodeAddress() {
    return mMasterNodeAddress;
  }

  /**
   * Gets start time.
   *
   * @return the start time
   */
  public String getStartTime() {
    return mStartTime;
  }

  /**
   * Gets storage tier infos.
   *
   * @return the storage tier infos
   */
  public List<StorageTierInfo> getStorageTierInfos() {
    return mStorageTierInfos;
  }

  /**
   * Gets uptime.
   *
   * @return the uptime
   */
  public String getUptime() {
    return mUptime;
  }

  /**
   * Gets used capacity.
   *
   * @return used capacity
   */
  public String getUsedCapacity() {
    return mUsedCapacity;
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
   * Gets inconsistent paths.
   *
   * @return inconsistent paths
   */
  public int getInconsistentPaths() {
    return mInconsistentPaths;
  }

  /**
   * Gets comfig check warn num.
   *
   * @return the comfig check warn num
   */
  public int getComfigCheckWarnNum() {
    return mComfigCheckWarnNum;
  }

  /**
   * Sets capacity.
   *
   * @param capacity the capacity
   * @return capacity
   */
  public MasterWebUIOverview setCapacity(String capacity) {
    mCapacity = capacity;
    return this;
  }

  /**
   * Sets config check error num.
   *
   * @param configCheckErrorNum the config check error num
   * @return config check error num
   */
  public MasterWebUIOverview setConfigCheckErrorNum(int configCheckErrorNum) {
    mConfigCheckErrorNum = configCheckErrorNum;
    return this;
  }

  /**
   * Sets config check errors.
   *
   * @param configCheckErrors the config check errors
   * @return config check errors
   */
  public MasterWebUIOverview setConfigCheckErrors(
      Map<Scope, List<InconsistentProperty>> configCheckErrors) {
    mConfigCheckErrors = configCheckErrors;
    return this;
  }

  /**
   * Sets config check status.
   *
   * @param configCheckStatus the config check status
   * @return config check status
   */
  public MasterWebUIOverview setConfigCheckStatus(
      ConfigCheckReport.ConfigStatus configCheckStatus) {
    mConfigCheckStatus = configCheckStatus;
    return this;
  }

  /**
   * Sets config check warns.
   *
   * @param configCheckWarns the config check warns
   * @return config check warns
   */
  public MasterWebUIOverview setConfigCheckWarns(
      Map<Scope, List<InconsistentProperty>> configCheckWarns) {
    mConfigCheckWarns = configCheckWarns;
    return this;
  }

  /**
   * Sets consistency check status.
   *
   * @param consistencyCheckStatus the consistency check status
   * @return consistency check status
   */
  public MasterWebUIOverview setConsistencyCheckStatus(String consistencyCheckStatus) {
    mConsistencyCheckStatus = consistencyCheckStatus;
    return this;
  }

  /**
   * Sets debug.
   *
   * @param debug the debug
   * @return debug
   */
  public MasterWebUIOverview setDebug(boolean debug) {
    mDebug = debug;
    return this;
  }

  /**
   * Sets disk capacity.
   *
   * @param diskCapacity the disk capacity
   * @return disk capacity
   */
  public MasterWebUIOverview setDiskCapacity(String diskCapacity) {
    mDiskCapacity = diskCapacity;
    return this;
  }

  /**
   * Sets disk free capacity.
   *
   * @param diskFreeCapacity the disk free capacity
   * @return disk free capacity
   */
  public MasterWebUIOverview setDiskFreeCapacity(String diskFreeCapacity) {
    mDiskFreeCapacity = diskFreeCapacity;
    return this;
  }

  /**
   * Sets disk used capacity.
   *
   * @param diskUsedCapacity the disk used capacity
   * @return disk used capacity
   */
  public MasterWebUIOverview setDiskUsedCapacity(String diskUsedCapacity) {
    mDiskUsedCapacity = diskUsedCapacity;
    return this;
  }

  /**
   * Sets free capacity.
   *
   * @param freeCapacity the free capacity
   * @return free capacity
   */
  public MasterWebUIOverview setFreeCapacity(String freeCapacity) {
    mFreeCapacity = freeCapacity;
    return this;
  }

  /**
   * Sets inconsistent path items.
   *
   * @param inconsistentPathItems the inconsistent path items
   * @return inconsistent path items
   */
  public MasterWebUIOverview setInconsistentPathItems(List<String> inconsistentPathItems) {
    mInconsistentPathItems = inconsistentPathItems;
    return this;
  }

  /**
   * Sets live worker nodes.
   *
   * @param liveWorkerNodes the live worker nodes
   * @return live worker nodes
   */
  public MasterWebUIOverview setLiveWorkerNodes(String liveWorkerNodes) {
    mLiveWorkerNodes = liveWorkerNodes;
    return this;
  }

  /**
   * Sets master node address.
   *
   * @param masterNodeAddress the master node address
   * @return master node address
   */
  public MasterWebUIOverview setMasterNodeAddress(String masterNodeAddress) {
    mMasterNodeAddress = masterNodeAddress;
    return this;
  }

  /**
   * Sets start time.
   *
   * @param startTime the start time
   * @return start time
   */
  public MasterWebUIOverview setStartTime(String startTime) {
    mStartTime = startTime;
    return this;
  }

  /**
   * Sets storage tier infos.
   *
   * @param storageTierInfos the storage tier infos
   * @return storage tier infos
   */
  public MasterWebUIOverview setStorageTierInfos(List<StorageTierInfo> storageTierInfos) {
    mStorageTierInfos = storageTierInfos;
    return this;
  }

  /**
   * Sets uptime.
   *
   * @param uptime the uptime
   * @return uptime
   */
  public MasterWebUIOverview setUptime(String uptime) {
    mUptime = uptime;
    return this;
  }

  /**
   * Sets used capacity.
   *
   * @param usedCapacity the used capacity
   * @return used capacity
   */
  public MasterWebUIOverview setUsedCapacity(String usedCapacity) {
    mUsedCapacity = usedCapacity;
    return this;
  }

  /**
   * Sets version.
   *
   * @param version the version
   * @return version
   */
  public MasterWebUIOverview setVersion(String version) {
    mVersion = version;
    return this;
  }

  /**
   * Sets inconsistent paths.
   *
   * @param size the size
   * @return inconsistent paths
   */
  public MasterWebUIOverview setInconsistentPaths(int size) {
    mInconsistentPaths = size;
    return this;
  }

  /**
   * Sets config check warn num.
   *
   * @param configCheckWarnNum the config check warn num
   * @return config check warn num
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
