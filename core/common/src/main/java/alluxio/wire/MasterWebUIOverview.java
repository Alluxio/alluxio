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

import alluxio.grpc.ConfigStatus;
import alluxio.grpc.Scope;
import alluxio.util.webui.StorageTierInfo;

import com.google.common.base.MoreObjects;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI overview information.
 */
@NotThreadSafe
public final class MasterWebUIOverview implements Serializable {
  private static final long serialVersionUID = 4814640287979962750L;

  private boolean mDebug;
  private ConfigStatus mConfigCheckStatus;
  private int mConfigCheckWarnNum;
  private int mConfigCheckErrorNum;
  private List<StorageTierInfo> mStorageTierInfos;
  private Map<Scope, List<InconsistentProperty>> mConfigCheckErrors;
  private Map<Scope, List<InconsistentProperty>> mConfigCheckWarns;
  private String mCapacity;
  private String mClusterId;
  private String mDiskCapacity;
  private String mDiskFreeCapacity;
  private String mDiskUsedCapacity;
  private String mFreeCapacity;
  private List<String> mJournalDiskWarnings;
  private String mJournalCheckpointTimeWarning;
  private String mLiveWorkerNodes;
  private String mMasterNodeAddress;
  private String mReplicaBlockCount;
  private String mStartTime;
  private String mTotalPath;
  private String mUptime;
  private String mUsedCapacity;
  private String mUniqueBlockCount;
  private String mVersion;
  private String mRevision;
  private String mMasterRole;
  private String mLeaderId;

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
   * Gets cluster id.
   *
   * @return the cluster id
   */
  public String getClusterId() {
    return mClusterId;
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
  public ConfigStatus getConfigCheckStatus() {
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
   * @return the journal checkpoint time warning
   */
  public String getJournalCheckpointTimeWarning() {
    return mJournalCheckpointTimeWarning;
  }

  /**
   * @return the journal disk warnings
   */
  public List<String> getJournalDiskWarnings() {
    return mJournalDiskWarnings;
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
   * Gets revision.
   *
   * @return the revision
   */
  public String getRevision() {
    return mRevision;
  }

  /**
   * Gets config check warn num.
   *
   * @return the config check warn num
   */
  public int getConfigCheckWarnNum() {
    return mConfigCheckWarnNum;
  }

  /**
   * Gets unique block count.
   *
   * @return unique block count
   */
  public String getUniqueBlockCount() {
    return mUniqueBlockCount;
  }

  /**
   * Gets total path.
   *
   * @return total path
   */
  public String getTotalPath() {
    return mTotalPath;
  }

  /**
   * Gets replica block count.
   *
   * @return replica block count
   */
  public String getReplicaBlockCount() {
    return mReplicaBlockCount;
  }

  /**
   * Gets master role name.
   *
   * @return the master role name
   */
  public String getMasterRole() {
    return mMasterRole;
  }

  /**
   * Gets leader id.
   *
   * @return the leader id
   */
  public String getLeaderId() {
    return mLeaderId;
  }

  /**
   * Sets capacity.
   *
   * @param capacity the capacity
   * @return the updated {@link MasterWebUIOverview} object
   */
  public MasterWebUIOverview setCapacity(String capacity) {
    mCapacity = capacity;
    return this;
  }

  /**
   * Sets cluster id.
   *
   * @param clusterId the cluster id
   * @return the updated {@link MasterWebUIOverview} instance
   */
  public MasterWebUIOverview setClusterId(String clusterId) {
    mClusterId = clusterId;
    return this;
  }

  /**
   * Sets config check error num.
   *
   * @param configCheckErrorNum the config check error num
   * @return the updated {@link MasterWebUIOverview} object
   */
  public MasterWebUIOverview setConfigCheckErrorNum(int configCheckErrorNum) {
    mConfigCheckErrorNum = configCheckErrorNum;
    return this;
  }

  /**
   * Sets config check errors.
   *
   * @param configCheckErrors the config check errors
   * @return the updated {@link MasterWebUIOverview} object
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
   * @return the updated {@link MasterWebUIOverview} object
   */
  public MasterWebUIOverview setConfigCheckStatus(
      ConfigStatus configCheckStatus) {
    mConfigCheckStatus = configCheckStatus;
    return this;
  }

  /**
   * Sets config check warns.
   *
   * @param configCheckWarns the config check warns
   * @return the updated {@link MasterWebUIOverview} object
   */
  public MasterWebUIOverview setConfigCheckWarns(
      Map<Scope, List<InconsistentProperty>> configCheckWarns) {
    mConfigCheckWarns = configCheckWarns;
    return this;
  }

  /**
   * Sets debug.
   *
   * @param debug the debug
   * @return the updated {@link MasterWebUIOverview} object
   */
  public MasterWebUIOverview setDebug(boolean debug) {
    mDebug = debug;
    return this;
  }

  /**
   * Sets disk capacity.
   *
   * @param diskCapacity the disk capacity
   * @return the updated {@link MasterWebUIOverview} object
   */
  public MasterWebUIOverview setDiskCapacity(String diskCapacity) {
    mDiskCapacity = diskCapacity;
    return this;
  }

  /**
   * Sets disk free capacity.
   *
   * @param diskFreeCapacity the disk free capacity
   * @return the updated {@link MasterWebUIOverview} object
   */
  public MasterWebUIOverview setDiskFreeCapacity(String diskFreeCapacity) {
    mDiskFreeCapacity = diskFreeCapacity;
    return this;
  }

  /**
   * Sets disk used capacity.
   *
   * @param diskUsedCapacity the disk used capacity
   * @return the updated {@link MasterWebUIOverview} object
   */
  public MasterWebUIOverview setDiskUsedCapacity(String diskUsedCapacity) {
    mDiskUsedCapacity = diskUsedCapacity;
    return this;
  }

  /**
   * Sets free capacity.
   *
   * @param freeCapacity the free capacity
   * @return the updated {@link MasterWebUIOverview} object
   */
  public MasterWebUIOverview setFreeCapacity(String freeCapacity) {
    mFreeCapacity = freeCapacity;
    return this;
  }

  /**
   * @param journalCheckpointTimeWarning the journal checkpoint time warning
   * @return the updated {@link MasterWebUIOverview} object
   */
  public MasterWebUIOverview setJournalCheckpointTimeWarning(String journalCheckpointTimeWarning) {
    mJournalCheckpointTimeWarning = journalCheckpointTimeWarning;
    return this;
  }

  /**
    * @param journalDiskWarnings the list of journal disk warnings
   * @return the updated {@link MasterWebUIOverview} object
   */
  public MasterWebUIOverview setJournalDiskWarnings(List<String> journalDiskWarnings) {
    mJournalDiskWarnings = journalDiskWarnings;
    return this;
  }

  /**
   * Sets live worker nodes.
   *
   * @param liveWorkerNodes the live worker nodes
   * @return the updated {@link MasterWebUIOverview} object
   */
  public MasterWebUIOverview setLiveWorkerNodes(String liveWorkerNodes) {
    mLiveWorkerNodes = liveWorkerNodes;
    return this;
  }

  /**
   * Sets master node address.
   *
   * @param masterNodeAddress the master node address
   * @return the master webui overview
   */
  public MasterWebUIOverview setMasterNodeAddress(String masterNodeAddress) {
    mMasterNodeAddress = masterNodeAddress;
    return this;
  }

  /**
   * Sets start time.
   *
   * @param startTime the start time
   * @return the master webui overview
   */
  public MasterWebUIOverview setStartTime(String startTime) {
    mStartTime = startTime;
    return this;
  }

  /**
   * Sets storage tier infos.
   *
   * @param storageTierInfos the storage tier infos
   * @return the master webui overview
   */
  public MasterWebUIOverview setStorageTierInfos(List<StorageTierInfo> storageTierInfos) {
    mStorageTierInfos = storageTierInfos;
    return this;
  }

  /**
   * Sets uptime.
   *
   * @param uptime the uptime
   * @return the master webui overview
   */
  public MasterWebUIOverview setUptime(String uptime) {
    mUptime = uptime;
    return this;
  }

  /**
   * Sets used capacity.
   *
   * @param usedCapacity the used capacity
   * @return the master webui overview
   */
  public MasterWebUIOverview setUsedCapacity(String usedCapacity) {
    mUsedCapacity = usedCapacity;
    return this;
  }

  /**
   * Sets version.
   *
   * @param version the version
   * @return the master webui overview
   */
  public MasterWebUIOverview setVersion(String version) {
    mVersion = version;
    return this;
  }

  /**
   * Sets revision.
   *
   * @param revision the revision
   * @return the master webui overview
   */
  public MasterWebUIOverview setRevision(String revision) {
    mRevision = revision;
    return this;
  }

  /**
   * Sets config check warn num.
   *
   * @param configCheckWarnNum the config check warn num
   * @return the master webui overview
   */
  public MasterWebUIOverview setConfigCheckWarnNum(int configCheckWarnNum) {
    mConfigCheckWarnNum = configCheckWarnNum;
    return this;
  }

  /**
   * Sets unique block count.
   *
   * @param uniqueBlockCount the unique block count
   * @return the master webui overview
   */
  public MasterWebUIOverview setUniqueBlockCount(String uniqueBlockCount) {
    mUniqueBlockCount = uniqueBlockCount;
    return this;
  }

  /**
   * Sets total path.
   *
   * @param totalPath the total path
   * @return the master webui overview
   */
  public MasterWebUIOverview setTotalPath(String totalPath) {
    mTotalPath = totalPath;
    return this;
  }

  /**
   * Sets replica block count.
   *
   * @param replicaBlockCount the replica block count
   * @return the master webui overview
   */
  public MasterWebUIOverview setReplicaBlockCount(String replicaBlockCount) {
    mReplicaBlockCount = replicaBlockCount;
    return this;
  }

  /**
   * Sets the master role name.
   *
   * @param roleName the master role name
   * @return the master webui overview
   */
  public MasterWebUIOverview setMasterRole(String roleName) {
    mMasterRole = roleName;
    return this;
  }

  /**
   * Sets the leader id.
   *
   * @param leaderId the leader id
   * @return the master webui overview
   */
  public MasterWebUIOverview setLeaderId(String leaderId) {
    mLeaderId = leaderId;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("capacity", mCapacity)
        .add("clusterId", mClusterId)
        .add("configCheckErrorNum", mConfigCheckErrorNum)
        .add("configCheckErrors", mConfigCheckErrors).add("configCheckStatus", mConfigCheckStatus)
        .add("configCheckWarnNum", mConfigCheckWarnNum)
        .add("configCheckWarns", mConfigCheckWarns)
        .add("debug", mDebug)
        .add("diskCapacity", mDiskCapacity).add("diskFreeCapacity", mDiskFreeCapacity)
        .add("diskUsedCapacity", mDiskUsedCapacity).add("freeCapacity", mFreeCapacity)
        .add("liveWorkerNodes", mLiveWorkerNodes).add("masterNodeAddress", mMasterNodeAddress)
        .add("replicaBlockCount", mReplicaBlockCount)
        .add("startTime", mStartTime).add("storageTierInfos", mStorageTierInfos)
        .add("totalPath", mTotalPath).add("uniqueBlockCount", mUniqueBlockCount)
        .add("uptime", mUptime).add("usedCapacity", mUsedCapacity)
        .add("version", mVersion).add("revision", mRevision)
        .add("leaderId", mLeaderId)
        .add("masterRole", mMasterRole)
        .toString();
  }
}
