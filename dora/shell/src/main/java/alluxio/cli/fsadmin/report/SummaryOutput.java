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

package alluxio.cli.fsadmin.report;

import alluxio.cli.fsadmin.FileSystemAdminShellUtils;
import alluxio.grpc.MasterInfo;
import alluxio.grpc.MasterVersion;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.wire.BlockMasterInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * An output class, describing the summary.
 */
public class SummaryOutput {
  private String mMasterAddress;
  private int mWebPort;
  private int mRpcPort;
  private String mStarted;
  private String mUptime;
  private String mVersion;
  private boolean mSafeMode;

  private List<String> mZookeeperAddress;
  private boolean mUseZookeeper;
  private List<String> mRaftJournalAddress;
  private boolean mUseRaftJournal;
  private List<SerializableMasterVersion> mMasterVersions;

  private int mLiveWorkers;
  private int mLostWorkers;
  private Map<String, String> mTotalCapacityOnTiers;
  private Map<String, String> mUsedCapacityOnTiers;
  private String mFreeCapacity;

  private static class SerializableMasterVersion {
    private String mHost;
    private int mPort;
    private String mState;
    private String mVersion;

    public SerializableMasterVersion(MasterVersion masterVersion) {
      mHost = masterVersion.getAddresses().getHost();
      mPort = masterVersion.getAddresses().getRpcPort();
      mState = masterVersion.getState();
      mVersion = masterVersion.getVersion();
    }

    public String getHost() {
      return mHost;
    }

    public void setHost(String host) {
      mHost = host;
    }

    public int getPort() {
      return mPort;
    }

    public void setPort(int port) {
      mPort = port;
    }

    public String getState() {
      return mState;
    }

    public void setState(String state) {
      mState = state;
    }

    public String getVersion() {
      return mVersion;
    }

    public void setVersion(String version) {
      mVersion = version;
    }
  }

  /**
   * Creates a new instance of {@link SummaryOutput}.
   *
   * @param masterInfo given master info
   * @param blockMasterInfo given block master info
   * @param dateFormatPattern specify the pattern of dates
   */
  public SummaryOutput(MasterInfo masterInfo, BlockMasterInfo blockMasterInfo,
        String dateFormatPattern) {
    // give values to internal properties
    mMasterAddress = masterInfo.getLeaderMasterAddress();
    mWebPort = masterInfo.getWebPort();
    mRpcPort = masterInfo.getRpcPort();
    mStarted = CommonUtils.convertMsToDate(masterInfo.getStartTimeMs(), dateFormatPattern);
    mUptime = CommonUtils.convertMsToClockTime(masterInfo.getUpTimeMs());
    mVersion = masterInfo.getVersion();
    mSafeMode = masterInfo.getSafeMode();

    mZookeeperAddress = masterInfo.getZookeeperAddressesList();
    mUseZookeeper = !mZookeeperAddress.isEmpty();
    mUseRaftJournal = masterInfo.getRaftJournal();
    if (mUseRaftJournal) {
      mRaftJournalAddress = masterInfo.getRaftAddressList();
    } else {
      mRaftJournalAddress = new ArrayList<>();
    }
    mMasterVersions = new ArrayList<>();
    for (MasterVersion masterVersion : masterInfo.getMasterVersionsList()) {
      mMasterVersions.add(new SerializableMasterVersion(masterVersion));
    }

    mLiveWorkers = blockMasterInfo.getLiveWorkerNum();
    mLostWorkers = blockMasterInfo.getLostWorkerNum();

    mTotalCapacityOnTiers = new TreeMap<>();
    mUsedCapacityOnTiers = new TreeMap<>();
    Map<String, Long> totalBytesOnTiers =
        new TreeMap<>(FileSystemAdminShellUtils::compareTierNames);
    totalBytesOnTiers.putAll(blockMasterInfo.getCapacityBytesOnTiers());
    for (Map.Entry<String, Long> totalBytesOnTier : totalBytesOnTiers.entrySet()) {
      mTotalCapacityOnTiers.put(totalBytesOnTier.getKey(),
          FormatUtils.getSizeFromBytes(totalBytesOnTier.getValue()));
    }
    Map<String, Long> usedBytesOnTiers =
        new TreeMap<>(FileSystemAdminShellUtils::compareTierNames);
    usedBytesOnTiers.putAll(blockMasterInfo.getUsedBytesOnTiers());
    for (Map.Entry<String, Long> usedBytesOnTier : usedBytesOnTiers.entrySet()) {
      mUsedCapacityOnTiers.put(usedBytesOnTier.getKey(),
          FormatUtils.getSizeFromBytes(usedBytesOnTier.getValue()));
    }
    mFreeCapacity = FormatUtils.getSizeFromBytes(blockMasterInfo.getFreeBytes());
  }

  /**
   * Get master address.
   *
   * @return master address
   */
  public String getMasterAddress() {
    return mMasterAddress;
  }

  /**
   * Set master address.
   *
   * @param masterAddress master address
   */
  public void setMasterAddress(String masterAddress) {
    mMasterAddress = masterAddress;
  }

  /**
   * Get web port.
   *
   * @return web port
   */
  public int getWebPort() {
    return mWebPort;
  }

  /**
   * Set web port.
   *
   * @param webPort web port
   */
  public void setWebPort(int webPort) {
    mWebPort = webPort;
  }

  /**
   * Get rpc port.
   *
   * @return rpc port
   */
  public int getRpcPort() {
    return mRpcPort;
  }

  /**
   * Set rpc port.
   *
   * @param rpcPort rpc port
   */
  public void setRpcPort(int rpcPort) {
    mRpcPort = rpcPort;
  }

  /**
   * Get started time.
   *
   * @return started time
   */
  public String getStarted() {
    return mStarted;
  }

  /**
   * Set started time.
   *
   * @param started started time
   */
  public void setStarted(String started) {
    mStarted = started;
  }

  /**
   * Get time running.
   *
   * @return time running
   */
  public String getUptime() {
    return mUptime;
  }

  /**
   * Set time running.
   *
   * @param uptime time running
   */
  public void setUptime(String uptime) {
    mUptime = uptime;
  }

  /**
   * Get Alluxio version.
   *
   * @return Alluxio version
   */
  public String getVersion() {
    return mVersion;
  }

  /**
   * Set Alluxio version.
   *
   * @param version Alluxio version
   */
  public void setVersion(String version) {
    mVersion = version;
  }

  /**
   * Get if in safe mode.
   *
   * @return if in safe mode
   */
  public boolean isSafeMode() {
    return mSafeMode;
  }

  /**
   * Set if in safe mode.
   *
   * @param safeMode if in safe mode
   */
  public void setSafeMode(boolean safeMode) {
    mSafeMode = safeMode;
  }

  /**
   * Get zookeeper address.
   *
   * @return zookeeper address
   */
  public List<String> getZookeeperAddress() {
    return mZookeeperAddress;
  }

  /**
   * Set zookeeper address.
   *
   * @param zookeeperAddress zookeeper address
   */
  public void setZookeeperAddress(List<String> zookeeperAddress) {
    mZookeeperAddress = zookeeperAddress;
  }

  /**
   * Get if zookeeper is running.
   *
   * @return if zookeeper is running
   */
  public boolean isUseZookeeper() {
    return mUseZookeeper;
  }

  /**
   * Set if zookeeper is running.
   *
   * @param useZookeeper if zookeeper is running
   */
  public void setUseZookeeper(boolean useZookeeper) {
    mUseZookeeper = useZookeeper;
  }

  /**
   * Get raft journal address.
   *
   * @return raft journal address
   */
  public List<String> getRaftJournalAddress() {
    return mRaftJournalAddress;
  }

  /**
   * Set raft journal address.
   *
   * @param raftJournalAddress raft journal address
   */
  public void setRaftJournalAddress(List<String> raftJournalAddress) {
    mRaftJournalAddress = raftJournalAddress;
  }

  /**
   * Get if Alluxio uses raft journal.
   *
   * @return if Alluxio uses raft journal
   */
  public boolean isUseRaftJournal() {
    return mUseRaftJournal;
  }

  /**
   * Set if Alluxio uses raft journal.
   *
   * @param useRaftJournal if Alluxio uses raft journal
   */
  public void setUseRaftJournal(boolean useRaftJournal) {
    mUseRaftJournal = useRaftJournal;
  }

  /**
   * Get master versions.
   *
   * @return master versions
   */
  public List<SerializableMasterVersion> getMasterVersions() {
    return mMasterVersions;
  }

  /**
   * Set master versions.
   *
   * @param masterVersions master versions
   */
  public void setMasterVersions(List<SerializableMasterVersion> masterVersions) {
    mMasterVersions = masterVersions;
  }

  /**
   * Get live workers.
   *
   * @return live workers
   */
  public int getLiveWorkers() {
    return mLiveWorkers;
  }

  /**
   * Set live workers.
   *
   * @param liveWorkers live workers
   */
  public void setLiveWorkers(int liveWorkers) {
    mLiveWorkers = liveWorkers;
  }

  /**
   * Get lost workers.
   *
   * @return lost workers
   */
  public int getLostWorkers() {
    return mLostWorkers;
  }

  /**
   * Set lost workers.
   *
   * @param lostWorkers lost workers
   */
  public void setLostWorkers(int lostWorkers) {
    mLostWorkers = lostWorkers;
  }

  /**
   * Get free capacity.
   *
   * @return free capacity
   */
  public String getFreeCapacity() {
    return mFreeCapacity;
  }

  /**
   * Set free capacity.
   *
   * @param freeCapacity free capacity
   */
  public void setFreeCapacity(String freeCapacity) {
    mFreeCapacity = freeCapacity;
  }

  /**
   * Get capacity by tiers.
   *
   * @return capacity by tiers
   */
  public Map<String, String> getTotalCapacityOnTiers() {
    return mTotalCapacityOnTiers;
  }

  /**
   * Get capacity by tiers.
   *
   * @param totalCapacityOnTiers capacity by tiers
   */
  public void setTotalCapacityOnTiers(Map<String, String> totalCapacityOnTiers) {
    mTotalCapacityOnTiers = totalCapacityOnTiers;
  }

  /**
   * Get used capacity by tiers.
   *
   * @return used capacity by tiers
   */
  public Map<String, String> getUsedCapacityOnTiers() {
    return mUsedCapacityOnTiers;
  }

  /**
   * Set used capacity by tiers.
   *
   * @param usedCapacityOnTiers used capacity by tiers
   */
  public void setUsedCapacityOnTiers(Map<String, String> usedCapacityOnTiers) {
    mUsedCapacityOnTiers = usedCapacityOnTiers;
  }
}
