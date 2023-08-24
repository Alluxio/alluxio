package alluxio.cli.fsadmin.report;

import alluxio.cli.fsadmin.FileSystemAdminShellUtils;
import alluxio.grpc.MasterInfo;
import alluxio.grpc.MasterVersion;
import alluxio.wire.BlockMasterInfo;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SummaryOutput {
    private String mMasterAddress;
    private int mWebPort;
    private int mRpcPort;
    private String mStarted;
    private String mUptime;
    private String mVersion;
    private boolean mSafeMode;

    private List<String> mZookeeperAddress;
    private boolean mZookeeper;
    private List<String> mRaftJournalAddress;
    private boolean mRaftJournal;
    private List<SerializableMasterVersion> mMasterVersions;

    private int mLiveWorkers;
    private int mLostWorkers;
    private Map<String, String> mTotalCapacityOnTiers;
    private Map<String, String> mUsedCapacityOnTiers;
    private String mFreeCapacity;

    private static class SerializableMasterVersion {
        @JsonProperty("Host")
        private String mHost;
        @JsonProperty("Port")
        private int mPort;
        @JsonProperty("State")
        private String mState;
        @JsonProperty("Version")
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
            this.mHost = host;
        }

        public int getPort() {
            return mPort;
        }

        public void setPort(int port) {
            this.mPort = port;
        }

        public String getState() {
            return mState;
        }

        public void setState(String state) {
            this.mState = state;
        }

        public String getVersion() {
            return mVersion;
        }

        public void setVersion(String version) {
            this.mVersion = version;
        }
    }

    public SummaryOutput(MasterInfo masterInfo, BlockMasterInfo blockMasterInfo, String dateFormatPattern) {
        // give values to internal properties
        mMasterAddress = masterInfo.getLeaderMasterAddress();
        mWebPort = masterInfo.getWebPort();
        mRpcPort = masterInfo.getRpcPort();
        mStarted = CommonUtils.convertMsToDate(masterInfo.getStartTimeMs(), dateFormatPattern);
        mUptime = CommonUtils.convertMsToClockTime(masterInfo.getUpTimeMs());
        mVersion = masterInfo.getVersion();
        mSafeMode = masterInfo.getSafeMode();

        mZookeeperAddress = masterInfo.getZookeeperAddressesList();
        mZookeeper = !mZookeeperAddress.isEmpty();
        mRaftJournal = masterInfo.getRaftJournal();
        if (mRaftJournal) {
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
        Map<String, Long> totalBytesOnTiers = new TreeMap<>(FileSystemAdminShellUtils::compareTierNames);
        totalBytesOnTiers.putAll(blockMasterInfo.getCapacityBytesOnTiers());
        for (Map.Entry<String, Long> totalBytesOnTier : totalBytesOnTiers.entrySet()) {
            mTotalCapacityOnTiers.put(totalBytesOnTier.getKey(), FormatUtils.getSizeFromBytes(totalBytesOnTier.getValue()));
        }
        Map<String, Long> usedBytesOnTiers = new TreeMap<>(FileSystemAdminShellUtils::compareTierNames);
        usedBytesOnTiers.putAll(blockMasterInfo.getUsedBytesOnTiers());
        for (Map.Entry<String, Long> usedBytesOnTier : usedBytesOnTiers.entrySet()) {
            mUsedCapacityOnTiers.put(usedBytesOnTier.getKey(), FormatUtils.getSizeFromBytes(usedBytesOnTier.getValue()));
        }
        mFreeCapacity = FormatUtils.getSizeFromBytes(blockMasterInfo.getFreeBytes());
    }

    public String getMasterAddress() {
        return mMasterAddress;
    }

    public void setMasterAddress(String masterAddress) {
        this.mMasterAddress = masterAddress;
    }

    public int getWebPort() {
        return mWebPort;
    }

    public void setWebPort(int webPort) {
        this.mWebPort = webPort;
    }

    public int getRpcPort() {
        return mRpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.mRpcPort = rpcPort;
    }

    public String getStarted() {
        return mStarted;
    }

    public void setStarted(String started) {
        this.mStarted = started;
    }

    public String getUptime() {
        return mUptime;
    }

    public void setUptime(String uptime) {
        this.mUptime = uptime;
    }

    public String getVersion() {
        return mVersion;
    }

    public void setVersion(String version) {
        this.mVersion = version;
    }

    public boolean ismSafeMode() {
        return mSafeMode;
    }

    public void setSafeMode(boolean safeMode) {
        this.mSafeMode = safeMode;
    }

    public List<String> getZookeeperAddress() {
        return mZookeeperAddress;
    }

    public void setZookeeperAddress(List<String> zookeeperAddress) {
        this.mZookeeperAddress = zookeeperAddress;
    }

    public boolean ismZookeeper() {
        return mZookeeper;
    }

    public void setZookeeper(boolean mZookeeper) {
        this.mZookeeper = mZookeeper;
    }

    public List<String> getRaftJournalAddress() {
        return mRaftJournalAddress;
    }

    public void setRaftJournalAddress(List<String> raftJournalAddress) {
        this.mRaftJournalAddress = raftJournalAddress;
    }

    public boolean ismRaftJournal() {
        return mRaftJournal;
    }

    public void setRaftJournal(boolean raftJournal) {
        this.mRaftJournal = raftJournal;
    }

    public List<SerializableMasterVersion> getMasterVersions() {
        return mMasterVersions;
    }

    public void setMasterVersions(List<SerializableMasterVersion> masterVersions) {
        this.mMasterVersions = masterVersions;
    }

    public int getLiveWorkers() {
        return mLiveWorkers;
    }

    public void setLiveWorkers(int liveWorkers) {
        this.mLiveWorkers = liveWorkers;
    }

    public int getLostWorkers() {
        return mLostWorkers;
    }

    public void setLostWorkers(int lostWorkers) {
        this.mLostWorkers = lostWorkers;
    }

    public String getFreeCapacity() {
        return mFreeCapacity;
    }

    public void setFreeCapacity(String freeCapacity) {
        this.mFreeCapacity = freeCapacity;
    }

    public Map<String, String> getTotalCapacityOnTiers() {
        return mTotalCapacityOnTiers;
    }

    public void setTotalCapacityOnTiers(Map<String, String> totalCapacityOnTiers) {
        this.mTotalCapacityOnTiers = totalCapacityOnTiers;
    }

    public Map<String, String> getUsedCapacityOnTiers() {
        return mUsedCapacityOnTiers;
    }

    public void setUsedCapacityOnTiers(Map<String, String> usedCapacityOnTiers) {
        this.mUsedCapacityOnTiers = usedCapacityOnTiers;
    }
}