package alluxio.cli.fsadmin.report;

import alluxio.cli.fsadmin.FileSystemAdminShellUtils;
import alluxio.grpc.MasterInfo;
import alluxio.grpc.MasterVersion;
import alluxio.wire.BlockMasterInfo;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;

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
        mMasterAddress = masterAddress;
    }

    public int getWebPort() {
        return mWebPort;
    }

    public void setWebPort(int webPort) {
        mWebPort = webPort;
    }

    public int getRpcPort() {
        return mRpcPort;
    }

    public void setRpcPort(int rpcPort) {
        mRpcPort = rpcPort;
    }

    public String getStarted() {
        return mStarted;
    }

    public void setStarted(String started) {
        mStarted = started;
    }

    public String getUptime() {
        return mUptime;
    }

    public void setUptime(String uptime) {
        mUptime = uptime;
    }

    public String getVersion() {
        return mVersion;
    }

    public void setVersion(String version) {
        mVersion = version;
    }

    public boolean ismSafeMode() {
        return mSafeMode;
    }

    public void setSafeMode(boolean safeMode) {
        mSafeMode = safeMode;
    }

    public List<String> getZookeeperAddress() {
        return mZookeeperAddress;
    }

    public void setZookeeperAddress(List<String> zookeeperAddress) {
        mZookeeperAddress = zookeeperAddress;
    }

    public boolean isUseZookeeper() {
        return mUseZookeeper;
    }

    public void setUseZookeeper(boolean useZookeeper) {
        mUseZookeeper = useZookeeper;
    }

    public List<String> getRaftJournalAddress() {
        return mRaftJournalAddress;
    }

    public void setRaftJournalAddress(List<String> raftJournalAddress) {
        mRaftJournalAddress = raftJournalAddress;
    }

    public boolean isUseRaftJournal() {
        return mUseRaftJournal;
    }

    public void setUseRaftJournal(boolean useRaftJournal) {
        mUseRaftJournal = useRaftJournal;
    }

    public List<SerializableMasterVersion> getMasterVersions() {
        return mMasterVersions;
    }

    public void setMasterVersions(List<SerializableMasterVersion> masterVersions) {
        mMasterVersions = masterVersions;
    }

    public int getLiveWorkers() {
        return mLiveWorkers;
    }

    public void setLiveWorkers(int liveWorkers) {
        mLiveWorkers = liveWorkers;
    }

    public int getLostWorkers() {
        return mLostWorkers;
    }

    public void setLostWorkers(int lostWorkers) {
        mLostWorkers = lostWorkers;
    }

    public String getFreeCapacity() {
        return mFreeCapacity;
    }

    public void setFreeCapacity(String freeCapacity) {
        mFreeCapacity = freeCapacity;
    }

    public Map<String, String> getTotalCapacityOnTiers() {
        return mTotalCapacityOnTiers;
    }

    public void setTotalCapacityOnTiers(Map<String, String> totalCapacityOnTiers) {
        mTotalCapacityOnTiers = totalCapacityOnTiers;
    }

    public Map<String, String> getUsedCapacityOnTiers() {
        return mUsedCapacityOnTiers;
    }

    public void setUsedCapacityOnTiers(Map<String, String> usedCapacityOnTiers) {
        mUsedCapacityOnTiers = usedCapacityOnTiers;
    }
}
