package alluxio.cli.fsadmin.report;

import alluxio.client.job.JobMasterClient;
import alluxio.grpc.JobMasterStatus;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.job.wire.StatusSummary;
import alluxio.util.CommonUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JobServiceOutput {
    private List<SerializableJobMasterStatus> mMasterStatus;
    private List<SerializableWorkerHealth> mWorkerHealth;
    private List<SerializableStatusSummary> mStatusSummary;
    private List<SerializableJobInfo> mRecentModifiedJobs;
    private List<SerializableJobInfo> mRecentFailedJobs;
    private List<SerializableJobInfo> mLongestRunningJobs;

    private static class SerializableJobMasterStatus {
        private String mHost;
        private int mPort;
        private String mState;
        private String mStartTime;
        private String mVersion;
        private String mRevision;

        public SerializableJobMasterStatus(JobMasterStatus jobMasterStatus, String dateFormat) {
            mHost = jobMasterStatus.getMasterAddress().getHost();
            mPort = jobMasterStatus.getMasterAddress().getRpcPort();
            mState = jobMasterStatus.getState();
            mStartTime = CommonUtils.convertMsToDate(jobMasterStatus.getStartTime(), dateFormat);
            mVersion = jobMasterStatus.getVersion().getVersion();
            mRevision = jobMasterStatus.getVersion().getRevision();
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

        public String getStartTime() {
            return mStartTime;
        }

        public void setStartTime(String startTime) {
            mStartTime = startTime;
        }

        public String getVersion() {
            return mVersion;
        }

        public void setVersion(String version) {
            mVersion = version;
        }

        public String getRevision() {
            return mRevision;
        }

        public void setRevision(String revision) {
            mRevision = revision;
        }
    }

    private static class SerializableWorkerHealth {
        private String mHost;
        private String mVersion;
        private String mRevision;
        private int mTaskPoolSize;
        private int mUnfinishedTasks;
        private int mActiveTasks;
        private List<Double> mLoadAverage;

        public SerializableWorkerHealth(JobWorkerHealth workerHealth) {
            mHost = workerHealth.getHostname();
            mVersion = workerHealth.getVersion().getVersion();
            mRevision = workerHealth.getVersion().getRevision();
            mTaskPoolSize = workerHealth.getTaskPoolSize();
            mUnfinishedTasks = workerHealth.getUnfinishedTasks();
            mActiveTasks = workerHealth.getNumActiveTasks();
            mLoadAverage = workerHealth.getLoadAverage();
        }
        public String getHost() {
            return mHost;
        }

        public void setHost(String host) {
            mHost = host;
        }

        public String getVersion() {
            return mVersion;
        }

        public void setVersion(String version) {
            mVersion = version;
        }

        public String getRevision() {
            return mRevision;
        }

        public void setRevision(String revision) {
            mRevision = revision;
        }

        public int getTaskPoolSize() {
            return mTaskPoolSize;
        }

        public void setTaskPoolSize(int taskPoolSize) {
            mTaskPoolSize = taskPoolSize;
        }

        public int getUnfinishedTasks() {
            return mUnfinishedTasks;
        }

        public void setUnfinishedTasks(int unfinishedTasks) {
            mUnfinishedTasks = unfinishedTasks;
        }

        public int getActiveTasks() {
            return mActiveTasks;
        }

        public void setActiveTasks(int activeTasks) {
            mActiveTasks = activeTasks;
        }

        public List<Double> getLoadAverage() {
            return mLoadAverage;
        }

        public void setLoadAverage(List<Double> loadAverage) {
            mLoadAverage = loadAverage;
        }

    }

    private static class SerializableStatusSummary {
        private String mStatus;
        private long mCount;

        public SerializableStatusSummary(StatusSummary statusSummary){
            mStatus = statusSummary.getStatus().toString();
            mCount = statusSummary.getCount();
        }

        public String getStatus() {
            return mStatus;
        }

        public void setStatus(String status) {
            mStatus = status;
        }

        public long getCount(){
            return mCount;
        }

        public void setCount(long count){
            mCount = count;
        }
    }

    private static class SerializableJobInfo {
        private String mTimestamp;
        private long mId;
        private String mName;
        private String mStatus;
        public SerializableJobInfo(JobInfo jobInfo, String dateFormat) {
            mTimestamp = CommonUtils.convertMsToDate(jobInfo.getLastUpdated(), dateFormat);
            mId = jobInfo.getId();
            mName = jobInfo.getName();
            mStatus = jobInfo.getStatus().toString();
        }

        public String getTimestamp() {
            return mTimestamp;
        }

        public void setTimestamp(String timestamp) {
            mTimestamp = timestamp;
        }

        public long getId() {
            return mId;
        }

        public void setId(long id) {
            mId = id;
        }

        public String getName() {
            return mName;
        }

        public void setName(String name) {
            mName = name;
        }

        public String getStatus() {
            return mStatus;
        }

        public void setStatus(String status) {
            mStatus = status;
        }
    }

    public JobServiceOutput(JobMasterClient jobMasterClient, String dateFormat) throws IOException {
        mMasterStatus = new ArrayList<>();
        for (JobMasterStatus masterStatus : jobMasterClient.getAllMasterStatus()) {
            mMasterStatus.add(new SerializableJobMasterStatus(masterStatus, dateFormat));
        }

        mWorkerHealth = new ArrayList<>();
        for (JobWorkerHealth workerHealth : jobMasterClient.getAllWorkerHealth()) {
            mWorkerHealth.add(new SerializableWorkerHealth(workerHealth));
        }

        JobServiceSummary jobServiceSummary = jobMasterClient.getJobServiceSummary();
        mStatusSummary = new ArrayList<>();
        for (StatusSummary statusSummary : jobServiceSummary.getSummaryPerStatus()) {
            mStatusSummary.add(new SerializableStatusSummary(statusSummary));
        }

        mRecentModifiedJobs = new ArrayList<>();
        mRecentFailedJobs = new ArrayList<>();
        mLongestRunningJobs = new ArrayList<>();
        for (JobInfo jobInfo : jobServiceSummary.getRecentActivities()){
            mRecentModifiedJobs.add(new SerializableJobInfo(jobInfo, dateFormat));
        }
        for (JobInfo jobInfo : jobServiceSummary.getRecentFailures()){
            mRecentFailedJobs.add(new SerializableJobInfo(jobInfo, dateFormat));
        }
        for (JobInfo jobInfo : jobServiceSummary.getLongestRunning()) {
            mLongestRunningJobs.add(new SerializableJobInfo(jobInfo, dateFormat));
        }
    }

    public List<SerializableJobMasterStatus> getMasterStatus() {
        return mMasterStatus;
    }

    public void setMasterStatus(List<SerializableJobMasterStatus> masterStatus) {
        mMasterStatus = masterStatus;
    }

    public List<SerializableWorkerHealth> getWorkerHealth() {
        return mWorkerHealth;
    }

    public void setWorkerHealth(List<SerializableWorkerHealth> workerHealth) {
        mWorkerHealth = workerHealth;
    }

    public List<SerializableStatusSummary> getStatusSummary() {
        return mStatusSummary;
    }

    public void setStatusSummary(List<SerializableStatusSummary> statusSummary) {
        mStatusSummary = statusSummary;
    }

    public List<SerializableJobInfo> getRecentModifiedJobs() {
        return mRecentModifiedJobs;
    }

    public void setRecentModifiedJobs(List<SerializableJobInfo> recentModifiedJobs) {
        mRecentModifiedJobs = recentModifiedJobs;
    }

    public List<SerializableJobInfo> getRecentFailedJobs() {
        return mRecentFailedJobs;
    }

    public void setRecentFailedJobs(List<SerializableJobInfo> recentFailedJobs) {
        mRecentFailedJobs = recentFailedJobs;
    }

    public List<SerializableJobInfo> getLongestRunningJobs() {
        return mLongestRunningJobs;
    }

    public void setLongestRunningJobs(List<SerializableJobInfo> longestRunningJobs) {
        mLongestRunningJobs = longestRunningJobs;
    }
}
