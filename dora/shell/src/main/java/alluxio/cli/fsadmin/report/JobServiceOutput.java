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

import alluxio.grpc.JobMasterStatus;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.JobWorkerHealth;
import alluxio.job.wire.StatusSummary;

import java.util.ArrayList;
import java.util.List;

/**
 * An output class, describing the job service.
 */
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
    private long mStartTime;
    private String mVersion;
    private String mRevision;

    public SerializableJobMasterStatus(JobMasterStatus jobMasterStatus) {
      mHost = jobMasterStatus.getMasterAddress().getHost();
      mPort = jobMasterStatus.getMasterAddress().getRpcPort();
      mState = jobMasterStatus.getState();
      mStartTime = jobMasterStatus.getStartTime();
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

    public long getStartTime() {
      return mStartTime;
    }

    public void setStartTime(long startTime) {
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

    public SerializableStatusSummary(StatusSummary statusSummary) {
      mStatus = statusSummary.getStatus().toString();
      mCount = statusSummary.getCount();
    }

    public String getStatus() {
      return mStatus;
    }

    public void setStatus(String status) {
      mStatus = status;
    }

    public long getCount() {
      return mCount;
    }

    public void setCount(long count) {
      mCount = count;
    }
  }

  private static class SerializableJobInfo {
    private long mLastUpdatedTime;
    private long mId;
    private String mName;
    private String mStatus;

    public SerializableJobInfo(JobInfo jobInfo) {
      mLastUpdatedTime = jobInfo.getLastUpdated();
      mId = jobInfo.getId();
      mName = jobInfo.getName();
      mStatus = jobInfo.getStatus().toString();
    }

    public long getLastUpdatedTime() {
      return mLastUpdatedTime;
    }

    public void setLastUpdatedTime(long lastUpdatedTime) {
      mLastUpdatedTime = lastUpdatedTime;
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

  /**
   * Creates a new instance of {@link JobServiceOutput}.
   *
   * @param allMasterStatus status of all masters
   * @param allWorkerHealth health info of all workers
   * @param jobServiceSummary summary of job service
   */
  public JobServiceOutput(List<JobMasterStatus> allMasterStatus,
                          List<JobWorkerHealth> allWorkerHealth,
                          JobServiceSummary jobServiceSummary) {
    mMasterStatus = new ArrayList<>();
    for (JobMasterStatus masterStatus : allMasterStatus) {
      mMasterStatus.add(new SerializableJobMasterStatus(masterStatus));
    }

    mWorkerHealth = new ArrayList<>();
    for (JobWorkerHealth workerHealth : allWorkerHealth) {
      mWorkerHealth.add(new SerializableWorkerHealth(workerHealth));
    }

    mStatusSummary = new ArrayList<>();
    for (StatusSummary statusSummary : jobServiceSummary.getSummaryPerStatus()) {
      mStatusSummary.add(new SerializableStatusSummary(statusSummary));
    }
    mRecentModifiedJobs = new ArrayList<>();
    mRecentFailedJobs = new ArrayList<>();
    mLongestRunningJobs = new ArrayList<>();
    for (JobInfo jobInfo : jobServiceSummary.getRecentActivities()) {
      mRecentModifiedJobs.add(new SerializableJobInfo(jobInfo));
    }
    for (JobInfo jobInfo : jobServiceSummary.getRecentFailures()) {
      mRecentFailedJobs.add(new SerializableJobInfo(jobInfo));
    }
    for (JobInfo jobInfo : jobServiceSummary.getLongestRunning()) {
      mLongestRunningJobs.add(new SerializableJobInfo(jobInfo));
    }
  }

  /**
   * Get master status.
   *
   * @return master status
   */
  public List<SerializableJobMasterStatus> getMasterStatus() {
    return mMasterStatus;
  }

  /**
   * Set master status.
   *
   * @param masterStatus master status
   */
  public void setMasterStatus(List<SerializableJobMasterStatus> masterStatus) {
    mMasterStatus = masterStatus;
  }

  /**
   * Get worker health info.
   *
   * @return worker health info
   */
  public List<SerializableWorkerHealth> getWorkerHealth() {
    return mWorkerHealth;
  }

  /**
   * Set worker health info.
   *
   * @param workerHealth worker health info
   */
  public void setWorkerHealth(List<SerializableWorkerHealth> workerHealth) {
    mWorkerHealth = workerHealth;
  }

  /**
   * Get status summary.
   *
   * @return status summary
   */
  public List<SerializableStatusSummary> getStatusSummary() {
    return mStatusSummary;
  }

  /**
   * Set status summary.
   *
   * @param statusSummary status summary
   */
  public void setStatusSummary(List<SerializableStatusSummary> statusSummary) {
    mStatusSummary = statusSummary;
  }

  /**
   * Get recent modified jobs.
   *
   * @return recent modified jobs
   */
  public List<SerializableJobInfo> getRecentModifiedJobs() {
    return mRecentModifiedJobs;
  }

  /**
   * Set recent modified jobs.
   *
   * @param recentModifiedJobs recent modified jobs
   */
  public void setRecentModifiedJobs(List<SerializableJobInfo> recentModifiedJobs) {
    mRecentModifiedJobs = recentModifiedJobs;
  }

  /**
   * Get recent failed jobs.
   *
   * @return recent failed jobs
   */
  public List<SerializableJobInfo> getRecentFailedJobs() {
    return mRecentFailedJobs;
  }

  /**
   * Set recent failed jobs.
   *
   * @param recentFailedJobs recent failed jobs
   */
  public void setRecentFailedJobs(List<SerializableJobInfo> recentFailedJobs) {
    mRecentFailedJobs = recentFailedJobs;
  }

  /**
   * Get the longest running jobs.
   *
   * @return longest running jobs
   */
  public List<SerializableJobInfo> getLongestRunningJobs() {
    return mLongestRunningJobs;
  }

  /**
   * Set the longest running jobs.
   *
   * @param longestRunningJobs longest running jobs
   */
  public void setLongestRunningJobs(List<SerializableJobInfo> longestRunningJobs) {
    mLongestRunningJobs = longestRunningJobs;
  }
}
