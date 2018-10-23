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

package alluxio.job.wire;

import alluxio.job.JobConfig;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The job descriptor.
 */
@NotThreadSafe
public final class JobInfo {
  private long mJobId;
  private JobConfig mJobConfig;
  private String mErrorMessage;
  private List<TaskInfo> mTaskInfoList;
  private Status mStatus;
  private String mResult;

  /**
   * Default constructor.
   */
  public JobInfo() {}

  /**
   * Constructs the job info from the job master's internal representation of job info.
   *
   * @param jobInfo the job master's internal job info
   */
  public JobInfo(alluxio.job.meta.JobInfo jobInfo) {
    mJobId = jobInfo.getId();
    mJobConfig = jobInfo.getJobConfig();
    mErrorMessage = jobInfo.getErrorMessage();
    mTaskInfoList = Lists.newArrayList();
    mStatus = Status.valueOf(jobInfo.getStatus().name());
    mResult = jobInfo.getResult();
    for (TaskInfo taskInfo : jobInfo.getTaskInfoList()) {
      mTaskInfoList.add(taskInfo);
    }
  }

  /**
   * Constructs a new instance of {@link JobInfo} from a Thrift object.
   *
   * @param jobInfo the thrift object
   * @throws IOException if the deserialization fails
   */
  public JobInfo(alluxio.thrift.JobInfo jobInfo) throws IOException {
    mJobId = jobInfo.getId();
    mErrorMessage = jobInfo.getErrorMessage();
    mTaskInfoList = new ArrayList<>();
    for (alluxio.thrift.TaskInfo taskInfo : jobInfo.getTaskInfos()) {
      mTaskInfoList.add(new TaskInfo(taskInfo));
    }
    mStatus = Status.valueOf(jobInfo.getStatus().name());
    mResult = jobInfo.getResult();
  }

  /**
   * @param jobId the job id
   */
  public void setJobId(long jobId) {
    mJobId = jobId;
  }

  /**
   * @return the job id
   */
  public long getJobId() {
    return mJobId;
  }

  /**
   * @param jobConfig the job config
   */
  public void setJobConfig(JobConfig jobConfig) {
    mJobConfig = jobConfig;
  }

  /**
   * @return the job config
   */
  public JobConfig getJobConfig() {
    return mJobConfig;
  }

  /**
   * Sets the job result.
   *
   * @param result the job result
   */
  public void setResult(String result) {
    mResult = result;
  }

  /**
   * @return the job result
   */
  public String getResult() {
    return mResult;
  }

  /**
   * Sets the job status.
   *
   * @param status the job status
   */
  public void setStatus(Status status) {
    mStatus = status;
  }

  /**
   * @return the job status
   */
  public Status getStatus() {
    return mStatus;
  }

  /**
   * @param taskInfoList the list of task descriptors
   */
  public void setTaskInfoList(List<TaskInfo> taskInfoList) {
    mTaskInfoList = Preconditions.checkNotNull(taskInfoList);
  }

  /**
   * @return the list of task descriptors
   */
  public List<TaskInfo> getTaskInfoList() {
    return mTaskInfoList;
  }

  /**
   * @param errorMessage the error message
   */
  public void setErrorMessage(String errorMessage) {
    mErrorMessage = errorMessage;
  }

  /**
   * @return the error message
   */
  public String getErrorMessage() {
    return mErrorMessage;
  }

  /**
   * @return thrift representation of the job info
   * @throws IOException if serialization fails
   */
  public alluxio.thrift.JobInfo toThrift() throws IOException {
    List<alluxio.thrift.TaskInfo> taskInfos = new ArrayList<>();
    for (TaskInfo taskInfo : mTaskInfoList) {
      taskInfos.add(taskInfo.toThrift());
    }
    return new alluxio.thrift.JobInfo(mJobId, mErrorMessage, taskInfos, mStatus.toThrift(),
        mResult);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof JobInfo)) {
      return false;
    }
    JobInfo that = (JobInfo) o;
    return Objects.equal(mJobId, that.mJobId)
        && Objects.equal(mJobConfig, that.mJobConfig)
        && Objects.equal(mErrorMessage, that.mErrorMessage)
        && Objects.equal(mTaskInfoList, that.mTaskInfoList)
        && Objects.equal(mStatus, that.mStatus)
        && Objects.equal(mResult, that.mResult);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mJobId, mJobConfig, mErrorMessage, mTaskInfoList, mStatus, mResult);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("jobId", mJobId)
        .add("jobConfig", mJobConfig)
        .add("errorMessage", mErrorMessage)
        .add("taskInfoList", mTaskInfoList)
        .add("status", mStatus)
        .add("result", mResult)
        .toString();
  }
}
