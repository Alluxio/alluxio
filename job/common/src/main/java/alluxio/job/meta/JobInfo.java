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

package alluxio.job.meta;

import alluxio.job.JobConfig;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.util.CommonUtils;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The job information used by the job master internally.
 */
@ThreadSafe
public final class JobInfo implements Comparable<JobInfo> {
  private final long mId;
  private final JobConfig mJobConfig;
  private final ConcurrentHashMap<Integer, TaskInfo> mTaskIdToInfo;
  private final Consumer<JobInfo> mStatusChangeCallback;
  private volatile Status mStatus;
  private volatile long mLastStatusChangeMs;
  private volatile String mErrorMessage;
  private volatile String mResult;

  /**
   * Creates a new instance of {@link JobInfo}.
   *
   * @param id the job id
   * @param jobConfig the job configuration
   * @param statusChangeCallback the callback to invoke upon status change
   */
  public JobInfo(long id, JobConfig jobConfig, Consumer<JobInfo> statusChangeCallback) {
    mId = id;
    mJobConfig = Preconditions.checkNotNull(jobConfig);
    mTaskIdToInfo = new ConcurrentHashMap<>(4, 0.95f);
    mLastStatusChangeMs = CommonUtils.getCurrentMs();
    mErrorMessage = "";
    mStatus = Status.CREATED;
    mStatusChangeCallback = statusChangeCallback;
  }

  /**
   * {@inheritDoc}
   *
   * This method orders jobs using the time their status was last modified. If the status is
   * equal, they are compared by jobId
   */
  @Override
  public int compareTo(JobInfo other) {
    int res = Long.compare(mLastStatusChangeMs, other.mLastStatusChangeMs);
    if (res != 0) {
      return res;
    }
    // Order by jobId as a secondary measure
    return Long.compare(mId, other.mId);
  }

  /**
   * Registers a task.
   *
   * @param taskId the task id
   */
  public void addTask(int taskId) {
    TaskInfo oldValue = mTaskIdToInfo.putIfAbsent(taskId,
        new TaskInfo().setJobId(mId).setTaskId(taskId).setStatus(Status.CREATED).setErrorMessage("")
            .setResult(null));
    // the task is expected to not exist in the map.
    Preconditions.checkState(oldValue == null,
        String.format("JobId %d cannot add duplicate taskId %d", mId, taskId));
  }

  /**
   * @return the job id
   */
  public long getId() {
    return mId;
  }

  /**
   * @return the job configuration
   */
  public JobConfig getJobConfig() {
    return mJobConfig;
  }

  /**
   * @return the time when the job status was last changed (in milliseconds)
   */
  public long getLastStatusChangeMs() {
    return mLastStatusChangeMs;
  }

  /**
   * @param errorMessage the error message
   */
  public void setErrorMessage(String errorMessage) {
    mErrorMessage = errorMessage == null ? "" : errorMessage;
  }

  /**
   * @return the error message
   */
  public String getErrorMessage() {
    return mErrorMessage;
  }

  /**
   * @param taskId the task ID to get the task info for
   * @return the task info, or null if the task ID doesn't exist
   */
  public TaskInfo getTaskInfo(int taskId) {
    return mTaskIdToInfo.get(taskId);
  }

  /**
   * Sets the information of a task.
   *
   * @param taskId the task id
   * @param taskInfo the task information
   */
  public void setTaskInfo(int taskId, TaskInfo taskInfo) {
    mTaskIdToInfo.put(taskId, taskInfo);
  }

  /**
   * @return the list of task ids
   */
  public List<Integer> getTaskIdList() {
    return Lists.newArrayList(mTaskIdToInfo.keySet());
  }

  /**
   * Sets the status of a job.
   *
   * A job can only move from one status to another if the job hasn't already finished. If a job
   * is finished and the caller tries to change the status, this method is a no-op.
   *
   * @param status the job status
   */
  public void setStatus(Status status) {
    synchronized (this) {
      // this is synchronized to serialize all setStatus calls.
      if (mStatus.isFinished()) {
        return;
      }
      Status oldStatus = mStatus;
      mStatus = status;
      mLastStatusChangeMs = CommonUtils.getCurrentMs();
      if (mStatusChangeCallback != null && status != oldStatus) {
        mStatusChangeCallback.accept(this);
      }
    }
  }

  /**
   * @return the status of the job
   */
  public Status getStatus() {
    return mStatus;
  }

  /**
   * @param result the joined job result
   */
  public void setResult(String result) {
    mResult = result;
  }

  /**
   * @return the result of the job
   */
  public String getResult() {
    return mResult;
  }

  /**
   * @return the list of task information
   */
  public List<TaskInfo> getTaskInfoList() {
    return Lists.newArrayList(mTaskIdToInfo.values());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof JobInfo)) {
      return false;
    }

    JobInfo other = (JobInfo) o;
    return Objects.equal(mId, other.mId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mId);
  }
}
