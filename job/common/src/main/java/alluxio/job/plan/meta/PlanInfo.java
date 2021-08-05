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

package alluxio.job.plan.meta;

import alluxio.job.JobConfig;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The job information used by the job master internally.
 */
@ThreadSafe
public final class PlanInfo implements Comparable<PlanInfo> {
  private static final Logger LOG = LoggerFactory.getLogger(PlanInfo.class);

  private final long mId;
  private final JobConfig mJobConfig;
  private final ConcurrentHashMap<Long, TaskInfo> mTaskIdToInfo;
  private final Consumer<PlanInfo> mStatusChangeCallback;
  private volatile Status mStatus;
  private volatile long mLastStatusChangeMs;
  private volatile String mErrorType;
  private volatile String mErrorMessage;
  private volatile String mResult;

  /**
   * Creates a new instance of {@link PlanInfo}.
   *
   * @param id the job id
   * @param jobConfig the job configuration
   * @param statusChangeCallback the callback to invoke upon status change
   */
  public PlanInfo(long id, JobConfig jobConfig, Consumer<PlanInfo> statusChangeCallback) {
    mId = id;
    mJobConfig = Preconditions.checkNotNull(jobConfig);
    mTaskIdToInfo = new ConcurrentHashMap<>(4, 0.95f);
    mLastStatusChangeMs = CommonUtils.getCurrentMs();
    mErrorType = "";
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
  public int compareTo(PlanInfo other) {
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
   * @param workerInfo the worker info
   * @param args the arguments
   */
  public void addTask(long taskId, WorkerInfo workerInfo, Object args) {
    TaskInfo oldValue = mTaskIdToInfo.putIfAbsent(taskId,
        new TaskInfo(mId, taskId, Status.CREATED, workerInfo.getAddress(), args));
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
   * @param errorType the error type
   */
  public void setErrorType(String errorType) {
    mErrorType = errorType == null ? "" : errorType;
  }

  /**
   * @return the error type
   */
  public String getErrorType() {
    return mErrorType;
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
  public TaskInfo getTaskInfo(long taskId) {
    return mTaskIdToInfo.get(taskId);
  }

  /**
   * Sets the information of a task.
   *
   * @param taskId the task id
   * @param taskInfo the task information
   */
  public void setTaskInfo(long taskId, TaskInfo taskInfo) {
    TaskInfo oldTaskInfo = mTaskIdToInfo.get(taskId);

    // Keep around the task description to avoid regenerating

    if (oldTaskInfo != null) {
      taskInfo.setDescription(oldTaskInfo.getDescription());
    }

    mTaskIdToInfo.put(taskId, taskInfo);
  }

  /**
   * @return the list of task ids
   */
  public List<Long> getTaskIdList() {
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
      if (status != oldStatus) {
        // status changed
        if (status.isFinished()) {
          if (status.equals(Status.COMPLETED)) {
            // for completed jobs
            LOG.debug("Job completed, Id={} Config={}",
                oldStatus.name(), status.name(),
                getId(), getJobConfig());
          } else {
            // for failed and cancelled jobs
            LOG.info("Job status changed from {} to {}, Id={} Config={} Error={}",
                oldStatus.name(), status.name(),
                getId(), getJobConfig(), getErrorMessage());
          }
        }

        if (status.equals(Status.FAILED)
            && (getErrorType().isEmpty() || getErrorMessage().isEmpty())) {
          LOG.warn("Job set to failed without given an error type or message, Id={} Config={}",
              getId(), getJobConfig());
        }

        mLastStatusChangeMs = CommonUtils.getCurrentMs();
        if (mStatusChangeCallback != null) {
          mStatusChangeCallback.accept(this);
        }
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

    if (!(o instanceof PlanInfo)) {
      return false;
    }

    PlanInfo other = (PlanInfo) o;
    return Objects.equal(mId, other.mId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mId);
  }
}
