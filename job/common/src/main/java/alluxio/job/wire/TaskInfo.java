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

import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.JobType;
import alluxio.job.util.SerializableVoid;
import alluxio.job.util.SerializationUtils;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The task description.
 */
@NotThreadSafe
public class TaskInfo implements JobInfo {
  private static final Logger LOG = LoggerFactory.getLogger(TaskInfo.class);

  private long mJobId;
  private long mTaskId;
  private Status mStatus;
  private String mErrorType;
  private String mErrorMessage;
  private Serializable mResult;
  private long mLastUpdated;
  private String mWorkerHost;
  private String mDescription;

  /**
   * Default constructor.
   */
  public TaskInfo() {
    mErrorType = "";
    mErrorMessage = "";
    mDescription = "";
  }

  /**
   * Constructs a new TaskInfo from jobId, taskId, Status, workerAddress, and arguments.
   * @param jobId the job id
   * @param taskId the task id
   * @param status the status
   * @param workerAddress the worker address
   * @param args the (Serializable) arguments that were used to execute the task
   */
  public TaskInfo(long jobId, long taskId, Status status, WorkerNetAddress workerAddress,
                  Object args) {
    mJobId = jobId;
    mTaskId = taskId;
    mStatus = status;
    mErrorType = "";
    mErrorMessage = "";
    mResult = null;
    mWorkerHost = workerAddress.getHost();
    mDescription = createDescription(args);
  }

  private static String createDescription(Object args) {
    if (args instanceof Collection) {
      return Arrays.toString(((Collection) args).toArray());
    }
    if (args instanceof SerializableVoid) {
      return "";
    }
    return ObjectUtils.toString(args);
  }

  /**
   * Constructs from the proto format.
   *
   * @param taskInfo the task info in proto format
   * @throws IOException if the deserialization fails
   */
  public TaskInfo(alluxio.grpc.JobInfo taskInfo) throws IOException {
    Preconditions.checkArgument(taskInfo.getType().equals(JobType.TASK), "Invalid type");

    mJobId = taskInfo.getParentId();
    mTaskId = taskInfo.getId();
    mStatus = Status.valueOf(taskInfo.getStatus().name());
    mErrorType = taskInfo.getErrorType();
    mErrorMessage = taskInfo.getErrorMessage();
    mWorkerHost = taskInfo.getWorkerHost();
    mResult = null;
    if (taskInfo.hasResult()) {
      try {
        mResult = SerializationUtils.deserialize(taskInfo.getResult().toByteArray());
      } catch (ClassNotFoundException e) {
        throw new InvalidArgumentException(e);
      }
    }
    mLastUpdated = taskInfo.getLastUpdated();
    mDescription = taskInfo.getDescription();
  }

  @Override
  public long getId() {
    return getTaskId();
  }

  /**
   * @return the task id
   */
  public long getTaskId() {
    return mTaskId;
  }

  /**
   * @param taskId the task id
   * @return the updated task info object
   */
  public TaskInfo setTaskId(long taskId) {
    mTaskId = taskId;
    return this;
  }

  @Override
  public Long getParentId() {
    return getJobId();
  }

  /**
   * @return the job id
   */
  public long getJobId() {
    return mJobId;
  }

  /**
   * @return the worker host
   */
  public Object getWorkerHost() {
    return mWorkerHost;
  }

  /**
   * @param jobId the job id
   * @return the updated task info object
   */
  public TaskInfo setJobId(long jobId) {
    mJobId = jobId;
    return this;
  }

  @Override
  public String getName() {
    return String.format("Task %s", mTaskId);
  }

  @Override
  public String getDescription() {
    return mDescription;
  }

  /**
   * @param description the description
   */
  public void setDescription(String description) {
    mDescription = description;
  }

  @Override
  public Status getStatus() {
    return mStatus;
  }

  /**
   * @param status the task status
   * @return the updated task info object
   */
  public TaskInfo setStatus(Status status) {
    mStatus = status;
    updateLastUpdated();
    return this;
  }

  @Override
  public long getLastUpdated() {
    return mLastUpdated;
  }

  private void updateLastUpdated() {
    mLastUpdated = CommonUtils.getCurrentMs();
  }

  @Override
  public List<JobInfo> getChildren() {
    return ImmutableList.of();
  }

  /**
   * @return the error type
   */
  @Override
  public String getErrorType() {
    return mErrorType;
  }

  /**
   * @param errorType the error type
   * @return the updated task info object
   */
  public TaskInfo setErrorType(String errorType) {
    mErrorType = errorType;
    return this;
  }

  /**
   * @return the error message
   */
  public String getErrorMessage() {
    return mErrorMessage;
  }

  /**
   * @param errorMessage the error message
   * @return the updated task info object
   */
  public TaskInfo setErrorMessage(String errorMessage) {
    mErrorMessage = errorMessage;
    return this;
  }

  /**
   * @return the result
   */
  public Serializable getResult() {
    return mResult;
  }

  /**
   * @param result the result
   * @return the updated task info object
   */
  public TaskInfo setResult(Serializable result) {
    mResult = result;
    return this;
  }

  /**
   * @param workerHost the worker host
   * @return the updated task info object
   */
  public TaskInfo setWorkerHost(String workerHost) {
    mWorkerHost = workerHost;
    return this;
  }

  /**
   * @return proto representation of the task info
   * @throws IOException if serialization fails
   */
  public alluxio.grpc.JobInfo toProto() {
    ByteBuffer result = null;
    try {
      result = mResult == null ? null : ByteBuffer.wrap(SerializationUtils.serialize(mResult));
    } catch (IOException e) {
      // TODO(bradley) better error handling
      LOG.error("Failed to serialize {}", mResult, e);
    }

    alluxio.grpc.JobInfo.Builder taskInfoBuilder =
        alluxio.grpc.JobInfo.newBuilder().setParentId(mJobId).setId(mTaskId)
            .setStatus(mStatus.toProto()).setErrorMessage(mErrorMessage)
            .setErrorType(mErrorType).setLastUpdated(mLastUpdated).setWorkerHost(mWorkerHost)
            .setType(JobType.TASK).setDescription(mDescription);
    if (result != null) {
      taskInfoBuilder.setResult(ByteString.copyFrom(result));
    }
    return taskInfoBuilder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TaskInfo)) {
      return false;
    }
    TaskInfo that = (TaskInfo) o;
    return Objects.equal(mJobId, that.mJobId) && Objects.equal(mTaskId, that.mTaskId)
        && Objects.equal(mStatus, that.mStatus) && Objects.equal(mErrorMessage, that.mErrorMessage)
        && Objects.equal(mResult, that.mResult) && Objects.equal(mLastUpdated, that.mLastUpdated)
        && Objects.equal(mWorkerHost, that.mWorkerHost)
        && Objects.equal(mErrorType, that.mErrorType)
        && Objects.equal(mDescription, that.mDescription);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mJobId, mTaskId, mStatus, mErrorMessage, mResult, mLastUpdated,
        mWorkerHost, mDescription);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("jobId", mJobId).add("taskId", mTaskId)
        .add("status", mStatus).add("errorMessage", mErrorMessage).add("result", mResult)
        .add("lastUpdated", mLastUpdated).add("workerHost", mWorkerHost)
        .add("description", mDescription).toString();
  }
}
