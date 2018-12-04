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
import alluxio.job.util.SerializationUtils;

import com.google.common.base.Objects;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The task description.
 */
@NotThreadSafe
public class TaskInfo {
  private long mJobId;
  private int mTaskId;
  private Status mStatus;
  private String mErrorMessage;
  private Serializable mResult;

  /**
   * Default constructor.
   */
  public TaskInfo() {}

  /**
   * Constructs from the proto format.
   *
   * @param taskInfo the task info in proto format
   * @throws IOException if the deserialization fails
   */
  public TaskInfo(alluxio.grpc.TaskInfo taskInfo) throws IOException {
    mJobId = taskInfo.getJobId();
    mTaskId = taskInfo.getTaskId();
    mStatus = Status.valueOf(taskInfo.getStatus().name());
    mErrorMessage = taskInfo.getErrorMessage();
    mResult = null;
    if(taskInfo.hasResult()) {
      try {
        mResult = SerializationUtils.deserialize(taskInfo.getResult().toByteArray());
      } catch (ClassNotFoundException e) {
        throw new InvalidArgumentException(e);
      }
    }
  }

  /**
   * @return the job id
   */
  public long getJobId() {
    return mJobId;
  }

  /**
   * @return the task id
   */
  public int getTaskId() {
    return mTaskId;
  }

  /**
   * @return the task status
   */
  public Status getStatus() {
    return mStatus;
  }

  /**
   * @return the error message
   */
  public String getErrorMessage() {
    return mErrorMessage;
  }

  /**
   * @return the result
   */
  public Serializable getResult() {
    return mResult;
  }

  /**
   * @param jobId the job id
   * @return the updated task info object
   */
  public TaskInfo setJobId(long jobId) {
    mJobId = jobId;
    return this;
  }

  /**
   * @param taskId the task id
   * @return the updated task info object
   */
  public TaskInfo setTaskId(int taskId) {
    mTaskId = taskId;
    return this;
  }

  /**
   * @param status the task status
   * @return the updated task info object
   */
  public TaskInfo setStatus(Status status) {
    mStatus = status;
    return this;
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
   * @param result the result
   * @return the updated task info object
   */
  public TaskInfo setResult(byte[] result) {
    mResult = result == null ? null : Arrays.copyOf(result, result.length);
    return this;
  }

  /**
   * @return proto representation of the task info
   * @throws IOException if serialization fails
   */
  public alluxio.grpc.TaskInfo toProto() throws IOException {
    ByteBuffer result =
        mResult == null ? null : ByteBuffer.wrap(SerializationUtils.serialize(mResult));

    alluxio.grpc.TaskInfo.Builder taskInfoBuilder =
        alluxio.grpc.TaskInfo.newBuilder().setJobId(mJobId).setTaskId(mTaskId)
            .setStatus(mStatus.toProto()).setErrorMessage(mErrorMessage);
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
        && Objects.equal(mResult, that.mResult);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mJobId, mTaskId, mStatus, mErrorMessage, mResult);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("jobId", mJobId).add("taskId", mTaskId)
        .add("status", mStatus).add("errorMessage", mErrorMessage).add("result", mResult)
        .toString();
  }
}
