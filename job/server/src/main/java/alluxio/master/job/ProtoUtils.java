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

package alluxio.master.job;

import alluxio.job.util.SerializationUtils;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.proto.journal.Job;

import com.google.common.base.Preconditions;

/**
 * Utilities for converting between in-memory and proto definitions.
 */
public final class ProtoUtils {

  /**
   * @param taskInfo the task to convert to proto
   * @return the protocol buffer version of this task info
   */
  public static Job.TaskInfo toProto(TaskInfo taskInfo) {
    Job.TaskInfo.Builder builder = Job.TaskInfo.newBuilder()
        .setJobId(taskInfo.getJobId())
        .setTaskId(taskInfo.getTaskId())
        .setStatus(toProto(taskInfo.getStatus()));
    if (taskInfo.getErrorMessage() != null) {
      builder.setErrorMessage(taskInfo.getErrorMessage());
    }
    if (taskInfo.getResult() != null) {
      alluxio.util.proto.ProtoUtils.setResult(builder,
          SerializationUtils.serialize(taskInfo.getResult(), "Failed to serialize task result"));
    }
    return builder.build();
  }

  /**
   * @param status the status to convert
   * @return the protocol buffer type representing the given status
   */
  public static Job.Status toProto(Status status) {
    return Job.Status.valueOf(status.name());
  }

  /**
   * @param taskInfo the protocol buffer task information to convert
   * @return the {@link TaskInfo} version of the given protocol buffer task info
   */
  public static TaskInfo fromProto(Job.TaskInfo taskInfo) {
    Preconditions.checkState(taskInfo.hasJobId(),
        "Deserializing protocol task info with unset jobId");
    Preconditions.checkState(taskInfo.hasTaskId(),
        "Deserializing protocol task info with unset taskId");
    Preconditions.checkState(taskInfo.hasStatus(),
        "Deserializing protocol task info with unset status");
    TaskInfo info = new TaskInfo()
        .setJobId(taskInfo.getJobId())
        .setTaskId(taskInfo.getTaskId())
        .setStatus(fromProto(taskInfo.getStatus()))
        .setErrorMessage(taskInfo.getErrorMessage());
    if (taskInfo.hasResult()) {
      info.setResult(taskInfo.getResult().toByteArray());
    }
    return info;
  }

  /**
   * @param status the protocol buffer status to convert
   * @return the {@link Status} type representing the given status
   */
  public static Status fromProto(Job.Status status) {
    return Status.valueOf(status.name());
  }

  private ProtoUtils() {} // prevent instantiation
}
