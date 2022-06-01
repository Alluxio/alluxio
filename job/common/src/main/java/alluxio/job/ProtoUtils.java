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

package alluxio.job;

import alluxio.grpc.JobStatusBlock;
import alluxio.job.wire.CmdStatusBlock;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.PlanInfo;
import alluxio.job.wire.SimpleJobStatusBlock;
import alluxio.job.wire.Status;
import alluxio.job.wire.TaskInfo;
import alluxio.job.wire.WorkflowInfo;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * Utility for converting proto job representation to java job representation.
 */
public class ProtoUtils {

  /**
   * @param jobInfo the proto representation of {@link JobInfo} to convert
   * @return {@link JobInfo} representation
   * @throws IOException if result can't be deserialized
   */
  public static JobInfo fromProto(alluxio.grpc.JobInfo jobInfo) throws IOException {
    switch (jobInfo.getType()) {
      case PLAN:
        return new PlanInfo(jobInfo);
      case TASK:
        return new TaskInfo(jobInfo);
      case WORKFLOW:
        return new WorkflowInfo(jobInfo);
      default:
        throw new IllegalStateException(String.format("Unexpected Type: %s", jobInfo.getType()));
    }
  }

  /**
   * @param status the proto status to convert
   * @return {@link Status} representation
   * @throws IOException if result can't be deserialized
   */
  public static Status fromProto(alluxio.grpc.Status status) throws IOException {
    switch (status) {
      case CREATED:
        return Status.CREATED;
      case CANCELED:
        return Status.CANCELED;
      case FAILED:
        return Status.FAILED;
      case RUNNING:
        return Status.RUNNING;
      case COMPLETED:
        return Status.COMPLETED;
      default:
        throw new IllegalStateException(String.format("Unexpected Status type: %s", status));
    }
  }

  /**
   * Convert proto to list of SimpleJobStatusBlock.
   * @param cmdStatusBlock the cmdStatusBlock to convert
   * @return {@link CmdStatusBlock}
   * @throws IOException if result can't be deserialized
   */
  public static CmdStatusBlock protoToCmdStatusBlock(
          alluxio.grpc.CmdStatusBlock cmdStatusBlock) throws IOException {
    List<SimpleJobStatusBlock> simpleJobStatusBlockList = Lists.newArrayList();
    List<alluxio.grpc.JobStatusBlock> blocks = cmdStatusBlock
            .getJobStatusBlockList();
    for (JobStatusBlock block : blocks) {
      simpleJobStatusBlockList
              .add(new SimpleJobStatusBlock(
                      block.getJobId(),
                      fromProto(block.getJobStatus()),
                      block.getFilePath(),
                      block.getFilePathFailed()));
    }
    return new CmdStatusBlock(cmdStatusBlock.getJobControlId(),
            simpleJobStatusBlockList, cmdStatusBlock.getOperationType());
  }

  private ProtoUtils() {} // prevent instantiation
}
