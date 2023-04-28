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

import alluxio.grpc.OperationType;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * This class holds command status detailed information.
 */
public class CmdStatusBlock {
  private long mJobControlId;
  private List<SimpleJobStatusBlock> mJobStatusBlockList;
  private OperationType mOperationType;

  /**
   * Constructor.
   * @param jobControlId command JobControlId
   * @param type the operation type
   */
  public CmdStatusBlock(long jobControlId, OperationType type) {
    mJobControlId = jobControlId;
    mJobStatusBlockList = Lists.newArrayList();
    mOperationType = type;
  }

  /**
   * Constructor.
   * @param jobControlId command JobControlId
   * @param blocks SimpleJobStatusBlock list
   * @param type operation type
   */
  public CmdStatusBlock(long jobControlId, List<SimpleJobStatusBlock> blocks, OperationType type) {
    mJobControlId = jobControlId;
    mJobStatusBlockList = blocks;
    mOperationType = type;
  }

  /**
   * Get job control ID of the command.
   * @return job control ID
   */
  public long getJobControlId() {
    return mJobControlId;
  }

  /**
   * Add an entry of job status block.
   * @param block
   */
  public void addJobStatusBlock(SimpleJobStatusBlock block) {
    mJobStatusBlockList.add(block);
  }

  /**
   * Get a list of job status block for a command.
   * @return list of SimpleJobStatusBlock
   */
  public List<SimpleJobStatusBlock> getJobStatusBlock() {
    return mJobStatusBlockList;
  }

  /**
   * Get OperationType.
   * @return OperationType
   */
  public OperationType getOperationType() {
    return mOperationType;
  }

  /**
   * Convert to proto type.
   * @return return proto type of CmdStatusBlock
   */
  public alluxio.grpc.CmdStatusBlock toProto() throws IOException {
    List<alluxio.grpc.JobStatusBlock> jobStatusBlockList = Lists.newArrayList();
    mJobStatusBlockList.forEach(block -> {
      alluxio.grpc.JobStatusBlock protoBlock =
              alluxio.grpc.JobStatusBlock
                      .newBuilder()
                      .setJobId(block.getJobId())
                      .setJobStatus(block.getStatus().toProto())
                      .setFilePath(block.getFilePath())
                      .setFilePathFailed(block.getFilesPathFailed())
                      .build();
      jobStatusBlockList.add(protoBlock);
    });
    return alluxio.grpc.CmdStatusBlock
            .newBuilder()
            .setJobControlId(mJobControlId)
            .addAllJobStatusBlock(jobStatusBlockList)
            .setOperationType(mOperationType)
            .build();
  }
}
