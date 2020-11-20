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

import alluxio.job.wire.JobInfo;
import alluxio.job.wire.PlanInfo;
import alluxio.job.wire.TaskInfo;
import alluxio.job.wire.WorkflowInfo;

import java.io.IOException;

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

  private ProtoUtils() {} // prevent instantiation
}
