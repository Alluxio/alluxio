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

import javax.annotation.concurrent.ThreadSafe;

/**
 * The JobSource for a Cmd job.
 */
@ThreadSafe
public enum JobSource {
  CLI, POLICY, SYSTEM;

  /**
   * @return proto representation of the JobSource
   */
  public alluxio.grpc.JobSource toProto() {
    switch (this) {
      case CLI:
        return alluxio.grpc.JobSource.CLI;
      case POLICY:
        return alluxio.grpc.JobSource.POLICY;
      case SYSTEM:
        return alluxio.grpc.JobSource.SYSTEM;
      default:
        return alluxio.grpc.JobSource.UNSUPPORTED;
    }
  }
}
