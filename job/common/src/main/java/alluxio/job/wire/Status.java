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
 * The status of a task.
 */
@ThreadSafe
public enum Status {
  CREATED, CANCELED, FAILED, RUNNING, COMPLETED;

  /**
   * @return whether this status represents a finished state, i.e. canceled, failed, or completed
   */
  public boolean isFinished() {
    return this.equals(CANCELED) || this.equals(FAILED) || this.equals(COMPLETED);
  }

  /**
   * @return proto representation of the status
   */
  public alluxio.grpc.Status toProto() {
    switch (this) {
      case CREATED:
        return alluxio.grpc.Status.CREATED;
      case CANCELED:
        return alluxio.grpc.Status.CANCELED;
      case FAILED:
        return alluxio.grpc.Status.FAILED;
      case RUNNING:
        return alluxio.grpc.Status.RUNNING;
      case COMPLETED:
        return alluxio.grpc.Status.COMPLETED;
      default:
        return alluxio.grpc.Status.UNKNOWN;
    }
  }
}
