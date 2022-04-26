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

/**
 * Class for simple job status information.
 */
public class SimpleJobStatusBlock {
  private long mJobId;
  private Status mStatus;

  /**
   * Constructor.
   * @param jobId
   * @param status
   */
  public SimpleJobStatusBlock(long jobId, Status status) {
    mJobId = jobId;
    mStatus = status;
  }

  /**
   * Get job id.
   * @return job id
   */
  public long getJobId() {
    return mJobId;
  }

  /**
   * Get status of the job.
   * @return status information
   */
  public Status getStatus() {
    return mStatus;
  }
}
