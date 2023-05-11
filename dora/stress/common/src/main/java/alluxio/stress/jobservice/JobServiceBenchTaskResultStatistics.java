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

package alluxio.stress.jobservice;

import alluxio.stress.StressConstants;
import alluxio.stress.common.TaskResultStatistics;

import java.util.Arrays;

/**
 * Statistics class that is used in {@link JobServiceBenchTaskResult}.
 */
public class JobServiceBenchTaskResultStatistics extends TaskResultStatistics {

  /**
   * Creates an instance.
   */
  public JobServiceBenchTaskResultStatistics() {
    // Default constructor required for json deserialization
    super();
    mMaxResponseTimeNs = new long[StressConstants.JOB_SERVICE_MAX_RESPONSE_TIME_COUNT];
    Arrays.fill(mMaxResponseTimeNs, -1);
  }
}
