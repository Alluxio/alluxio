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

package alluxio.stress.master;

import alluxio.stress.StressConstants;
import alluxio.stress.common.TaskResultStatistics;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Arrays;
import javax.annotation.Nullable;

/**
 * Statistics class that is used in {@link MasterBenchTaskResult}.
 */
public class MasterBenchTaskResultStatistics extends TaskResultStatistics {
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public Operation mOperation;

  /**
   * Creates an instance.
   *
   * @param operation the operation
   */
  public MasterBenchTaskResultStatistics(@Nullable Operation operation) {
    super();
    mOperation = operation;
    mMaxResponseTimeNs = new long[StressConstants.MAX_TIME_COUNT];
    Arrays.fill(mMaxResponseTimeNs, -1);
  }

  /**
   * Creates an instance.
   */
  public MasterBenchTaskResultStatistics() {
    this(null);
  }
}
