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

package alluxio.cross.cluster.cli;

import alluxio.Constants;
import alluxio.stress.StressConstants;
import alluxio.stress.common.SummaryStatistics;
import alluxio.stress.common.TaskResultStatistics;
import alluxio.util.JsonSerializable;

import java.util.Arrays;
import java.util.zip.DataFormatException;

/**
 * Record results of operation latencies.
 */
public class LatencyResultsStatistics extends TaskResultStatistics {

  long mDurationMs;

  /**
   * Create a new latency results instance.
   */
  public LatencyResultsStatistics() {
    super();
    mMaxResponseTimeNs = new long[StressConstants.MAX_TIME_COUNT];
    Arrays.fill(mMaxResponseTimeNs, -1);
  }

  /**
   * Merges (updates) a task result statistics with this statistics.
   *
   * @param statistics the task result statistics to merge
   */
  public void merge(LatencyResultsStatistics statistics) throws Exception {
    super.merge(statistics);
    mDurationMs = Math.max(statistics.mDurationMs, mDurationMs);
  }

  class LatencyResultsSummary implements JsonSerializable {
    public SummaryStatistics mSummaryStatistics = LatencyResultsStatistics
        .this.toBenchSummaryStatistics();
    public long mDurationMs = LatencyResultsStatistics.this.mDurationMs;
    public double mOpsPerSecond = (double) LatencyResultsStatistics.this.mNumSuccess
        / ((double) LatencyResultsStatistics.this.mDurationMs / Constants.SECOND_MS);

    LatencyResultsSummary() throws DataFormatException {
    }
  }

  /**
   * Set the benchmark duration.
   * @param durationMs the duration in milliseconds
   */
  public void recordDuration(long durationMs) {
    mDurationMs = durationMs;
  }

  /**
   * @param latencyNs the latency of an operation in nanoseconds
   */
  public void recordResult(long latencyNs) {
    mNumSuccess++;
    for (int i = 0; i < mMaxResponseTimeNs.length; i++) {
      if (mMaxResponseTimeNs[i] < latencyNs) {
        if (mMaxResponseTimeNs.length - 1 - i >= 0) {
          System.arraycopy(mMaxResponseTimeNs, i, mMaxResponseTimeNs,
              i + 1, mMaxResponseTimeNs.length - 1 - i);
        }
        mMaxResponseTimeNs[i] = latencyNs;
        break;
      }
    }
  }

  LatencyResultsSummary toResults() throws DataFormatException {
    return new LatencyResultsSummary();
  }

  /**
   * @return the results as a summary
   */
  JsonSerializable toSummary() throws DataFormatException {
    return new LatencyResultsSummary();
  }
}
