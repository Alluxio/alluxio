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

import alluxio.annotation.SuppressFBWarnings;
import alluxio.stress.StressConstants;
import alluxio.stress.common.SummaryStatistics;
import alluxio.stress.common.TaskResultStatistics;
import alluxio.util.JsonSerializable;

import java.util.Arrays;
import java.util.zip.DataFormatException;

/**
 * Keeps results for cross cluster latency.
 */
public class CrossClusterLatencyStatistics extends TaskResultStatistics {
  private long[] mUfsOpsCountByCluster;
  private RandResult mRandResult;

  /**
   * Creates an instance.
   */
  public CrossClusterLatencyStatistics() {
    super();
    mMaxResponseTimeNs = new long[StressConstants.MAX_TIME_COUNT];
    Arrays.fill(mMaxResponseTimeNs, -1);
  }

  /**
   * @param randResult the results from the random reader thread
   */
  public void setRandResult(RandResult randResult) {
    mRandResult = randResult;
  }

  /**
   * Sets the number of ufs sync operations performed at each cluster
   * @param ufsOpsCountByCluster the array of counts
   */
  public void setUfsOpsCountByCluster(long[] ufsOpsCountByCluster) {
    mUfsOpsCountByCluster = ufsOpsCountByCluster;
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

  class CrossClusterSummary implements JsonSerializable {
    public long[] mUfsOpsCountByCluster = CrossClusterLatencyStatistics.this.mUfsOpsCountByCluster;
    public long mNumSuccess = CrossClusterLatencyStatistics.this.mNumSuccess;
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
    public long[] mMaxResponseTimeNs = CrossClusterLatencyStatistics.this.mMaxResponseTimeNs;
    public RandResult mRandomReadResults = CrossClusterLatencyStatistics.this.mRandResult;
    public SummaryStatistics mSummaryStatistics = CrossClusterLatencyStatistics
        .this.toBenchSummaryStatistics();

    CrossClusterSummary() throws DataFormatException {
    }
  }

  /**
   * @return the results as a summary
   */
  CrossClusterSummary toSummary() throws DataFormatException {
    return new CrossClusterSummary();
  }
}
