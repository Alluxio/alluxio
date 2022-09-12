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

import alluxio.util.JsonSerializable;

import java.util.zip.DataFormatException;

/**
 * Keeps results for cross cluster latency.
 */
public class CrossClusterLatencyStatistics extends LatencyResultsStatistics {
  private long[] mUfsOpsCountByCluster;
  private RandResult mRandResult;

  /**
   * Creates an instance.
   */
  public CrossClusterLatencyStatistics() {
    super();
  }

  /**
   * @param randResult the results from the random reader thread
   */
  public void setRandResult(RandResult randResult) {
    mRandResult = randResult;
  }

  /**
   * Sets the number of ufs sync operations performed at each cluster.
   * @param ufsOpsCountByCluster the array of counts
   */
  public void setUfsOpsCountByCluster(long[] ufsOpsCountByCluster) {
    mUfsOpsCountByCluster = ufsOpsCountByCluster.clone();
  }

  class CrossClusterSummary implements JsonSerializable {
    public long[] mUfsOpsCountByCluster = CrossClusterLatencyStatistics.this
        .mUfsOpsCountByCluster.clone();
    public long mNumSuccess = CrossClusterLatencyStatistics.this.mNumSuccess;
    public long[] mMaxResponseTimeNs = CrossClusterLatencyStatistics.this
        .mMaxResponseTimeNs.clone();
    public RandResult mRandomReadResults = CrossClusterLatencyStatistics.this.mRandResult;
    public JsonSerializable mSummaryStatistics = CrossClusterLatencyStatistics.this.toResults();

    CrossClusterSummary() throws DataFormatException {
    }
  }

  /**
   * @return the results as a summary
   */
  @Override
  JsonSerializable toSummary() throws DataFormatException {
    return new CrossClusterSummary();
  }
}
