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

import alluxio.stress.graph.LineGraph;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Statistics class for {@link MasterBenchSummary}.
 */
public class MasterBenchSummaryStatistics {
  /** number of successes. */
  public long mNumSuccess;

  /** response times for all percentiles from 0 -> 100 (101 values). */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public float[] mResponseTimePercentileMs;
  /** percentiles of just 99.x%. first entry is 99%, second is 99.9%, etc. */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public float[] mResponseTime99PercentileMs;
  /** max response time over time, over the duration of the test. */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public float[] mMaxResponseTimeMs;

  /**
   * Creates an instance.
   */
  public MasterBenchSummaryStatistics() {
    // Default constructor required for json deserialization
  }

  /**
   * Creates an instance.
   *
   * @param numSuccess the number of success
   * @param responseTimePercentileMs the response times (in ms), for all percentiles
   * @param responseTime99PercentileMs the response times (in ms), for the 99.x percentiles
   * @param maxResponseTimeMs the max response times (in ms) over time
   */
  public MasterBenchSummaryStatistics(long numSuccess, float[] responseTimePercentileMs,
                                      float[] responseTime99PercentileMs,
                                      float[] maxResponseTimeMs) {
    mNumSuccess = numSuccess;
    mResponseTimePercentileMs = responseTimePercentileMs;
    mResponseTime99PercentileMs = responseTime99PercentileMs;
    mMaxResponseTimeMs = maxResponseTimeMs;
  }

  /**
   * @return the response time linegraph data
   */
  public LineGraph.Data computeResponseTimeData() {
    LineGraph.Data data = new LineGraph.Data();

    data.addData(50, mResponseTimePercentileMs[50]);
    data.addData(75, mResponseTimePercentileMs[75]);
    data.addData(90, mResponseTimePercentileMs[90]);
    data.addData(95, mResponseTimePercentileMs[95]);

    int counter = 0;
    for (float ms : mResponseTime99PercentileMs) {
      float percentile = (float) (100.0 - 1.0 / (Math.pow(10.0, counter)));
      data.addData(percentile, ms);
      counter++;
    }

    return data;
  }
}
