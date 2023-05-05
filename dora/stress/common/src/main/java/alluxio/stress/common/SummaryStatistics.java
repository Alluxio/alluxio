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

package alluxio.stress.common;

import alluxio.annotation.SuppressFBWarnings;
import alluxio.stress.StressConstants;
import alluxio.stress.graph.LineGraph;
import alluxio.stress.master.MasterBenchSummary;

import java.util.Arrays;

/**
 * Statistics class for {@link MasterBenchSummary}.
 */
public class SummaryStatistics {
  /** number of successes. */
  public long mNumSuccess;

  /** response times for all percentiles from 0 -> 100 (101 values). */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public float[] mTimePercentileMs;
  /** percentiles of just 99.x%. first entry is 99%, second is 99.9%, etc. */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public float[] mTime99PercentileMs;
  /** max time over time, over the duration of the test. */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  public float[] mMaxTimeMs;

  /**
   * Creates an instance.
   */
  public SummaryStatistics() {
    // Default constructor required for json deserialization
    mTimePercentileMs = new float[101];
    Arrays.fill(mTimePercentileMs, 0);
    mTime99PercentileMs = new float[StressConstants.TIME_99_COUNT];
    Arrays.fill(mTime99PercentileMs, 0);
    mMaxTimeMs = new float[StressConstants.MAX_TIME_COUNT];
    Arrays.fill(mMaxTimeMs, 0);
  }

  /**
   * Creates an instance.
   *
   * @param numSuccess the number of success
   * @param timePercentileMs the response times (in ms), for all percentiles
   * @param time99PercentileMs the response times (in ms), for the 99.x percentiles
   * @param maxTimeMs the max response times (in ms) over time
   */
  public SummaryStatistics(long numSuccess, float[] timePercentileMs,
                                      float[] time99PercentileMs,
                                      float[] maxTimeMs) {
    mNumSuccess = numSuccess;
    mTimePercentileMs = timePercentileMs;
    mTime99PercentileMs = time99PercentileMs;
    mMaxTimeMs = maxTimeMs;
  }

  /**
   * @return the response time linegraph data
   */
  public LineGraph.Data computeTimeData() {
    LineGraph.Data data = new LineGraph.Data();

    if (mNumSuccess == 0) {
      // Return empty data for empty results
      return data;
    }

    data.addData(50, mTimePercentileMs[50]);
    data.addData(75, mTimePercentileMs[75]);
    data.addData(90, mTimePercentileMs[90]);
    data.addData(95, mTimePercentileMs[95]);

    int counter = 0;
    for (float ms : mTime99PercentileMs) {
      float percentile = (float) (100.0 - 1.0 / (Math.pow(10.0, counter)));
      data.addData(percentile, ms);
      counter++;
    }

    return data;
  }
}
