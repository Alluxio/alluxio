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

import alluxio.stress.StressConstants;

import org.HdrHistogram.Histogram;

class CrossClusterResultsRunning {
  CrossClusterLatencyStatistics mResults = new CrossClusterLatencyStatistics();
  Histogram mLatencies = new Histogram(StressConstants.TIME_HISTOGRAM_MAX,
      StressConstants.TIME_HISTOGRAM_PRECISION);

  void opCompleted(long durationNs) {
    mLatencies.recordValue(durationNs);
    mResults.recordResult(durationNs);
  }

  CrossClusterLatencyStatistics toResult() {
    mResults.encodeResponseTimeNsRaw(mLatencies);
    return mResults;
  }
}
