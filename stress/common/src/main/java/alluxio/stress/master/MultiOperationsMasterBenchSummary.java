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

import alluxio.collections.Pair;
import alluxio.stress.Parameters;
import alluxio.stress.Summary;
import alluxio.stress.common.GeneralBenchSummary;
import alluxio.stress.common.SummaryStatistics;
import alluxio.stress.graph.BarGraph;
import alluxio.stress.graph.Graph;
import alluxio.stress.graph.LineGraph;

import com.google.common.base.Splitter;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

/**
 * The summary for the master stress tests.
 */
public final class MultiOperationsMasterBenchSummary
    extends GeneralBenchSummary<MultiOperationsMasterBenchTaskResult> {
  private long mDurationMs;
  private long mEndTimeMs;
  private MultiOperationsMasterBenchParameters mParameters;

  private List<SummaryStatistics> mStatistics;

  private Map<String, SummaryStatistics> mStatisticsPerMethod;

  /**
   * Creates an instance.
   */
  public MultiOperationsMasterBenchSummary() {
    // Default constructor required for json deserialization
  }

  /**
   * Creates an instance.
   *
   * @param mergedTaskResults the merged task result
   * @param nodes the map storing the nodes' result
   */
  public MultiOperationsMasterBenchSummary(
      MultiOperationsMasterBenchTaskResult mergedTaskResults,
      Map<String, MultiOperationsMasterBenchTaskResult> nodes) throws DataFormatException {
    mStatistics = mergedTaskResults.getSummaryStatistics();

    mStatisticsPerMethod = new HashMap<>();
    for (Map.Entry<String, MasterBenchTaskResultStatistics> entry :
        mergedTaskResults.getStatisticsPerMethod().entrySet()) {
      final String key = entry.getKey();
      final MasterBenchTaskResultStatistics value = entry.getValue();

      mStatisticsPerMethod.put(key, value.toBenchSummaryStatistics());
    }

    mDurationMs = mergedTaskResults.getEndMs() - mergedTaskResults.getRecordStartMs();
    mEndTimeMs = mergedTaskResults.getEndMs();
    mThroughput = ((float) mergedTaskResults.getTotalSuccess() / mDurationMs) * 1000.0f;
    mParameters = (MultiOperationsMasterBenchParameters) mergedTaskResults.mParameters;
    mNodeResults = nodes;
  }

  /**
   * @return the duration (in ms)
   */
  public long getDurationMs() {
    return mDurationMs;
  }

  /**
   * @param durationMs the duration (in ms)
   */
  public void setDurationMs(long durationMs) {
    mDurationMs = durationMs;
  }

  /**
   * @return the parameters
   */
  public MultiOperationsMasterBenchParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the parameters
   */
  public void setParameters(MultiOperationsMasterBenchParameters parameters) {
    mParameters = parameters;
  }

  /**
   * @return the end time (in ms)
   */
  public long getEndTimeMs() {
    return mEndTimeMs;
  }

  /**
   * @param endTimeMs the end time (in ms)
   */
  public void setEndTimeMs(long endTimeMs) {
    mEndTimeMs = endTimeMs;
  }

  /**
   * @return the statistics
   */
  public List<SummaryStatistics> getStatistics() {
    return mStatistics;
  }

  /**
   * @return statistics per method map
   */
  public Map<String, SummaryStatistics> getStatisticsPerMethod() {
    return mStatisticsPerMethod;
  }

  /**
   * @param statisticsPerMethod the statistics per method map
   */
  public void setStatisticsPerMethod(Map<String, SummaryStatistics>
                                         statisticsPerMethod) {
    mStatisticsPerMethod = statisticsPerMethod;
  }

  @Override
  public alluxio.stress.GraphGenerator graphGenerator() {
    throw new RuntimeException("TODO implement the graph generator");
  }
}
