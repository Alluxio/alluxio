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
import alluxio.stress.graph.Graph;
import alluxio.stress.graph.LineGraph;

import com.google.common.base.Splitter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
public final class MasterBenchSummary implements Summary {
  private long mDurationMs;
  private long mEndTimeMs;
  private MasterBenchParameters mParameters;
  private List<String> mNodes;
  private Map<String, List<String>> mErrors;

  private MasterBenchSummaryStatistics mStatistics;

  private Map<String, MasterBenchSummaryStatistics> mStatisticsPerMethod;

  /**
   * Creates an instance.
   */
  public MasterBenchSummary() {
    // Default constructor required for json deserialization
  }

  /**
   * Creates an instance.
   *
   * @param mergedTaskResults the merged task result
   * @param nodes the list of nodes
   * @param errors the list of errors
   */
  public MasterBenchSummary(MasterBenchTaskResult mergedTaskResults, List<String> nodes,
      Map<String, List<String>> errors) throws DataFormatException {
    mStatistics = mergedTaskResults.getResultStatistics().toMasterBenchSummaryStatistics();

    mStatisticsPerMethod = new HashMap<>();
    for (Map.Entry<String, MasterBenchTaskResultStatistics> entry :
        mergedTaskResults.getStatisticsPerMethod().entrySet()) {
      final String key = entry.getKey();
      final MasterBenchTaskResultStatistics value = entry.getValue();

      mStatisticsPerMethod.put(key, value.toMasterBenchSummaryStatistics());
    }

    mDurationMs = mergedTaskResults.getEndMs() - mergedTaskResults.getRecordStartMs();
    mEndTimeMs = mergedTaskResults.getEndMs();
    mParameters = mergedTaskResults.getParameters();
    mNodes = nodes;
    mErrors = errors;
  }

  /**
   * @return the throughput
   */
  public float computeThroughput() {
    return ((float) mStatistics.mNumSuccess / mDurationMs) * 1000.0f;
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
  public MasterBenchParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the parameters
   */
  public void setParameters(MasterBenchParameters parameters) {
    mParameters = parameters;
  }

  /**
   * @return the list of nodes
   */
  public List<String> getNodes() {
    return mNodes;
  }

  /**
   * @param nodes the list of nodes
   */
  public void setNodes(List<String> nodes) {
    mNodes = nodes;
  }

  /**
   * @return the errors
   */
  public Map<String, List<String>> getErrors() {
    return mErrors;
  }

  /**
   * @param errors the errors
   */
  public void setErrors(Map<String, List<String>> errors) {
    mErrors = errors;
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

  public MasterBenchSummaryStatistics getStatistics() {
    return mStatistics;
  }

  public void setStatistics(MasterBenchSummaryStatistics statistics) {
    mStatistics = statistics;
  }

  public Map<String, MasterBenchSummaryStatistics> getStatisticsPerMethod() {
    return mStatisticsPerMethod;
  }

  public void setStatisticsPerMethod(Map<String, MasterBenchSummaryStatistics> statisticsPerMethod) {
    mStatisticsPerMethod = statisticsPerMethod;
  }

  private LineGraph.Data getResponseTimeData() {
    LineGraph.Data data = new LineGraph.Data();
    data.addData("50", mStatistics.mResponseTimePercentileMs[50]);
    data.addData("75", mStatistics.mResponseTimePercentileMs[75]);
    data.addData("90", mStatistics.mResponseTimePercentileMs[90]);
    data.addData("95", mStatistics.mResponseTimePercentileMs[95]);

    String percentile = "99";
    for (float ms : mStatistics.mResponseTime99PercentileMs) {
      data.addData(percentile, ms);
      if (percentile.equals("99")) {
        percentile += ".";
      }
      percentile += "9";
    }

    return data;
  }

  @Override
  public alluxio.stress.GraphGenerator graphGenerator() {
    return new GraphGenerator();
  }

  /**
   * The graph generator for this summary.
   */
  public static final class GraphGenerator extends alluxio.stress.GraphGenerator {
    @Override
    public List<Graph> generate(List<? extends Summary> results) {
      List<Graph> graphs = new ArrayList<>();
      // only examine MasterBenchTaskSummary
      List<MasterBenchSummary> summaries =
          results.stream().map(x -> (MasterBenchSummary) x).collect(Collectors.toList());

      // Iterate over all operations
      for (Operation operation : Operation.values()) {
        List<MasterBenchSummary> opSummaries =
            summaries.stream().filter(x -> x.mParameters.mOperation == operation)
                .collect(Collectors.toList());

        if (!opSummaries.isEmpty()) {
          // first() is the list of common field names, second() is the list of unique field names
          Pair<List<String>, List<String>> fieldNames = Parameters.partitionFieldNames(
              opSummaries.stream().map(x -> x.mParameters).collect(Collectors.toList()));

          // Split up common description into 100 character chunks, for the sub title
          List<String> subTitle = new ArrayList<>(Splitter.fixedLength(100).splitToList(
              opSummaries.get(0).mParameters.getDescription(fieldNames.getFirst())));

          for (MasterBenchSummary summary : opSummaries) {
            String series = summary.mParameters.getDescription(fieldNames.getSecond());
            subTitle.add(
                series + ": " + DateFormat.getDateTimeInstance().format(summary.getEndTimeMs()));
          }

          LineGraph responseTimeGraph =
              new LineGraph(operation + " - Response Time (ms)", subTitle, "Percentile",
                  "Response Time (ms)");

          for (MasterBenchSummary summary : opSummaries) {
            String series = summary.mParameters.getDescription(fieldNames.getSecond());
            responseTimeGraph.addDataSeries(series, summary.getResponseTimeData());
          }

          graphs.add(responseTimeGraph);
        }
      }

      return graphs;
    }
  }
}
