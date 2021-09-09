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

import alluxio.collections.Pair;
import alluxio.stress.Parameters;
import alluxio.stress.Summary;
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
 * The summary for the job service stress tests.
 */
public final class JobServiceBenchSummary implements Summary {
  private long mDurationMs;
  private long mEndTimeMs;
  private JobServiceBenchParameters mParameters;
  private List<String> mNodes;
  private Map<String, List<String>> mErrors;

  private float mThroughput;
  private SummaryStatistics mStatistics;
  private Map<String, SummaryStatistics> mStatisticsPerMethod;

  /**
   * Creates an instance.
   */
  public JobServiceBenchSummary() {
    // Default constructor required for json deserialization
  }

  /**
   * Creates an instance.
   *
   * @param mergedTaskResults the merged task result
   * @param nodes the list of nodes
   * @param errors the list of errors
   */
  public JobServiceBenchSummary(JobServiceBenchTaskResult mergedTaskResults, List<String> nodes,
      Map<String, List<String>> errors) throws DataFormatException {
    mStatistics = mergedTaskResults.getStatistics().toBenchSummaryStatistics();
    mStatisticsPerMethod = new HashMap<>();
    for (Map.Entry<String, JobServiceBenchTaskResultStatistics> entry :
        mergedTaskResults.getStatisticsPerMethod().entrySet()) {
      final String key = entry.getKey();
      final JobServiceBenchTaskResultStatistics value = entry.getValue();
      mStatisticsPerMethod.put(key, value.toBenchSummaryStatistics());
    }
    mEndTimeMs = mergedTaskResults.getEndMs();
    mParameters = mergedTaskResults.getParameters();
    mDurationMs = mEndTimeMs - mergedTaskResults.getRecordStartMs();
    mThroughput = ((float) mStatistics.mNumSuccess / mDurationMs) * 1000.0f;
    mNodes = nodes;
    mErrors = errors;
  }

  /**
   * @return the throughput
   */
  public float getThroughput() {
    return mThroughput;
  }

  /**
   * @param throughput the throughput
   */
  public void setThroughput(float throughput) {
    mThroughput = throughput;
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
  public JobServiceBenchParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the parameters
   */
  public void setParameters(JobServiceBenchParameters parameters) {
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
  /**
   * @return the statistics
   */
  public SummaryStatistics getStatistics() {
    return mStatistics;
  }

  /**
   * @param statistics the statistics
   */
  public void setStatistics(SummaryStatistics statistics) {
    mStatistics = statistics;
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
  public void setStatisticsPerMethod(Map<String, SummaryStatistics> statisticsPerMethod) {
    mStatisticsPerMethod = statisticsPerMethod;
  }

  private LineGraph.Data computeResponseTimeData() {
    return mStatistics.computeTimeData();
  }

  private List<String> collectErrors() {
    List<String> errors = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : mErrors.entrySet()) {
      // add all the errors for this node, with the node appended to prefix
      errors.addAll(entry.getValue().stream().map(err -> entry.getKey() + ": " + err)
          .collect(Collectors.toList()));
    }
    return errors;
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
      List<JobServiceBenchSummary> summaries =
          results.stream().map(x -> (JobServiceBenchSummary) x).collect(Collectors.toList());
      // Iterate over all operations
      for (JobServiceBenchOperation operation : JobServiceBenchOperation.values()) {
        List<JobServiceBenchSummary> opSummaries = summaries.stream()
            .filter(x -> x.mParameters.mOperation == operation)
            .collect(Collectors.toList());
        if (opSummaries.isEmpty()) {
          continue;
        }
        // first() is the list of common field names, second() is the list of unique field names
        Pair<List<String>, List<String>> fieldNames = Parameters.partitionFieldNames(
            opSummaries.stream().map(x -> x.mParameters).collect(Collectors.toList()));
        // Split up common description into 100 character chunks, for the sub title
        List<String> subTitle = new ArrayList<>(Splitter.fixedLength(100).splitToList(
            opSummaries.get(0).mParameters.getDescription(fieldNames.getFirst())));
        for (JobServiceBenchSummary summary : opSummaries) {
          String series = summary.mParameters.getDescription(fieldNames.getSecond());
          subTitle.add(
              series + ": " + DateFormat.getDateTimeInstance().format(summary.getEndTimeMs()));
        }
        LineGraph responseTimeGraph =
            new LineGraph(operation + " - Response Time (ms)", subTitle, "Percentile",
                "Response Time (ms)");
        graphs.add(responseTimeGraph);
        Map<String, LineGraph> responseTimeGraphPerMethod = new HashMap<>();
        // Maps method name to max number of calls
        Map<String, Long> methodCounts = new HashMap<>();
        for (JobServiceBenchSummary summary : opSummaries) {
          String series = summary.mParameters.getDescription(fieldNames.getSecond());
          responseTimeGraph.addDataSeries(series, summary.computeResponseTimeData());
          responseTimeGraph.setErrors(series, summary.collectErrors());
          // add graph for method call
          for (Map.Entry<String, SummaryStatistics> entry :
              summary.getStatisticsPerMethod().entrySet()) {
            final String method = entry.getKey();
            final LineGraph.Data responseTimeData = entry.getValue().computeTimeData();

            if (!responseTimeGraphPerMethod.containsKey(method)) {
              responseTimeGraphPerMethod.put(method,
                  new LineGraph(operation + " - Response Time (ms) " + method, subTitle,
                      "Percentile", "Response Time (ms)"));
            }
            responseTimeGraphPerMethod.get(method).addDataSeries(series, responseTimeData);
            // collect max success for each method
            methodCounts.put(method,
                Math.max(methodCounts.getOrDefault(method, 0L), entry.getValue().mNumSuccess));
          }
        }

        // add the api count graph
        BarGraph maxGraph = new BarGraph(operation + " - Max API Calls", subTitle, "# API calls");
        for (Map.Entry<String, Long> entry : methodCounts.entrySet()) {
          BarGraph.Data data = new BarGraph.Data();
          data.addData(entry.getValue());
          maxGraph.addDataSeries(entry.getKey(), data);
        }
        graphs.add(maxGraph);

        for (LineGraph graph : responseTimeGraphPerMethod.values()) {
          graphs.add(graph);
        }
      }

      return graphs;
    }
  }
}
