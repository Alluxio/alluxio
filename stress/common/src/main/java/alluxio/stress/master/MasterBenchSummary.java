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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The summary for the master stress tests.
 */
public final class MasterBenchSummary implements Summary {
  private long mDurationMs;
  private long mNumSuccess;
  private float mThroughput;
  private long mEndTimeMs;
  private MasterBenchParameters mParameters;
  private List<String> mNodes;
  private Map<String, List<String>> mErrors;

  /** response times for all percentiles from 0 -> 100 (101 values). */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  private float[] mResponseTimePercentileMs;
  /** percentiles of just 99.x%. first entry is 99%, second is 99.9%, etc. */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  private float[] mResponseTime99PercentileMs;
  /** max response time over time, over the duration of the test. */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP2")
  private float[] mMaxResponseTimeMs;

  /**
   * Creates an instance.
   */
  public MasterBenchSummary() {
    // Default constructor required for json deserialization
  }

  /**
   * Creates an instance.
   *
   * @param durationMs the duration (in ms)
   * @param numSuccess the number of success
   * @param endTimeMs the end time (in ms)
   * @param responseTimePercentileMs the response times (in ms), for all percentiles
   * @param responseTime99PercentileMs the response times (in ms), for the 99.x percentiles
   * @param maxResponseTimeMs the max response times (in ms) over time
   * @param parameters the parameters
   * @param nodes the list of nodes
   * @param errors the list of errors
   */
  public MasterBenchSummary(long durationMs, long numSuccess, long endTimeMs,
      float[] responseTimePercentileMs, float[] responseTime99PercentileMs,
      float[] maxResponseTimeMs, MasterBenchParameters parameters, List<String> nodes,
      Map<String, List<String>> errors) {
    mDurationMs = durationMs;
    mNumSuccess = numSuccess;
    mEndTimeMs = endTimeMs;
    mResponseTimePercentileMs = responseTimePercentileMs;
    mResponseTime99PercentileMs = responseTime99PercentileMs;
    mThroughput = ((float) mNumSuccess / mDurationMs) * 1000.0f;
    mMaxResponseTimeMs = maxResponseTimeMs;
    mParameters = parameters;
    mNodes = nodes;
    mErrors = errors;
  }

  /**
   * @return the duration (in ms)
   */
  public long getDurationMs() {
    return mDurationMs;
  }

  /**
   * @return the number of successes
   */
  public long getNumSuccess() {
    return mNumSuccess;
  }

  /**
   * @return the throughput
   */
  public float getThroughput() {
    return mThroughput;
  }

  /**
   * @return the response times (in ms) for all percentiles
   */
  public float[] getResponseTimePercentileMs() {
    return mResponseTimePercentileMs;
  }

  /**
   * @return the response times (in ms) for 99.x%. first entry is 99%, second is 99.9%, etc
   */
  public float[] getResponseTime99PercentileMs() {
    return mResponseTime99PercentileMs;
  }

  /**
   * @return the list of max response times throughout the duration of the run
   */
  public float[] getMaxResponseTimeMs() {
    return mMaxResponseTimeMs;
  }

  /**
   * @param durationMs the duration (in ms)
   */
  public void setDurationMs(long durationMs) {
    mDurationMs = durationMs;
  }

  /**
   * @param numSuccess the number of successes
   */
  public void setNumSuccess(long numSuccess) {
    mNumSuccess = numSuccess;
  }

  /**
   * @param throughput the throughput
   */
  public void setThroughput(float throughput) {
    mThroughput = throughput;
  }

  /**
   * @param responseTimePercentileMs the response times (in ms) for all percentiles
   */
  public void setResponseTimePercentileMs(float[] responseTimePercentileMs) {
    mResponseTimePercentileMs = responseTimePercentileMs;
  }

  /**
   * @param responseTime99PercentileMs the response times (in ms) for 99.x%
   */
  public void setResponseTime99PercentileMs(float[] responseTime99PercentileMs) {
    mResponseTime99PercentileMs = responseTime99PercentileMs;
  }

  /**
   * @param maxResponseTimeMs the list of max response times throughout the duration of the run
   */
  public void setMaxResponseTimeMs(float[] maxResponseTimeMs) {
    mMaxResponseTimeMs = maxResponseTimeMs;
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

  private LineGraph.Data getResponseTimeData() {
    LineGraph.Data data = new LineGraph.Data();
    data.addData("50", mResponseTimePercentileMs[50]);
    data.addData("75", mResponseTimePercentileMs[75]);
    data.addData("90", mResponseTimePercentileMs[90]);
    data.addData("95", mResponseTimePercentileMs[95]);

    String percentile = "99";
    for (float ms : mResponseTime99PercentileMs) {
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
