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
import alluxio.stress.graph.BarGraph;
import alluxio.stress.graph.Graph;

import com.google.common.base.Splitter;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The summary for the max throughput test.
 */
public final class MaxThroughputSummary implements Summary {
  private float mMaxThroughput;
  private long mEndTimeMs;
  private MasterBenchParameters mParameters;
  private Map<Long, MasterBenchSummary> mPassedRuns;
  private Map<Long, MasterBenchSummary> mFailedRuns;

  /**
   * Creates an instance.
   */
  public MaxThroughputSummary() {
    // Default constructor required for json deserialization
    mPassedRuns = new HashMap<>();
    mFailedRuns = new HashMap<>();
  }

  /**
   * @return the max throughput
   */
  public float getMaxThroughput() {
    return mMaxThroughput;
  }

  /**
   * @param maxThroughput the max throughput
   */
  public void setMaxThroughput(float maxThroughput) {
    mMaxThroughput = maxThroughput;
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
   * Adds a passing run to the summary, for a target throughput.
   *
   * @param targetThroughput the target throughput
   * @param summary the summary
   */
  public void addPassedRun(long targetThroughput, MasterBenchSummary summary) {
    mPassedRuns.put(targetThroughput, summary);
  }

  /**
   * @return the Map(target throughput -> summary) of passed runs
   */
  public Map<Long, MasterBenchSummary> getPassedRuns() {
    return mPassedRuns;
  }

  /**
   * @param passedRuns the Map(target throughput -> summary) of passed runs
   */
  public void setPassedRuns(Map<Long, MasterBenchSummary> passedRuns) {
    mPassedRuns = passedRuns;
  }

  /**
   * Adds a failing run to the summary, for a target throughput.
   *
   * @param targetThroughput the target throughput
   * @param summary the summary
   */
  public void addFailedRun(long targetThroughput, MasterBenchSummary summary) {
    mFailedRuns.put(targetThroughput, summary);
  }

  /**
   * @return the Map(target throughput -> summary) of failed runs
   */
  public Map<Long, MasterBenchSummary> getFailedRuns() {
    return mFailedRuns;
  }

  /**
   * @param failedRuns the Map(target throughput -> summary) of failed runs
   */
  public void setFailedRuns(Map<Long, MasterBenchSummary> failedRuns) {
    mFailedRuns = failedRuns;
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
      // only examine MaxThroughputSummary
      List<MaxThroughputSummary> summaries =
          results.stream().map(x -> (MaxThroughputSummary) x).collect(Collectors.toList());

      // Iterate over all operations
      for (Operation operation : Operation.values()) {
        List<MaxThroughputSummary> opSummaries =
            summaries.stream().filter(x -> x.mParameters.mOperation == operation)
                .collect(Collectors.toList());

        if (!opSummaries.isEmpty()) {
          // first() is the list of common field names, second() is the list of unique field names
          Pair<List<String>, List<String>> fieldNames = Parameters.partitionFieldNames(
              opSummaries.stream().map(x -> x.mParameters).collect(Collectors.toList()));

          // Split up common description into 100 character chunks, for the sub title
          List<String> subTitle = new ArrayList<>(Splitter.fixedLength(100).splitToList(
              opSummaries.get(0).mParameters.getDescription(fieldNames.getFirst())));

          for (MaxThroughputSummary summary : opSummaries) {
            String series = summary.mParameters.getDescription(fieldNames.getSecond());
            subTitle.add(
                series + ": " + DateFormat.getDateTimeInstance().format(summary.getEndTimeMs()));
          }

          BarGraph maxGraph = new BarGraph(operation + " - Max Throughput", subTitle, "Throughput");

          for (MaxThroughputSummary summary : opSummaries) {
            String series = summary.mParameters.getDescription(fieldNames.getSecond());
            BarGraph.Data data = new BarGraph.Data();
            data.addData(summary.getMaxThroughput());
            maxGraph.addDataSeries(series, data);
          }
          graphs.add(maxGraph);

          // graph all the response times for the passing iterations
          for (MaxThroughputSummary summary : opSummaries) {
            String series = summary.mParameters.getDescription(fieldNames.getSecond());

            List<MasterBenchSummary> runs = new ArrayList<>(summary.getPassedRuns().values());
            alluxio.stress.GraphGenerator runGraphGenerator = runs.get(0).graphGenerator();
            List<Graph> runGraphs = runGraphGenerator.generate(runs);
            for (Graph graph : runGraphs) {
              List<String> newTitle = new ArrayList<>();
              newTitle.add(series);
              newTitle.addAll(graph.getTitle());
              graph.setTitle(newTitle);
            }

            graphs.addAll(runGraphs);
          }
        }
      }

      return graphs;
    }
  }
}
