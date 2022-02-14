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

import alluxio.collections.Pair;
import alluxio.stress.BaseParameters;
import alluxio.stress.Parameters;
import alluxio.stress.Summary;
import alluxio.stress.TaskResult;
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
 * abstract class for result of MaxThroughput stressBench.
 * @param <T> the stress bench parameter
 * @param <S> the Bench Summary
 */
public abstract class AbstractMaxThroughputSummary<T extends Parameters,
    S extends GeneralBenchSummary> implements Summary, TaskResult {
  private float mMaxThroughput;
  private long mEndTimeMs;
  private T mParameters;
  private Map<Long, S> mPassedRuns;
  private Map<Long, S> mFailedRuns;

  /**
   * Creates an instance.
   */
  public AbstractMaxThroughputSummary() {
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

  @Override
  public alluxio.stress.GraphGenerator graphGenerator() {
    return new GraphGenerator();
  }

  /**
   * @return the parameters
   */
  public T getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the parameters
   */
  public void setParameters(T parameters) {
    mParameters = parameters;
  }

  /**
   * Adds a passing run to the summary, for a target throughput.
   *
   * @param targetThroughput the target throughput
   * @param summary the summary
   */
  public void addPassedRun(long targetThroughput, S summary) {
    mPassedRuns.put(targetThroughput, summary);
  }

  /**
   * @return the Map(target throughput -> summary) of passed runs
   */
  public Map<Long, S> getPassedRuns() {
    return mPassedRuns;
  }

  /**
   * @param passedRuns the Map(target throughput -> summary) of passed runs
   */
  public void setPassedRuns(Map<Long, S> passedRuns) {
    mPassedRuns = passedRuns;
  }

  /**
   * Adds a failing run to the summary, for a target throughput.
   *
   * @param targetThroughput the target throughput
   * @param summary the summary
   */
  public void addFailedRun(long targetThroughput, S summary) {
    mFailedRuns.put(targetThroughput, summary);
  }

  /**
   * @return the Map(target throughput -> summary) of failed runs
   */
  public Map<Long, S> getFailedRuns() {
    return mFailedRuns;
  }

  /**
   * @param failedRuns the Map(target throughput -> summary) of failed runs
   */
  public void setFailedRuns(Map<Long, S> failedRuns) {
    mFailedRuns = failedRuns;
  }

  /**
   * The graph generator for this summary.
   */
  public static class GraphGenerator extends alluxio.stress.GraphGenerator {
    @Override
    public List<Graph> generate(List<? extends Summary> results) {
      List<Graph> graphs = new ArrayList<>();
      // only examine MaxThroughputSummary
      List<AbstractMaxThroughputSummary> summaries =
          results.stream().map(x -> (AbstractMaxThroughputSummary) x).collect(Collectors.toList());
      List<Enum> operations = summaries.stream().map(x -> x.mParameters.operation()).distinct()
          .collect(Collectors.toList());
      // Iterate over all operations
      for (Enum operation : operations) {
        List<AbstractMaxThroughputSummary> opSummaries = summaries.stream()
            .filter(x -> x.mParameters.operation() == operation).collect(Collectors.toList());

        if (!opSummaries.isEmpty()) {
          // first() is the list of common field names, second() is the list of unique field names
          Pair<List<String>, List<String>> fieldNames = Parameters.partitionFieldNames(
              opSummaries.stream().map(x -> x.mParameters).collect(Collectors.toList()));

          // Split up common description into 100 character chunks, for the sub title
          List<String> subTitle = new ArrayList<>(Splitter.fixedLength(100)
              .splitToList(opSummaries.get(0).mParameters.getDescription(fieldNames.getFirst())));

          for (AbstractMaxThroughputSummary summary : opSummaries) {
            String series = summary.mParameters.getDescription(fieldNames.getSecond());
            subTitle.add(
                series + ": " + DateFormat.getDateTimeInstance().format(summary.getEndTimeMs()));
          }

          BarGraph maxGraph = new BarGraph(operation + " - Max Throughput", subTitle, "Throughput");

          for (AbstractMaxThroughputSummary summary : opSummaries) {
            String series = summary.mParameters.getDescription(fieldNames.getSecond());
            BarGraph.Data data = new BarGraph.Data();
            data.addData(summary.getMaxThroughput());
            maxGraph.addDataSeries(series, data);
          }
          graphs.add(maxGraph);

          // graph all the response times for the passing iterations
          for (AbstractMaxThroughputSummary summary : opSummaries) {
            String series = summary.mParameters.getDescription(fieldNames.getSecond());

            List<Summary> runs = new ArrayList<>(summary.getPassedRuns().values());
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

  @Override
  public Aggregator aggregator() {
    throw new UnsupportedOperationException("This method is not implemented yet.");
  }

  @Override
  public List<String> getErrors() {
    return null;
  }

  @Override
  public BaseParameters getBaseParameters() {
    return null;
  }
}
