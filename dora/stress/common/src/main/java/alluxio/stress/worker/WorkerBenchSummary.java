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

package alluxio.stress.worker;

import alluxio.Constants;
import alluxio.collections.Pair;
import alluxio.stress.Parameters;
import alluxio.stress.StressConstants;
import alluxio.stress.Summary;
import alluxio.stress.common.GeneralBenchSummary;
import alluxio.stress.graph.Graph;
import alluxio.stress.graph.LineGraph;
import alluxio.util.FormatUtils;

import com.google.common.base.Splitter;
import org.HdrHistogram.Histogram;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The summary for the worker stress tests.
 */
public final class WorkerBenchSummary extends GeneralBenchSummary<WorkerBenchTaskResult> {
  private WorkerBenchParameters mParameters;

  private long mDurationMs;
  private long mEndTimeMs;
  private long mIOBytes;
  private List<Long> mThroughputPercentiles;

  /**
   * Creates an instance.
   * Default constructor required for json deserialization.
   */
  public WorkerBenchSummary() {
    mNodeResults = new HashMap<>();
    mThroughputPercentiles = new ArrayList<>();
  }

  /**
   * Creates an instance.
   *
   * @param mergedTaskResults the merged task result
   * @param nodes the list of nodes
   */
  public WorkerBenchSummary(WorkerBenchTaskResult mergedTaskResults,
      Map<String, WorkerBenchTaskResult> nodes) {
    mDurationMs = mergedTaskResults.getEndMs() - mergedTaskResults.getRecordStartMs();
    mEndTimeMs = mergedTaskResults.getEndMs();
    mIOBytes = mergedTaskResults.getIOBytes();
    mParameters = mergedTaskResults.getParameters();
    mNodeResults = nodes;
    mThroughput = getIOMBps();

    mThroughputPercentiles = new ArrayList<>();
    Histogram throughputHistogram = new Histogram(
        FormatUtils.parseSpaceSize(mParameters.mFileSize),
        StressConstants.TIME_HISTOGRAM_PRECISION);
    mergedTaskResults.getAllThroughput().forEach(throughputHistogram::recordValue);
    for (int i = 0; i <= 100; i++) {
      mThroughputPercentiles.add(throughputHistogram.getValueAtPercentile(i));
    }
  }

  /**
   * @return the throughput (MB/s)
   */
  public float getIOMBps() {
    return ((float) mIOBytes / mDurationMs) * 1000.0f / Constants.MB;
  }

  /**
   * @param ioMBps the throughput (MB / s)
   */
  public void setIOMBps(float ioMBps) {
    // ignore, since this is computed dynamically
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
  public WorkerBenchParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the parameters
   */
  public void setParameters(WorkerBenchParameters parameters) {
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
   * @return number of bytes
   */
  public long getIOBytes() {
    return mIOBytes;
  }

  /**
   * @param IOBytes the number of bytes
   */
  public void setIOBytes(long IOBytes) {
    mIOBytes = IOBytes;
  }

  /**
   * @return 0~100 percentiles of recorded durations
   */
  public List<Long> getThroughputPercentiles() {
    return mThroughputPercentiles;
  }

  /**
   * @param percentiles a list of  calculated percentiles from recorded durations
   */
  public void setThroughputPercentiles(List<Long> percentiles) {
    mThroughputPercentiles = percentiles;
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
      // only examine WorkerBenchSummary
      List<WorkerBenchSummary> summaries =
          results.stream().map(x -> (WorkerBenchSummary) x).collect(Collectors.toList());

      if (summaries.isEmpty()) {
        return graphs;
      }

      // first() is the list of common field names, second() is the list of unique field names
      Pair<List<String>, List<String>> fieldNames = Parameters.partitionFieldNames(
          summaries.stream().map(x -> x.mParameters).collect(Collectors.toList()));

      // Split up common description into 100 character chunks, for the subtitle
      List<String> subTitle = new ArrayList<>(Splitter.fixedLength(100).splitToList(
          summaries.get(0).mParameters.getDescription(fieldNames.getFirst())));

      LineGraph throughputGraph =
          new LineGraph("Worker Throughput (MB/s)", subTitle, "Total Client Threads",
              "Throughput (MB/s)");

      // remove the thread count from series fields, since the x-axis is thread counts.
      List<String> seriesFields = fieldNames.getSecond().stream()
          .filter(f -> !"mThreads".equals(f)).collect(Collectors.toList());

      // map(series name -> map(total threads -> throughput MB/s))
      Map<String, Map<Integer, Float>> allSeries = new HashMap<>();
      // map(series name -> list of errors)
      Map<String, List<String>> allSeriesErrors = new HashMap<>();

      for (WorkerBenchSummary summary : summaries) {
        String series = summary.mParameters.getDescription(seriesFields);

        // update the series data
        allSeries.compute(series, (key, value) -> {
          if (value == null) {
            value = new HashMap<>();
          }
          int totalThreads = summary.getNodeResults().size() * summary.getParameters().mThreads;
          value.put(totalThreads, summary.getIOMBps());
          return value;
        });

        // update series errors
        allSeriesErrors.compute(series, (key, value) -> {
          if (value == null) {
            value = new ArrayList<>();
          }
          value.addAll(summary.collectErrorsFromAllNodes());
          return value;
        });
      }

      // add series data to graph
      for (Map.Entry<String, Map<Integer, Float>> entry : allSeries.entrySet()) {
        LineGraph.Data seriesLine = new LineGraph.Data();
        for (Map.Entry<Integer, Float> dataPoint : entry.getValue().entrySet()) {
          seriesLine.addData(dataPoint.getKey(), dataPoint.getValue());
        }
        throughputGraph.addDataSeries(entry.getKey(), seriesLine);
      }

      // add series errors
      for (Map.Entry<String, List<String>> entry : allSeriesErrors.entrySet()) {
        throughputGraph.setErrors(entry.getKey(), entry.getValue());
      }

      graphs.add(throughputGraph);

      return graphs;
    }
  }
}
