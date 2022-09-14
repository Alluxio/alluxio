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

package alluxio.stress.client;

import alluxio.Constants;
import alluxio.collections.Pair;
import alluxio.stress.BaseParameters;
import alluxio.stress.Parameters;
import alluxio.stress.Summary;
import alluxio.stress.TaskResult;
import alluxio.stress.common.SummaryStatistics;
import alluxio.stress.graph.Graph;
import alluxio.stress.graph.LineGraph;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Splitter;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The task result for the master stress tests.
 */
public final class ClientIOTaskResult implements TaskResult, Summary {
  private long mRecordStartMs;
  private long mEndMs;
  private Map<Integer, ThreadCountResult> mThreadCountResults;
  private BaseParameters mBaseParameters;
  private ClientIOParameters mParameters;

  private Map<Integer, Map<String, SummaryStatistics>> mTimeToFirstByte;

  /**
   * Creates an instance.
   */
  public ClientIOTaskResult() {
    // Default constructor required for json deserialization
    mThreadCountResults = new HashMap<>();
    mTimeToFirstByte = new HashMap<>();
  }

  @Override
  public BaseParameters getBaseParameters() {
    return mBaseParameters;
  }

  /**
   * @param baseParameters the base parameters
   */
  public void setBaseParameters(BaseParameters baseParameters) {
    mBaseParameters = baseParameters;
  }

  /**
   * @return the parameters
   */
  public ClientIOParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the parameters
   */
  public void setParameters(ClientIOParameters parameters) {
    mParameters = parameters;
  }

  /**
   * @return the start time (in ms)
   */
  public long getRecordStartMs() {
    return mRecordStartMs;
  }

  /**
   * @param recordStartMs the start time (in ms)
   */
  public void setRecordStartMs(long recordStartMs) {
    mRecordStartMs = recordStartMs;
  }

  /**
   * @return client IO statistics per method
   */
  public Map<Integer, Map<String, SummaryStatistics>> getTimeToFirstBytePerThread() {
    return mTimeToFirstByte;
  }

  /**
   * @param timeToFirstByte time to first statistics
   */
  public void setTimeToFirstBytePerThread(Map<Integer, Map<String,
      SummaryStatistics>> timeToFirstByte) {
    mTimeToFirstByte = timeToFirstByte;
  }

  /**
   * @param numThreads thread count
   * @param statistics ClientIOTaskResultStatistics
   */
  public void putTimeToFirstBytePerThread(Integer numThreads,
      Map<String, SummaryStatistics> statistics) {
    mTimeToFirstByte.put(numThreads, statistics);
  }

  /**
   * @return the end time (in ms)
   */
  public long getEndMs() {
    return mEndMs;
  }

  /**
   * @param endMs the end time (in ms)
   */
  public void setEndMs(long endMs) {
    mEndMs = endMs;
  }

  /**
   * @return the map of thread counts to results
   */
  public Map<Integer, ThreadCountResult> getThreadCountResults() {
    return mThreadCountResults;
  }

  /**
   * @param threadCountResults the map of thread counts to results
   */
  public void setThreadCountResults(Map<Integer, ThreadCountResult> threadCountResults) {
    mThreadCountResults = threadCountResults;
  }

  /**
   * @param threadCount the thread count of the results
   * @param threadCountResult the results to add
   */
  public void addThreadCountResults(int threadCount, ThreadCountResult threadCountResult) {
    mThreadCountResults.put(threadCount, threadCountResult);
  }

  private long computeLastEndMs() {
    long endMs = 0;
    for (ThreadCountResult result : mThreadCountResults.values()) {
      endMs = Math.max(endMs, result.getEndMs());
    }
    return endMs;
  }

  private LineGraph.Data getThroughputData() {
    LineGraph.Data data = new LineGraph.Data();
    for (Map.Entry<Integer, ThreadCountResult> entry : mThreadCountResults.entrySet()) {
      data.addData(entry.getKey(), entry.getValue().getIOMBps());
    }
    return data;
  }

  private void getNumSuccessData(String series, LineGraph lineGraph) {
    Map<String, LineGraph.Data> data = new HashMap<>();

    for (Map.Entry<Integer, Map<String, SummaryStatistics>> threadEntry :
        mTimeToFirstByte.entrySet()) {
      for (Map.Entry<String, SummaryStatistics> methodEntry :
          threadEntry.getValue().entrySet()) {
        String prefix = series + ", method: " + methodEntry.getKey();
        LineGraph.Data currentData = data.getOrDefault(prefix, new LineGraph.Data());
        currentData.addData(threadEntry.getKey(), methodEntry.getValue().mNumSuccess);
        data.put(prefix, currentData);
      }
    }

    for (Map.Entry<String, LineGraph.Data> entry : data.entrySet()) {
      lineGraph.addDataSeries(entry.getKey(), entry.getValue());
    }
  }

  private void getTimeToFistByteData(String series, LineGraph lineGraph) {
    for (Map.Entry<Integer, Map<String, SummaryStatistics>> threadEntry :
        mTimeToFirstByte.entrySet()) {
      for (Map.Entry<String, SummaryStatistics> methodEntry :
          threadEntry.getValue().entrySet()) {
        lineGraph.addDataSeries(series
            + ", method: " + methodEntry.getKey()
            + ", thread: " + threadEntry.getKey(), methodEntry.getValue().computeTimeData());
      }
    }
  }

  @Override
  public List<String> getErrors() {
    List<String> errors = new ArrayList<>();
    for (Map.Entry<Integer, ThreadCountResult> entry : mThreadCountResults.entrySet()) {
      // add all the errors for this thread count, with the thread count appended to prefix
      errors.addAll(
          entry.getValue().getErrors().stream().map(err -> entry.getKey().toString() + ": " + err)
              .collect(Collectors.toList()));
    }
    return errors;
  }

  @Override
  public TaskResult.Aggregator aggregator() {
    return new Aggregator();
  }

  private static final class Aggregator implements TaskResult.Aggregator<ClientIOTaskResult> {
    @SuppressWarnings("checkstyle:OperatorWrap")
    @Override
    public ClientIOSummary aggregate(Iterable<ClientIOTaskResult> results) throws Exception {
      long recordStartMs = 0;
      long endMs = 0;
      ClientIOParameters clientIOParameters = null;
      BaseParameters baseParameters = null;
      Map<String, ClientIOTaskResult> nodes = new HashMap<>();
      for (ClientIOTaskResult taskResult: results) {
        clientIOParameters = taskResult.getParameters();
        baseParameters = taskResult.getBaseParameters();
        String jobWorkerUniqueId = taskResult.getBaseParameters().mId;
        nodes.put(jobWorkerUniqueId, taskResult);
      }

      Map<Integer, Long> threadCountIOBytes = new HashMap<>();
      Map<Integer, Long> threadCountRecordedStartMs = new HashMap<>();
      Map<Integer, Long> threadCountEndMs = new HashMap<>();
      for (ClientIOTaskResult taskResult: results) {
        for (Map.Entry<Integer, ThreadCountResult> entry :
            taskResult.getThreadCountResults().entrySet()) {
          int numThreads = entry.getKey();
          ThreadCountResult result = entry.getValue();
          long ioBytes = result.getIOBytes();
          threadCountIOBytes.put(numThreads,
              threadCountIOBytes.getOrDefault(numThreads, (long) 0) + ioBytes);
          if (threadCountRecordedStartMs.containsKey(numThreads)) {
            threadCountRecordedStartMs.put(numThreads,
                Math.min(threadCountRecordedStartMs.get(numThreads), result.getRecordStartMs()));
          } else {
            threadCountRecordedStartMs.put(numThreads, result.getRecordStartMs());
          }
          if (threadCountEndMs.containsKey(numThreads)) {
            threadCountEndMs.put(numThreads,
                Math.max(threadCountEndMs.get(numThreads), result.getEndMs()));
          } else {
            threadCountEndMs.put(numThreads, result.getEndMs());
          }
        }
      }
      Map<Integer, Float> threadCountIoMbps = new HashMap<>();
      for (Map.Entry<Integer, Long> threadCountIOBytesEntry: threadCountIOBytes.entrySet()) {
        int numThreads = threadCountIOBytesEntry.getKey();
        long ioBytes = threadCountIOBytesEntry.getValue();
        threadCountIoMbps.put(numThreads, (float) ioBytes / 1000
            / (threadCountEndMs.get(numThreads) - threadCountRecordedStartMs.get(numThreads)));
      }

      return new ClientIOSummary(clientIOParameters, baseParameters, nodes, threadCountIoMbps);
    }
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
      // expecting ClientIOTaskResult, or will throw ClassCastException
      List<ClientIOTaskResult> summaries =
          results.stream().map(x -> (ClientIOTaskResult) x).collect(Collectors.toList());

      if (!summaries.isEmpty()) {
        // first() is the list of common field names, second() is the list of unique field names
        Pair<List<String>, List<String>> fieldNames = Parameters.partitionFieldNames(
            summaries.stream().map(x -> x.mParameters).collect(Collectors.toList()));

        // Split up common description into 100 character chunks, for the subtitle
        List<String> subTitle = new ArrayList<>(Splitter.fixedLength(100).splitToList(
            summaries.get(0).mParameters.getDescription(fieldNames.getFirst())));

        for (ClientIOTaskResult summary : summaries) {
          String series = summary.mParameters.getDescription(fieldNames.getSecond());
          subTitle.add(series + ": " + DateFormat.getDateTimeInstance()
              .format(summary.computeLastEndMs()));
        }
        ClientIOOperation operation = summaries.get(0).getParameters().mOperation;
        LineGraph responseTimeGraph = new LineGraph(String
            .format("%s - %s - Throughput", operation,
                summaries.get(0).mParameters.getDescription(
                    Collections.singletonList(ClientIOParameters.FIELD_READ_RANDOM))),
            subTitle, "# Threads", "Throughput (MB/s)");

        LineGraph numSuccessGraph = new LineGraph(String
            .format("%s - %s - API calls", operation,
                summaries.get(0).mParameters.getDescription(
                    Collections.singletonList(ClientIOParameters.FIELD_READ_RANDOM))),
            subTitle, "# Threads", "# API calls");

        LineGraph timeToFirstByteGraph = new LineGraph(String
            .format("%s - %s - Time To First Byte", operation,
                summaries.get(0).mParameters.getDescription(
                    Collections.singletonList(ClientIOParameters.FIELD_READ_RANDOM))),
            subTitle, "# Threads", "Time To First Byte (Ms)");

        for (ClientIOTaskResult summary : summaries) {
          String series = summary.mParameters.getDescription(fieldNames.getSecond());
          responseTimeGraph.addDataSeries(series, summary.getThroughputData());
          responseTimeGraph.setErrors(series, summary.getErrors());

          summary.getNumSuccessData(series, numSuccessGraph);

          summary.getTimeToFistByteData(series, timeToFirstByteGraph);
        }
        graphs.add(responseTimeGraph);
        graphs.add(numSuccessGraph);
        graphs.add(timeToFirstByteGraph);
      }
      return graphs;
    }
  }

  /**
   * A result for a single thread count test.
   */
  public static final class ThreadCountResult {
    private long mRecordStartMs;
    private long mEndMs;
    private long mIOBytes;
    private List<String> mErrors;

    /**
     * Creates an instance.
     */
    public ThreadCountResult() {
      // Default constructor required for json deserialization
      mErrors = new ArrayList<>();
    }

    /**
     * Merges (updates) a result with this result.
     *
     * @param result  the result to merge
     */
    public void merge(ClientIOTaskResult.ThreadCountResult result) {
      mRecordStartMs = Math.min(mRecordStartMs, result.mRecordStartMs);
      mEndMs = Math.max(mEndMs, result.mEndMs);
      mIOBytes += result.mIOBytes;
      mErrors.addAll(result.mErrors);
    }

    /**
     * @return the duration (in ms)
     */
    public long getDurationMs() {
      return mEndMs - mRecordStartMs;
    }

    /**
     * @param durationMs the duration (in ms)
     */
    @JsonIgnore
    public void setDurationMs(long durationMs) {
      // ignore
    }

    /**
     * @return bytes of IO
     */
    public long getIOBytes() {
      return mIOBytes;
    }

    /**
     * Increments the bytes of IO an amount.
     *
     * @param ioBytes the amount to increment by
     */
    public void incrementIOBytes(long ioBytes) {
      mIOBytes += ioBytes;
    }

    /**
     * @param ioBytes bytes of IO
     */
    public void setIOBytes(long ioBytes) {
      mIOBytes = ioBytes;
    }

    /**
     * @return the start time (in ms)
     */
    public long getRecordStartMs() {
      return mRecordStartMs;
    }

    /**
     * @param recordStartMs the start time (in ms)
     */
    public void setRecordStartMs(long recordStartMs) {
      mRecordStartMs = recordStartMs;
    }

    /**
     * @return the end time (in ms)
     */
    public long getEndMs() {
      return mEndMs;
    }

    /**
     * @param endMs the end time (in ms)
     */
    public void setEndMs(long endMs) {
      mEndMs = endMs;
    }

    /**
     * @return the list of errors
     */
    public List<String> getErrors() {
      return mErrors;
    }

    /**
     * @param errors the list of errors
     */
    public void setErrors(List<String> errors) {
      mErrors = errors;
    }

    /**
     * @param errMesssage the error message to add
     */
    public void addErrorMessage(String errMesssage) {
      mErrors.add(errMesssage);
    }

    /**
     * @return the throughput (MB/s)
     */
    public float getIOMBps() {
      return ((float) mIOBytes / getDurationMs()) * 1000.0f / Constants.MB;
    }

    /**
     * @param ioMBps the throughput (MB / s)
     */
    @JsonIgnore
    public void setIOMBps(float ioMBps) {
      // ignore
    }
  }
}
