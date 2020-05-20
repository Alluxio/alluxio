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

import alluxio.Constants;
import alluxio.stress.BaseParameters;
import alluxio.stress.TaskResult;

import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

/**
 * The task result for the master stress tests.
 */
public final class MasterBenchTaskResult implements TaskResult {

  private long mRecordStartMs;
  private long mEndMs;
  private long mDurationMs;
  private BaseParameters mBaseParameters;
  private MasterBenchParameters mParameters;
  private List<String> mErrors;

  private MasterBenchTaskResultStatistics mResultStatistics;

  private Map<String, MasterBenchTaskResultStatistics> mStatisticsPerMethod;

  /**
   * Creates an instance.
   */
  public MasterBenchTaskResult() {
    // Default constructor required for json deserialization
    mErrors = new ArrayList<>();
    mResultStatistics = new MasterBenchTaskResultStatistics();
    mStatisticsPerMethod = new HashMap<>();
  }

  /**
   * Merges (updates) a task result with this result.
   *
   * @param result  the task result to merge
   */
  public void merge(MasterBenchTaskResult result) throws Exception {
    mResultStatistics.merge(result.mResultStatistics);

    mRecordStartMs = result.mRecordStartMs;
    if (result.mEndMs > mEndMs) {
      mEndMs = result.mEndMs;
    }
    mBaseParameters = result.mBaseParameters;
    mParameters = result.mParameters;
    mErrors.addAll(result.mErrors);

    for (Map.Entry<String, MasterBenchTaskResultStatistics> entry :
        result.mStatisticsPerMethod.entrySet()) {
      final String key = entry.getKey();
      final MasterBenchTaskResultStatistics value = entry.getValue();

      if (!mStatisticsPerMethod.containsKey(key)) {
        mStatisticsPerMethod.put(key, value);
      } else {
        mStatisticsPerMethod.get(key).merge(value);
      }
    }
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
   * @return number of successes
   */
  public long getNumSuccess() {
    return mResultStatistics.mNumSuccess;
  }

  /**
   * Increments the number of successes by an amount.
   *
   * @param numSuccess the amount to increment by
   */
  public void incrementNumSuccess(long numSuccess) {
    mResultStatistics.mNumSuccess += numSuccess;
  }

  /**
   * @param numSuccess number of successes
   */
  public void setNumSuccess(long numSuccess) {
    mResultStatistics.mNumSuccess = numSuccess;
  }

  /**
   * @return the raw response time data
   */
  public byte[] getResponseTimeNsRaw() {
    return mResultStatistics.mResponseTimeNsRaw;
  }

  public void encodeResponseTimeNsRaw(Histogram responseTimeNs) {
    mResultStatistics.encodeResponseTimeNsRaw(responseTimeNs);
  }

  /**
   * @param responseTimeNsRaw the raw response time data
   */
  public void setResponseTimeNsRaw(byte[] responseTimeNsRaw) {
    mResultStatistics.mResponseTimeNsRaw = responseTimeNsRaw;
  }

  /**
   * @return the base parameters
   */
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
   * @return the array of max response times (in ns)
   */
  public long[] getMaxResponseTimeNs() {
    return mResultStatistics.mMaxResponseTimeNs;
  }

  /**
   * @param maxResponseTimeNs the array of max response times (in ns)
   */
  public void setMaxResponseTimeNs(long[] maxResponseTimeNs) {
    mResultStatistics.mMaxResponseTimeNs = maxResponseTimeNs;
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

  public MasterBenchTaskResultStatistics getResultStatistics() {
    return mResultStatistics;
  }

  public void setResultStatistics(MasterBenchTaskResultStatistics resultStatistics) {
    this.mResultStatistics = mResultStatistics;
  }

  public Map<String, MasterBenchTaskResultStatistics> getStatisticsPerMethod() {
    return mStatisticsPerMethod;
  }

  public void setStatisticsPerMethod(Map<String, MasterBenchTaskResultStatistics> statisticsPerMethod) {
    this.mStatisticsPerMethod = statisticsPerMethod;
  }

  public void putStatisticsForMethod(String method, MasterBenchTaskResultStatistics statistics) {
    mStatisticsPerMethod.put(method, statistics);
  }

  @Override
  public TaskResult.Aggregator aggregator() {
    return new Aggregator();
  }

  private static final class Aggregator implements TaskResult.Aggregator {
    @Override
    public MasterBenchSummary aggregate(Iterable<TaskResult> results) throws Exception {
      List<String> nodes = new ArrayList<>();
      Map<String, List<String>> errors = new HashMap<>();

      MasterBenchTaskResult mergingTaskResult = null;

      for (TaskResult taskResult : results) {
        if (!(taskResult instanceof MasterBenchTaskResult)) {
          throw new IOException(
              "TaskResult is not of type MasterBenchTaskResult. class: " + taskResult.getClass()
                  .getName());
        }
        MasterBenchTaskResult result = (MasterBenchTaskResult) taskResult;
        nodes.add(result.getBaseParameters().mId);
        if (!result.getErrors().isEmpty()) {
          List<String> errorList = new ArrayList<>(result.getErrors());
          errors.put(result.getBaseParameters().mId, errorList);
        }

        if (mergingTaskResult == null) {
          mergingTaskResult = result;
          continue;
        }
        mergingTaskResult.merge(result);
      }

      return new MasterBenchSummary(mergingTaskResult, nodes, errors);
    }
  }
}
