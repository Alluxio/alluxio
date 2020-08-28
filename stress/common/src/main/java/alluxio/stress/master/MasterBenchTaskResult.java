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

import alluxio.stress.BaseParameters;
import alluxio.stress.TaskResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  private MasterBenchTaskResultStatistics mStatistics;

  private Map<String, MasterBenchTaskResultStatistics> mStatisticsPerMethod;

  /**
   * Creates an instance.
   */
  public MasterBenchTaskResult() {
    // Default constructor required for json deserialization
    mErrors = new ArrayList<>();
    mStatistics = new MasterBenchTaskResultStatistics();
    mStatisticsPerMethod = new HashMap<>();
  }

  /**
   * Merges (updates) a task result with this result.
   *
   * @param result  the task result to merge
   */
  public void merge(MasterBenchTaskResult result) throws Exception {
    mStatistics.merge(result.mStatistics);

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
   * Increments the number of successes by an amount.
   *
   * @param numSuccess the amount to increment by
   */
  public void incrementNumSuccess(long numSuccess) {
    mStatistics.mNumSuccess += numSuccess;
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
    return mStatistics.mMaxResponseTimeNs;
  }

  /**
   * @param maxResponseTimeNs the array of max response times (in ns)
   */
  public void setMaxResponseTimeNs(long[] maxResponseTimeNs) {
    mStatistics.mMaxResponseTimeNs = maxResponseTimeNs;
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
   * @return the statistics
   */
  public MasterBenchTaskResultStatistics getStatistics() {
    return mStatistics;
  }

  /**
   * @param statistics the statistics
   */
  public void setStatistics(MasterBenchTaskResultStatistics statistics) {
    mStatistics = statistics;
  }

  /**
   * @return the statistics per method
   */
  public Map<String, MasterBenchTaskResultStatistics> getStatisticsPerMethod() {
    return mStatisticsPerMethod;
  }

  /**
   * @param statisticsPerMethod the statistics per method
   */
  public void setStatisticsPerMethod(Map<String, MasterBenchTaskResultStatistics>
                                         statisticsPerMethod) {
    mStatisticsPerMethod = statisticsPerMethod;
  }

  /**
   * @param method the name of the method to insert statistics for
   * @param statistics the statistics for the method
   */
  public void putStatisticsForMethod(String method, MasterBenchTaskResultStatistics statistics) {
    mStatisticsPerMethod.put(method, statistics);
  }

  @Override
  public TaskResult.Aggregator aggregator() {
    return new Aggregator();
  }

  private static final class Aggregator implements TaskResult.Aggregator<MasterBenchTaskResult> {
    @Override
    public MasterBenchSummary aggregate(Iterable<MasterBenchTaskResult> results) throws Exception {
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
