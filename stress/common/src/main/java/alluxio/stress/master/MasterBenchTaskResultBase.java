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
import alluxio.stress.common.FileSystemParameters;
import alluxio.stress.common.SummaryStatistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

/**
 * The task result for the master stress tests.
 */
public abstract class MasterBenchTaskResultBase implements TaskResult {
  protected long mRecordStartMs;
  protected long mEndMs;
  protected long mDurationMs;
  protected BaseParameters mBaseParameters;
  protected FileSystemParameters mParameters;
  protected List<String> mErrors;

  protected Map<String, MasterBenchTaskResultStatistics> mStatisticsPerMethod;

  /**
   * Creates an instance.
   */
  public MasterBenchTaskResultBase() {
    // Default constructor required for json deserialization
    mErrors = new ArrayList<>();
    mStatisticsPerMethod = new HashMap<>();
  }

  /**
   * Merges (updates) a task result with this result.
   *
   * @param result  the task result to merge
   */
  public void merge(MasterBenchTaskResultBase result) throws Exception {
    // When merging results within a node, we need to merge all the error information.
    mErrors.addAll(result.mErrors);
    aggregateByWorker(result);
  }

  abstract void mergeResultStatistics(MasterBenchTaskResultBase result) throws Exception;

  abstract long getTotalSuccess();

  abstract List<SummaryStatistics> getSummaryStatistics() throws DataFormatException;

  /**
   * Merges (updates) a task result with this result except the error information.
   *
   * @param result  the task result to merge
   */
  public void aggregateByWorker(MasterBenchTaskResultBase result) throws Exception {
    mergeResultStatistics(result);
    mRecordStartMs = result.mRecordStartMs;
    if (result.mEndMs > mEndMs) {
      mEndMs = result.mEndMs;
    }
    mBaseParameters = result.mBaseParameters;
    mParameters = result.mParameters;

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
  public FileSystemParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the parameters
   */
  public void setParameters(FileSystemParameters parameters) {
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

  @Override
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
}
