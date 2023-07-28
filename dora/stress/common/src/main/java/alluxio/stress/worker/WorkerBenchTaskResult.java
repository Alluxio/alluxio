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

import alluxio.stress.BaseParameters;
import alluxio.stress.TaskResult;

import java.util.*;

/**
 * The task results for the worker stress test.
 */
public final class WorkerBenchTaskResult implements TaskResult {
  private BaseParameters mBaseParameters;
  private WorkerBenchParameters mParameters;

  private long mRecordStartMs;
  private long mEndMs;
  private long mIOBytes;
  private List<String> mErrors;
  private List<WorkerBenchDataPoint> mDataPoints;

  /**
   * Creates an instance.
   */
  public WorkerBenchTaskResult() {
    // Default constructor required for json deserialization
    mErrors = new ArrayList<>();
    mDataPoints = new ArrayList<>();
  }

  /**
   * Merges (updates) a task result with this result.
   *
   * @param result  the task result to merge
   */
  public void merge(WorkerBenchTaskResult result) throws Exception {
    // When merging results within a node, we need to merge all the error information.
    mErrors.addAll(result.mErrors);
    mDataPoints.addAll(result.mDataPoints);
    aggregateByWorker(result);
  }

  /**
   * Merges (updates) a task result with this result except the error information.
   *
   * @param result  the task result to merge
   */
  public void aggregateByWorker(WorkerBenchTaskResult result) {
    // When merging result from different workers, we don't need to merge the error information
    // since we will keep all the result information in a map.
    mBaseParameters = result.mBaseParameters;
    mParameters = result.mParameters;

    mRecordStartMs = result.mRecordStartMs;
    // mRecordStartMs = Math.min(mRecordStartMs, result.mRecordStartMs);
    mEndMs = Math.max(mEndMs, result.mEndMs);
    mIOBytes += result.mIOBytes;
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
   * @param errMessage the error message to add
   */
  public void addErrorMessage(String errMessage) {
    mErrors.add(errMessage);
  }

  public List<WorkerBenchDataPoint> getDataPoints() {
    return mDataPoints;
  }

  public void addDataPoint(WorkerBenchDataPoint stats) {
    mDataPoints.add(stats);
  }

  public void addDataPoints(Collection<WorkerBenchDataPoint> stats) {
    mDataPoints.addAll(stats);
  }

  @Override
  public TaskResult.Aggregator aggregator() {
    return new Aggregator();
  }

  private static final class Aggregator implements TaskResult.Aggregator<WorkerBenchTaskResult> {
    @Override
    public WorkerBenchSummary aggregate(Iterable<WorkerBenchTaskResult> results) throws Exception {
      Map<String, WorkerBenchTaskResult> nodes = new HashMap<>();

      WorkerBenchTaskResult mergedTaskResult = new WorkerBenchTaskResult();

      for (WorkerBenchTaskResult result : results) {
        nodes.put(result.getBaseParameters().mId, result);
        mergedTaskResult.merge(result);
      }

      return new WorkerBenchSummary(mergedTaskResult, nodes);
    }
  }
}
