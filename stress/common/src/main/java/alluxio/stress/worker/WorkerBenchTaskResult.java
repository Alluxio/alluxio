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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  /**
   * Creates an instance.
   */
  public WorkerBenchTaskResult() {
    // Default constructor required for json deserialization
    mErrors = new ArrayList<>();
  }

  /**
   * Merges (updates) a task result with this result.
   *
   * @param result  the task result to merge
   */
  public void merge(WorkerBenchTaskResult result) throws Exception {
    mBaseParameters = result.mBaseParameters;
    mParameters = result.mParameters;

    mRecordStartMs = result.mRecordStartMs;
    mEndMs = Math.max(mEndMs, result.mEndMs);
    mIOBytes += result.mIOBytes;
    mErrors.addAll(result.mErrors);
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

  @Override
  public TaskResult.Aggregator aggregator() {
    return new Aggregator();
  }

  private static final class Aggregator implements TaskResult.Aggregator<WorkerBenchTaskResult> {
    @Override
    public WorkerBenchSummary aggregate(Iterable<WorkerBenchTaskResult> results) throws Exception {
      List<String> nodes = new ArrayList<>();
      Map<String, List<String>> errors = new HashMap<>();

      WorkerBenchTaskResult mergingTaskResult = new WorkerBenchTaskResult();

      for (WorkerBenchTaskResult result : results) {
        nodes.add(result.getBaseParameters().mId);
        if (!result.getErrors().isEmpty()) {
          List<String> errorList = new ArrayList<>(result.getErrors());
          errors.put(result.getBaseParameters().mId, errorList);
        }
        mergingTaskResult.merge(result);
      }

      return new WorkerBenchSummary(mergingTaskResult, nodes, errors);
    }
  }
}
