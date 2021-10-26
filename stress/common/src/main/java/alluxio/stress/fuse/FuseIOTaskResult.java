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

package alluxio.stress.fuse;

import alluxio.Constants;
import alluxio.stress.BaseParameters;
import alluxio.stress.TaskResult;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The task result for the Fuse IO stress tests.
 */
public final class FuseIOTaskResult implements TaskResult {
  private long mRecordStartMs;
  private long mEndMs;
  private long mIOBytes;
  private List<String> mErrors;
  private BaseParameters mBaseParameters;
  private FuseIOParameters mParameters;

  /**
   * Creates an instance.
   */
  public FuseIOTaskResult() {
    mErrors = new ArrayList<>();
  }

  @Override
  public TaskResult.Aggregator aggregator() {
    return new Aggregator();
  }

  private static final class Aggregator implements TaskResult.Aggregator<FuseIOTaskResult> {
    @Override
    public FuseIOSummary aggregate(Iterable<FuseIOTaskResult> results) throws Exception {
      long recordStartMs = 0;
      long endMs = 0;
      long ioBytes = 0;
      FuseIOParameters fuseIOParameters = null;
      BaseParameters baseParameters = null;
      List<String> nodes = new ArrayList<>();
      Map<String, List<String>> errors = new HashMap<>();
      Map<String, Float> individualThroughput = new HashMap<>();

      for (FuseIOTaskResult taskResult: results) {
        recordStartMs = taskResult.getRecordStartMs();
        endMs = Math.max(endMs, taskResult.getEndMs());
        ioBytes += taskResult.getIOBytes();
        fuseIOParameters = taskResult.getParameters();
        baseParameters = taskResult.getBaseParameters();

        String jobWorkerUniqueId = taskResult.getBaseParameters().mId;
        nodes.add(jobWorkerUniqueId);
        individualThroughput.put(jobWorkerUniqueId, taskResult.getIOMBps());
        errors.put(jobWorkerUniqueId, taskResult.getErrors());
      }

      float ioMBps = (float) ioBytes / (endMs - recordStartMs) * 1000.0f / Constants.MB;

      return new FuseIOSummary(fuseIOParameters, baseParameters, nodes, errors, recordStartMs,
          endMs, ioBytes, ioMBps, individualThroughput);
    }
  }

  /**
   * Merges this thread result into the Fuse IO task result.
   *
   * @param result  the result to merge
   */
  public void merge(FuseIOTaskResult result) {
    mRecordStartMs = Math.min(mRecordStartMs, result.mRecordStartMs);
    mEndMs = Math.max(mEndMs, result.mEndMs);
    mIOBytes += result.mIOBytes;
    mErrors.addAll(result.mErrors);
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
   * @return the Fuse IO Stress Bench parameters
   */
  public FuseIOParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the Fuse IO Stress Bench parameters
   */
  public void setParameters(FuseIOParameters parameters) {
    mParameters = parameters;
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
    return Collections.unmodifiableList(mErrors);
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

  /**
   * @return the throughput (MB/s)
   */
  public float getIOMBps() {
    return ((float) mIOBytes / getDurationMs()) * 1000.0f / Constants.MB;
  }

  /**
   * @param ioMBps the throughput (MB / s)
   */
  public void setIOMBps(float ioMBps) {
    // ignore
  }
}
