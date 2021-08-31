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

package alluxio.stress.rpc;

import alluxio.stress.BaseParameters;
import alluxio.stress.GraphGenerator;
import alluxio.stress.Summary;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.math.Quantiles;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This object is used to summarize the RPC stress test results.
 */
public class RpcTaskSummary implements Summary {
  private List<RpcTaskResult.Point> mPoints;
  private List<String> mErrors;
  private BaseParameters mBaseParameters;
  private RpcBenchParameters mParameters;
  public long mCount;
  public long mTotalDurationMs;
  public double mAvgDurationMs;
  public double mPercentile5th;
  public double mPercentile25th;
  public double mMedian;
  public double mPercentile75th;
  public double mPercentile95th;

  /**
   * Used for deserialization.
   * */
  @JsonCreator
  public RpcTaskSummary() {}

  /**
   * Creates a summary from a task result.
   * @param r task result
   */
  public RpcTaskSummary(RpcTaskResult r) {
    mParameters = r.getParameters();
    mBaseParameters = r.getBaseParameters();
    mErrors = r.getErrors();
    mPoints = r.getPoints();
    mCount = mPoints.size();
    calculate();
  }

  private void calculate() {
    for (RpcTaskResult.Point p : mPoints) {
      mTotalDurationMs += p.mDurationMs;
    }
    mAvgDurationMs = (mCount == 0) ? 0.0 : mTotalDurationMs / ((double) mCount);
    Map<Integer, Double> percentiles = getPercentiles(5, 25, 50, 75, 95);
    mPercentile5th = percentiles.get(5);
    mPercentile25th = percentiles.get(25);
    mMedian = percentiles.get(50);
    mPercentile75th = percentiles.get(75);
    mPercentile95th = percentiles.get(95);
  }

  @Override
  public GraphGenerator graphGenerator() {
    return null;
  }

  @Override
  public String toString() {
    return String.format("RpcTaskSummary: Data points: %d, Errors: %d%n"
        + "Total: %.3e, Average: %.3e, Median: %.3e%n"
        + "5 Percentile: %.3e%n25 Percentile: %.3e%n75 Percentile: %.3e%n95 Percentile: %.3e%n",
        mPoints.size(), mErrors.size(), (double) mTotalDurationMs, mAvgDurationMs, mMedian,
        mPercentile5th, mPercentile25th, mPercentile75th, mPercentile95th);
  }

  /**
   * Gets percentiles of the results at different indices.
   * @param indices the indices of the percentiles, e.g. 50th or 75th
   * @return a map of indices to percentiles
   */
  public Map<Integer, Double> getPercentiles(int... indices) {
    indices = Arrays.stream(indices)
        .map((index) -> Math.max(0, Math.min(100, index)))
        .toArray();
    return Quantiles
        .percentiles()
        .indexes(indices)
        .compute(mPoints.stream().map((p) -> p.mDurationMs).collect(Collectors.toList()));
  }

  /**
   * @return the points recorded
   */
  public List<RpcTaskResult.Point> getPoints() {
    return mPoints;
  }

  /**
   * @param points the data points
   */
  public void setPoints(List<RpcTaskResult.Point> points) {
    mPoints = points;
  }

  /**
   * @return the errors recorded
   */
  public List<String> getErrors() {
    return mErrors;
  }

  /**
   * @param errors the errors
   */
  public void setErrors(List<String> errors) {
    mErrors = errors;
  }

  /**
   * @return the {@link BaseParameters}
   */
  public BaseParameters getBaseParameters() {
    return mBaseParameters;
  }

  /**
   * @param baseParameters the {@link BaseParameters}
   */
  public void setBaseParameters(BaseParameters baseParameters) {
    mBaseParameters = baseParameters;
  }

  /**
   * @return the task specific {@link RpcBenchParameters}
   */
  @Nullable
  public RpcBenchParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the {@link RpcBenchParameters}
   */
  public void setParameters(RpcBenchParameters parameters) {
    mParameters = parameters;
  }
}
