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
import alluxio.stress.worker.UfsIOParameters;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;

public class RpcTaskSummary implements Summary {
  private List<RpcTaskResult.Point> mPoints;
  private List<String> mErrors;
  private BaseParameters mBaseParameters;
  private RegisterWorkerParameters mParameters;
  public long mCount;
  // TODO(jiacheng): calculate p50 p99
  public long mTotalDurationMs;
  public double mAvgDurationMs;

  /**
   * Used for deserialization.
   * */
  @JsonCreator
  public RpcTaskSummary() {}

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
    mAvgDurationMs = (mCount == 0) ? 0.0 : mTotalDurationMs / (mCount + 0.0000001);
  }

  @Override
  public GraphGenerator graphGenerator() {
    return null;
  }

  @Override
  public String toString() {
    return String.format("RpcTaskSummary: {Points=%s, Errors=%s}%n",
            mPoints, mErrors);
  }

  /**
   * @return the points recorded
   * */
  public List<RpcTaskResult.Point> getPoints() {
    return mPoints;
  }

  /**
   * @param points the data points
   * */
  public void setPoints(List<RpcTaskResult.Point> points) {
    mPoints = points;
  }

  /**
   * @return the errors recorded
   * */
  public List<String> getErrors() {
    return mErrors;
  }

  /**
   * @param errors the errors
   * */
  public void setErrors(List<String> errors) {
    mErrors = errors;
  }

  /**
   * @return the {@link BaseParameters}
   * */
  public BaseParameters getBaseParameters() {
    return mBaseParameters;
  }

  /**
   * @param baseParameters the {@link BaseParameters}
   * */
  public void setBaseParameters(BaseParameters baseParameters) {
    mBaseParameters = baseParameters;
  }

  /**
   * @return the task specific {@link UfsIOParameters}
   * */
  public RegisterWorkerParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the {@link UfsIOParameters}
   * */
  public void setParameters(RegisterWorkerParameters parameters) {
    mParameters = parameters;
  }
}