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
import alluxio.stress.Summary;
import alluxio.stress.TaskResult;
import alluxio.util.JsonSerializable;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import java.util.ArrayList;
import java.util.List;

public class RpcTaskResult implements TaskResult {
  private List<Point> mPoints;
  private List<String> mErrors;
  private BaseParameters mBaseParameters;
  private RegisterWorkerParameters mParameters;

  public RpcTaskResult() {
    mPoints = new ArrayList<>();
    mErrors = new ArrayList<>();
  }

  /**
   * @return the {@link BaseParameters}
   * */
  public BaseParameters getBaseParameters() {
    return mBaseParameters;
  }

  /**
   * @param baseParameters the {@link BaseParameters} to use
   * */
  public void setBaseParameters(BaseParameters baseParameters) {
    mBaseParameters = baseParameters;
  }

  /**
   * @return the {@link UfsIOParameters}
   * */
  public RegisterWorkerParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the {@link UfsIOParameters} to use
   * */
  public void setParameters(RegisterWorkerParameters parameters) {
    mParameters = parameters;
  }

  /**
   * @param errorMsg an error msg to add
   * */
  public void addError(String errorMsg) {
    mErrors.add(errorMsg);
  }

  /**
   * @return all the error messages
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

  public void addPoint(Point p) {
    mPoints.add(p);
  }

  public List<Point> getPoints() {
    return mPoints;
  }

  public void setPoints(List<Point> points) {
    mPoints = points;
  }

  @Override
  public Aggregator aggregator() {
    return new Aggregator();
  }

  public void merge(RpcTaskResult r) {
    mErrors.addAll(r.mErrors);
    mPoints.addAll(r.mPoints);
  }

  private static final class Aggregator implements TaskResult.Aggregator<RpcTaskResult> {
    @Override
    public Summary aggregate(Iterable<RpcTaskResult> results) throws Exception {
      return new RpcTaskSummary(reduceList(results));
    }

    /**
     * Reduce a list of {@link IOTaskResult} into one.
     *
     * @param results a list of results to combine
     * @return the combined result
     * */
    public static RpcTaskResult reduceList(Iterable<RpcTaskResult> results) {
      RpcTaskResult aggreResult = new RpcTaskResult();
      for (RpcTaskResult r : results) {
        aggreResult.merge(r);
      }
      return aggreResult;
    }
  }

  public static class Point implements JsonSerializable {
    public long mDurationMs;

    public Point(@JsonProperty("duration") long ms) {
      mDurationMs = ms;
    }

    @Override
    public String toString() {
      return String.format("Point: {duration: %sms}", mDurationMs);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
            .add("mPoints", mPoints)
            .add("mErrors", mErrors).toString();
  }

  public static RpcTaskResult reduceList(Iterable<RpcTaskResult> results) {
    RpcTaskResult aggreResult = new RpcTaskResult();
    for (RpcTaskResult r : results) {
      aggreResult.merge(r);
    }
    return aggreResult;
  }
}