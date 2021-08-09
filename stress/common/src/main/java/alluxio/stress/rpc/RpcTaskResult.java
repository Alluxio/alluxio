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
import alluxio.stress.Parameters;
import alluxio.stress.Summary;
import alluxio.stress.TaskResult;
import alluxio.util.JsonSerializable;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;

import java.util.ArrayList;
import java.util.List;

/**
 * This object holds the results from one RPC benchmark test run, containing all
 * successful and failed RPCs.
 * For one successful run, the result is a data point.
 * For one failed run, the result is an error.
 */
public class RpcTaskResult implements TaskResult {
  private List<Point> mPoints;
  private List<String> mErrors;
  private BaseParameters mBaseParameters;
  private Parameters mParameters;

  /**
   * Constructor.
   */
  public RpcTaskResult() {
    mPoints = new ArrayList<>();
    mErrors = new ArrayList<>();
  }

  /**
   * @return the {@link BaseParameters}
   */
  public BaseParameters getBaseParameters() {
    return mBaseParameters;
  }

  /**
   * @param baseParameters the {@link BaseParameters} to use
   */
  public void setBaseParameters(BaseParameters baseParameters) {
    mBaseParameters = baseParameters;
  }

  /**
   * @return the {@link Parameters}
   */
  public Parameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the {@link Parameters} to use
   */
  public void setParameters(Parameters parameters) {
    mParameters = parameters;
  }

  /**
   * @param errorMsg an error msg to add
   */
  public void addError(String errorMsg) {
    mErrors.add(errorMsg);
  }

  /**
   * @return all the error messages
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
   * @param p the data point to add to the results
   */
  public void addPoint(Point p) {
    mPoints.add(p);
  }

  /**
   * @return all data points from successful RPCs
   */
  public List<Point> getPoints() {
    return mPoints;
  }

  /**
   *
   * @param points data points
   */
  public void setPoints(List<Point> points) {
    mPoints = points;
  }

  @Override
  public Aggregator aggregator() {
    return new Aggregator();
  }

  /**
   * @param r another result to merge into this one
   */
  public void merge(RpcTaskResult r) {
    mErrors.addAll(r.mErrors);
    mPoints.addAll(r.mPoints);
  }

  public static final class Aggregator implements TaskResult.Aggregator<RpcTaskResult> {
    @Override
    public Summary aggregate(Iterable<RpcTaskResult> results) throws Exception {
      return new RpcTaskSummary(reduceList(results));
    }

    /**
     * Reduce a list of {@link RpcTaskResult} into one.
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

  /**
   * Each point stands for one successful RPC.
   */
  public static class Point implements JsonSerializable {
    public long mDurationNs;

    public Point(@JsonProperty("duration") long ns) {
      mDurationNs = ns;
    }

    @Override
    public String toString() {
      return String.format("Point: {duration: %sns}", mDurationNs);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("mPoints", mPoints)
        .add("mErrors", mErrors).toString();
  }
}
