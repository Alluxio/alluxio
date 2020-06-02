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
import alluxio.stress.JsonSerializable;
import alluxio.stress.Summary;
import alluxio.stress.TaskResult;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.ArrayList;
import java.util.List;

/**
 * Task result for the UFS I/O test.
 * */
@NotThreadSafe
public class IOTaskResult implements TaskResult {
  private List<Point> mPoints;
  private List<String> mErrors;
  private BaseParameters mBaseParameters;
  private WorkerBenchParameters mParameters;

  /**
   * An empty constructor.
   * */
  public IOTaskResult() {
    mPoints = new ArrayList<>();
    mErrors = new ArrayList<>();
  }

  /**
   * The constructor used for serialization.
   *
   * @param points the points
   * @param errors the errors
   * */
  @JsonCreator
  public IOTaskResult(@JsonProperty("points") List<Point> points,
                      @JsonProperty("errors") List<String> errors) {
    mPoints = points;
    mErrors = errors;
  }

  /**
   * @param p the point to add
   * */
  public void addPoint(Point p) {
    mPoints.add(p);
  }

  /**
   * @return the points
   * */
  public List<Point> getPoints() {
    return mPoints;
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

  /**
   * @param points the points
   */
  public void setPoints(List<Point> points) {
    mPoints = points;
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
   * @return the {@link WorkerBenchParameters}
   * */
  public WorkerBenchParameters getParameters() {
    return mParameters;
  }

  /**
   * @param parameters the {@link WorkerBenchParameters} to use
   * */
  public void setParameters(WorkerBenchParameters parameters) {
    mParameters = parameters;
  }

  /**
   * Merge another result into itself.
   * Combine all the numbers.
   *
   * @param anotherResult another {@link IOTaskResult}
   * @return self
   * */
  public IOTaskResult merge(IOTaskResult anotherResult) {
    mPoints.addAll(anotherResult.getPoints());
    mErrors.addAll(anotherResult.getErrors());
    mBaseParameters = anotherResult.getBaseParameters();
    mParameters = anotherResult.getParameters();
    return this;
  }

  /**
   * Reduce a list of {@link IOTaskResult} into one.
   *
   * @param results a list of results to combine
   * @return the combined result
   * */
  public static IOTaskResult reduceList(Iterable<IOTaskResult> results) {
    IOTaskResult aggreResult = new IOTaskResult();
    for (IOTaskResult r : results) {
      aggreResult.merge(r);
    }
    return aggreResult;
  }

  @Override
  public String toString() {
    return String.format("Points=%s, Errors=%s",
            mPoints, mErrors);
  }

  /**
   * An object representation of a successful I/O operation to the UFS.
   * */
  public static class Point implements JsonSerializable {
    public IOMode mMode;
    public double mDuration;
    public int mDataSizeMB;

    /**
     * @param mode the I/O mode
     * @param duration the time taken
     * @param dataSize the size of I/O in MB
     * */
    @JsonCreator
    public Point(@JsonProperty("mode") IOMode mode,
                 @JsonProperty("duration") double duration,
                 @JsonProperty("dataSizeMB") int dataSize) {
      mMode = mode;
      mDuration = duration;
      mDataSizeMB = dataSize;
    }

    @Override
    public String toString() {
      return String.format("{mode=%s, duration=%ss, dataSize=%sMB}",
              mMode, mDuration, mDataSizeMB);
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Point)) {
        return false;
      }
      Point b = (Point) other;
      return this.mMode == b.mMode && this.mDataSizeMB == b.mDataSizeMB
              && this.mDuration == b.mDuration;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mMode, mDataSizeMB, mDuration);
    }
  }

  @Override
  public TaskResult.Aggregator aggregator() {
    return new IOTaskResult.Aggregator();
  }

  private static final class Aggregator implements TaskResult.Aggregator<IOTaskResult> {
    @Override
    public Summary aggregate(Iterable<IOTaskResult> results) throws Exception {
      return new IOTaskSummary(reduceList(results));
    }
  }

  public enum IOMode {
    READ,
    WRITE
  }
}
