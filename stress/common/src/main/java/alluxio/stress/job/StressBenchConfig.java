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

package alluxio.stress.job;

import alluxio.job.plan.PlanConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Config for the stress test.
 */
@ThreadSafe
public final class StressBenchConfig implements PlanConfig {
  private static final long serialVersionUID = 7883915266950426997L;
  private static final String NAME = "StressBench";

  private final String mClassName;
  private final List<String> mArgs;
  private final long mStartDelayMs;
  private final int mClusterLimit;

  /**
   * @param className the class name of the benchmark to run
   * @param args the args for the benchmark
   * @param startDelayMs the start delay for the distributed tasks, in ms
   * @param clusterLimit the max number of workers to run on. If 0, run on entire cluster,
   *                     If < 0, starts scheduling from the end of the worker list
   */
  public StressBenchConfig(@JsonProperty("className") String className,
      @JsonProperty("args") List<String> args,
      @JsonProperty("startDelayMs") long startDelayMs,
      @JsonProperty("clusterLimit") int clusterLimit) {
    mClassName = Preconditions.checkNotNull(className, "className");
    mArgs = Preconditions.checkNotNull(args, "args");
    mStartDelayMs = startDelayMs;
    mClusterLimit = clusterLimit;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Collection<String> affectedPaths() {
    return Collections.EMPTY_LIST;
  }

  /**
   * @return the class name
   */
  public String getClassName() {
    return mClassName;
  }

  /**
   * @return the list of arguments
   */
  public List<String> getArgs() {
    return mArgs;
  }

  /**
   * @return the start delay in ms
   */
  public long getStartDelayMs() {
    return mStartDelayMs;
  }

  /**
   * @return the number of workers to run on. If < 0, starts from the end of the worker list
   *         If == 0, run on entire cluster
   */
  public int getClusterLimit() {
    return mClusterLimit;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof StressBenchConfig)) {
      return false;
    }
    StressBenchConfig that = (StressBenchConfig) obj;
    return Objects.equal(mClassName, that.mClassName)
        && Objects.equal(mArgs, that.mArgs)
        && Objects.equal(mStartDelayMs, that.mStartDelayMs)
        && Objects.equal(mClusterLimit, that.mClusterLimit);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mClassName, mArgs, mStartDelayMs, mClusterLimit);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("className", mClassName)
        .add("args", mArgs)
        .add("startDelayMs", mStartDelayMs)
        .add("clusterLimit", mClusterLimit)
        .toString();
  }
}
