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

  /**
   * @param className the class name of the benchmark to run
   * @param args the args for the benchmark
   */
  public StressBenchConfig(@JsonProperty("className") String className,
      @JsonProperty("args") List<String> args) {
    mClassName = Preconditions.checkNotNull(className, "className");
    mArgs = Preconditions.checkNotNull(args, "args");
  }

  @Override
  public String getName() {
    return NAME;
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
        && Objects.equal(mArgs, that.mArgs);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mClassName);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("command", mClassName)
        .add("args", mArgs)
        .toString();
  }
}
