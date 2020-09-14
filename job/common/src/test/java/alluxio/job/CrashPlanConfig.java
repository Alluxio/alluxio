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

package alluxio.job;

import alluxio.job.plan.PlanConfig;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import java.util.Collection;
import java.util.Objects;

/**
 * Config for a plan that does nothing.
 */
public class CrashPlanConfig implements PlanConfig {

  public static final String NAME = "Crash";

  public String mPath;

  public CrashPlanConfig(@JsonProperty("path") String path) {
    mPath = path;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    return obj instanceof CrashPlanConfig;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPath);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).toString();
  }

  @Override
  public String getName() {
    return NAME;
  }

  public String getPath() {
    return mPath;
  }

  @Override
  public Collection<String> affectedPaths() {
    return ImmutableList.of(mPath);
  }
}
