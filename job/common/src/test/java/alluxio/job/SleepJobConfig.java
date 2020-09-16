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
import com.google.common.base.Objects;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.Collections;

/**
 * Job configuration for the sleep job.
 */
@ThreadSafe
public class SleepJobConfig implements PlanConfig {
  private static final long serialVersionUID = 43139051130518451L;

  public static final String NAME = "Sleep";

  private final long mTimeMs;
  private final int mTasksPerWorker;

  /**
   * @param timeMs the time to sleep for in milliseconds
   */
  public SleepJobConfig(long timeMs) {
    this(timeMs, 1);
  }

  public SleepJobConfig(@JsonProperty("timeMs") long timeMs,
                        @JsonProperty("tasksPerWorker") int tasksPerWorker) {
    mTimeMs = timeMs;
    mTasksPerWorker = tasksPerWorker;
  }

  /**
   * @return the time to sleep for in milliseconds
   */
  public long getTimeMs() {
    return mTimeMs;
  }

  /**
   * @return
   */
  public int getTasksPerWorker() {
    return mTasksPerWorker;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof SleepJobConfig)) {
      return false;
    }
    SleepJobConfig that = (SleepJobConfig) obj;
    return mTimeMs == that.mTimeMs;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mTimeMs);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("timeMs", mTimeMs).toString();
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Collection<String> affectedPaths() {
    return Collections.EMPTY_LIST;
  }
}
