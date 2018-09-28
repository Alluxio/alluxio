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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Job configuration for the sleep job.
 */
@ThreadSafe
public class SleepJobConfig implements JobConfig {
  private static final long serialVersionUID = 43139051130518451L;

  public static final String NAME = "Sleep";

  private final long mTimeMs;

  /**
   * @param timeMs the time to sleep for in milliseconds
   */
  public SleepJobConfig(@JsonProperty("timeMs") long timeMs) {
    mTimeMs = timeMs;
  }

  /**
   * @return the time to sleep for in milliseconds
   */
  public long getTimeMs() {
    return mTimeMs;
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
    return Objects.toStringHelper(this).add("timeMs", mTimeMs).toString();
  }

  @Override
  public String getName() {
    return NAME;
  }
}
