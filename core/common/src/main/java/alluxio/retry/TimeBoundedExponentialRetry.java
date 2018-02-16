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

package alluxio.retry;

import alluxio.time.TimeContext;

import java.time.Duration;

/**
 * A retry policy which uses exponential backoff and a maximum duration time bound.
 *
 * A final retry will be performed at the time bound before giving up.
 *
 * For example, with initial sleep 10ms, maximum sleep 100ms, and maximum duration 500ms, the sleep
 * timings would be [10, 20, 40, 80, 100, 100, 100, 50], assuming the operation being retries takes
 * no time. The 50 at the end is because the previous times add up to 450, so the mechanism sleeps
 * for only 50ms before the final attempt.
 */
public final class TimeBoundedExponentialRetry extends TimeBoundedRetry {
  private final Duration mMaxSleep;
  private Duration mNextSleep;

  /**
   * See {@link Builder}.
   */
  private TimeBoundedExponentialRetry(TimeContext timeCtx, Duration maxDuration,
      Duration initialSleep, Duration maxSleep) {
    super(timeCtx, maxDuration);
    mMaxSleep = maxSleep;
    mNextSleep = initialSleep;
  }

  @Override
  protected Duration nextWaitTime() {
    Duration next = mNextSleep;
    mNextSleep = mNextSleep.multipliedBy(2);
    if (mNextSleep.compareTo(mMaxSleep) > 0) {
      mNextSleep = mMaxSleep;
    }
    return next;
  }

  /**
   * @return a builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for time bounded exponential retry mechanisms.
   */
  public static class Builder {
    private TimeContext mTimeCtx = TimeContext.SYSTEM;
    private Duration mMaxDuration;
    private Duration mInitialSleep;
    private Duration mMaxSleep;

    /**
     * @param timeCtx time context
     * @return the builder
     */
    public Builder withTimeCtx(TimeContext timeCtx) {
      mTimeCtx = timeCtx;
      return this;
    }

    /**
     * @param maxDuration max total duration to retry for
     * @return the builder
     */
    public Builder withMaxDuration(Duration maxDuration) {
      mMaxDuration = maxDuration;
      return this;
    }

    /**
     * @param initialSleep initial sleep interval between retries
     * @return the builder
     */
    public Builder withInitialSleep(Duration initialSleep) {
      mInitialSleep = initialSleep;
      return this;
    }

    /**
     * @param maxSleep maximum sleep interval between retries
     * @return the builder
     */
    public Builder withMaxSleep(Duration maxSleep) {
      mMaxSleep = maxSleep;
      return this;
    }

    /**
     * @return the built retry mechanism
     */
    public TimeBoundedExponentialRetry build() {
      return new TimeBoundedExponentialRetry(mTimeCtx, mMaxDuration, mInitialSleep, mMaxSleep);
    }
  }
}
