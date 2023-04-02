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

package alluxio.heartbeat;

/**
 * Fixed interval supplier.
 */
public class FixedIntervalSupplier implements SleepIntervalSupplier {
  private final long mInterval;

  /**
   * Constructs a new {@link FixedIntervalSupplier}.
   *
   * @param fixedInterval the fixed interval
   */
  public FixedIntervalSupplier(long fixedInterval) {
    mInterval = fixedInterval;
  }

  @Override
  public long getNextInterval(long mPreviousTickedMs, long nowTimeStampMillis) {
    long executionTimeMs = nowTimeStampMillis - mPreviousTickedMs;
    if (executionTimeMs > mInterval) {
      return 0;
    }
    return mInterval - executionTimeMs;
  }

  @Override
  public long getRunLimit(long mPreviousTickedMs) {
    return mInterval;
  }
}
