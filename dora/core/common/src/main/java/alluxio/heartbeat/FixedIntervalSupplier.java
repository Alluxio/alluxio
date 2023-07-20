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

import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

import java.util.Objects;

/**
 * Fixed interval supplier.
 */
public class FixedIntervalSupplier implements SleepIntervalSupplier {

  private final long mInterval;
  protected final Logger mLogger;

  /**
   * Constructs a new {@link FixedIntervalSupplier}.
   *
   * @param fixedInterval the fixed interval
   * @param logger the logger
   */
  public FixedIntervalSupplier(long fixedInterval, Logger logger) {
    mInterval = fixedInterval;
    mLogger = logger;
  }

  /**
   * Constructs a new {@link FixedIntervalSupplier}.
   *
   * @param fixedInterval the fixed interval
   */
  public FixedIntervalSupplier(long fixedInterval) {
    this(fixedInterval, NOPLogger.NOP_LOGGER);
  }

  @Override
  public long getNextInterval(long previousTickedMs, long nowTimeStampMillis) {
    if (previousTickedMs == -1) {
      return -1;
    }
    long executionTimeMs = nowTimeStampMillis - previousTickedMs;
    if (executionTimeMs > mInterval) {
      mLogger.warn("{} last execution took {} ms. Longer than the interval {}",
          Thread.currentThread().getName(), executionTimeMs, mInterval);
      return 0;
    }
    return mInterval - executionTimeMs;
  }

  @Override
  public long getRunLimit(long previousTickedMs) {
    return mInterval;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FixedIntervalSupplier that = (FixedIntervalSupplier) o;
    return mInterval == that.mInterval;
  }

  @Override
  public int hashCode() {
    return Objects.hash(mInterval);
  }
}
