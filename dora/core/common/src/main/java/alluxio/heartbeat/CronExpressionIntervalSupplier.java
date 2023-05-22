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

import org.apache.logging.log4j.core.util.CronExpression;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;

/**
* Calculate the next interval by given cron expression.
*/
public class CronExpressionIntervalSupplier implements SleepIntervalSupplier {
  private final long mInterval;
  private final CronExpression mCron;

  /**
   * Constructs a new {@link CronExpressionIntervalSupplier}.
   *
   * @param cronExpression the cron expression
   * @param fixedInterval the fixed interval
   */
  public CronExpressionIntervalSupplier(CronExpression cronExpression, long fixedInterval) {
    mInterval = fixedInterval;
    mCron = cronExpression;
  }

  @Override
  public long getNextInterval(long mPreviousTickedMs, long nowTimeStampMillis) {
    long nextInterval = 0;
    long executionTimeMs = nowTimeStampMillis - mPreviousTickedMs;
    if (executionTimeMs < mInterval) {
      nextInterval = mInterval - executionTimeMs;
    }
    Date now = Date.from(Instant.ofEpochMilli(nowTimeStampMillis + nextInterval));
    if (mCron.isSatisfiedBy(now)) {
      return nextInterval;
    }
    return nextInterval + Duration.between(
        now.toInstant(), mCron.getNextValidTimeAfter(now).toInstant()).toMillis();
  }

  @Override
  public long getRunLimit(long mPreviousTickedMs) {
    Date now = Date.from(Instant.ofEpochMilli(mPreviousTickedMs));
    return Duration.between(now.toInstant(),
        mCron.getNextInvalidTimeAfter(now).toInstant()).toMillis();
  }
}
