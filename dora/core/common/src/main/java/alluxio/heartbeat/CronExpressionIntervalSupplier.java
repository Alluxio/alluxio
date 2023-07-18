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

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.core.util.CronExpression;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Objects;

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
    Preconditions.checkNotNull(cronExpression, "CronExpression is null");
    mInterval = fixedInterval;
    mCron = cronExpression;
  }

  @Override
  public long getNextInterval(long previousTickedMs, long nowTimeStampMillis) {
    long nextInterval = 0;
    long executionTimeMs = nowTimeStampMillis - previousTickedMs;
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
  public long getRunLimit(long previousTickedMs) {
    Date now = Date.from(Instant.ofEpochMilli(previousTickedMs));
    return Duration.between(now.toInstant(),
        mCron.getNextInvalidTimeAfter(now).toInstant()).toMillis();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CronExpressionIntervalSupplier that = (CronExpressionIntervalSupplier) o;
    return mInterval == that.mInterval
        && Objects.equals(mCron.getCronExpression(), that.mCron.getCronExpression());
  }

  @Override
  public int hashCode() {
    return Objects.hash(mInterval, mCron.getCronExpression());
  }
}
