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

import alluxio.time.Sleeper;
import alluxio.time.ThreadSleeper;

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import org.apache.logging.log4j.core.util.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class can be used for executing heartbeats follow cron expression.
 */
@NotThreadSafe
public final class CronTimer extends SleepingTimer {
  private final Supplier<String> mPeriodCronExpressionSupplier;
  private volatile CronExpression mPeriodCrontabExpression;

  /**
   * Creates a new instance of {@link CronTimer}.
   *
   * @param threadName the thread name
   * @param clock for telling the current time
   * @param intervalSupplier Sleep time between different heartbeat supplier
   * @param periodCronExpressionSupplier the period cron expression
   */
  public CronTimer(String threadName, Clock clock, Supplier<Long> intervalSupplier,
      Supplier<String> periodCronExpressionSupplier) {
    this(threadName, LoggerFactory.getLogger(CronTimer.class),
        clock, ThreadSleeper.INSTANCE, intervalSupplier, periodCronExpressionSupplier);
  }

  /**
   * Creates a new instance of {@link CronTimer}.
   *
   * @param threadName the thread name
   * @param logger the logger to log to
   * @param clock for telling the current time
   * @param sleeper the utility to use for sleeping
   * @param intervalSupplier Sleep time between different heartbeat supplier
   * @param periodCronExpressionSupplier the period cron expression
   */
  public CronTimer(String threadName, Logger logger, Clock clock,
      Sleeper sleeper, Supplier<Long> intervalSupplier,
      Supplier<String> periodCronExpressionSupplier) {
    super(threadName, logger, clock, sleeper, intervalSupplier, periodCronExpressionSupplier);
    mPeriodCronExpressionSupplier = periodCronExpressionSupplier;
    tryUpdatePeriodCrontabExpression();
  }

  /**
   * Enforces the thread waits for the given interval between consecutive ticks.
   *
   * @throws InterruptedException if the thread is interrupted while waiting
   */
  @Override
  public long tick() throws InterruptedException {
    long runLimit = super.tick();
    if (mPeriodCrontabExpression != null) {
      Date date = new Date(mClock.millis());
      if (!mPeriodCrontabExpression.isSatisfiedBy(date)) {
        mSleeper.sleep(Duration.ofMillis(
            mPeriodCrontabExpression.getNextValidTimeAfter(date).getTime() - date.getTime()));
      }
      mPreviousTickMs = mClock.millis();
      date = new Date(mPreviousTickMs);
      return mPeriodCrontabExpression.getNextInvalidTimeAfter(date).getTime() - date.getTime();
    }
    return runLimit;
  }

  @Override
  public void update() {
    super.update();
    tryUpdatePeriodCrontabExpression();
  }

  private void tryUpdatePeriodCrontabExpression() {
    if (mPeriodCronExpressionSupplier != null) {
      String cronExpression = mPeriodCronExpressionSupplier.get();
      try {
        if (Strings.isNullOrEmpty(cronExpression)) {
          mPeriodCrontabExpression = null;
        } else {
          mPeriodCrontabExpression = new CronExpression(cronExpression);
        }
      } catch (ParseException e) {
        throw new RuntimeException(
            "The configured period crontab expression :" + cronExpression + " is not valid", e);
      }
    }
  }
}
