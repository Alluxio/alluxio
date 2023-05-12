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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import alluxio.Constants;
import alluxio.clock.ManualClock;
import alluxio.time.Sleeper;
import alluxio.time.SteppingThreadSleeper;

import org.apache.logging.log4j.core.util.CronExpression;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * Unit tests for {@link SleepingTimer}.
 */
public final class SleepingTimerForCronExpressionIntervalSupplierTest {
  private static final String THREAD_NAME = "cron-test-thread-name";
  private static final long INTERVAL_MS = 10 * Constants.MINUTE_MS;
  private Logger mMockLogger;
  private ManualClock mFakeClock;
  private Sleeper mMockSleeper;
  private long mAllSleepTimeMs;

  @Before
  public void before() throws InterruptedException {
    mMockLogger = mock(Logger.class);
    mFakeClock = new ManualClock();
    mMockSleeper = mock(Sleeper.class);
    doAnswer((invocation) -> {
      Duration duration = invocation.getArgument(0);
      mFakeClock.addTime(duration);
      mAllSleepTimeMs += duration.toMillis();
      return null;
    }).when(mMockSleeper).sleep(any(Duration.class));
  }

  /**
   * Tests that the cron timer will attempt to run at the same interval, independently of how
   * long the execution between ticks takes. For example, if the interval is 100ms and execution
   * takes 80ms, the timer should sleep for only 20ms to maintain the regular interval of 100ms.
   */
  @Test
  public void maintainInterval() throws Exception {
    SleepingTimer timer =
        new SleepingTimer(THREAD_NAME, mMockLogger, mFakeClock,
            new SteppingThreadSleeper(mMockSleeper, mFakeClock),
            () -> {
              try {
                return new CronExpressionIntervalSupplier(
                    new CronExpression("* 30-59 0-1,4-9,13-23 * * ? *"), INTERVAL_MS);
              } catch (ParseException e) {
                throw new RuntimeException(e);
              }
            });
    DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date startDate = formatter.parse("2022-01-01 00:00:00");
    Assert.assertEquals(-1, timer.mPreviousTickedMs);
    mFakeClock.setTimeMs(startDate.getTime());
    long limitMs = timer.tick();
    long lastAllSleepTimeMs = mAllSleepTimeMs;
    Assert.assertEquals(30 * Constants.MINUTE_MS, mAllSleepTimeMs);
    Assert.assertEquals(30 * Constants.MINUTE_MS, limitMs);
    Assert.assertEquals(formatter.parse("2022-01-01 00:30:00"), new Date(timer.mPreviousTickedMs));
    Assert.assertEquals(formatter.parse("2022-01-01 00:30:00"), new Date(mFakeClock.millis()));
    // Mock heartbeat 1 minute
    mFakeClock.addTime(Duration.ofMinutes(1));

    limitMs = timer.tick();
    Assert.assertEquals(9 * Constants.MINUTE_MS, mAllSleepTimeMs - lastAllSleepTimeMs);
    lastAllSleepTimeMs = mAllSleepTimeMs;
    Assert.assertEquals(20 * Constants.MINUTE_MS, limitMs);
    Assert.assertEquals(formatter.parse("2022-01-01 00:40:00"), new Date(timer.mPreviousTickedMs));
    Assert.assertEquals(formatter.parse("2022-01-01 00:40:00"), new Date(mFakeClock.millis()));
    // Mock heartbeat 5 minute
    mFakeClock.addTime(Duration.ofMinutes(5));

    limitMs = timer.tick();
    Assert.assertEquals(5 * Constants.MINUTE_MS, mAllSleepTimeMs - lastAllSleepTimeMs);
    lastAllSleepTimeMs = mAllSleepTimeMs;
    Assert.assertEquals(10 * Constants.MINUTE_MS, limitMs);
    Assert.assertEquals(formatter.parse("2022-01-01 00:50:00"), new Date(timer.mPreviousTickedMs));
    Assert.assertEquals(formatter.parse("2022-01-01 00:50:00"), new Date(mFakeClock.millis()));
    // Mock heartbeat 5 minute
    mFakeClock.addTime(Duration.ofMinutes(5));

    limitMs = timer.tick();
    Assert.assertEquals(35 * Constants.MINUTE_MS, mAllSleepTimeMs - lastAllSleepTimeMs);
    lastAllSleepTimeMs = mAllSleepTimeMs;
    Assert.assertEquals(30 * Constants.MINUTE_MS, limitMs);
    Assert.assertEquals(formatter.parse("2022-01-01 01:30:00"), new Date(timer.mPreviousTickedMs));
    Assert.assertEquals(formatter.parse("2022-01-01 01:30:00"), new Date(mFakeClock.millis()));
    // Mock heartbeat 30 minute
    mFakeClock.addTime(Duration.ofMinutes(30));

    limitMs = timer.tick();
    Assert.assertEquals(150 * Constants.MINUTE_MS, mAllSleepTimeMs - lastAllSleepTimeMs);
    Assert.assertEquals(30 * Constants.MINUTE_MS, limitMs);
    Assert.assertEquals(formatter.parse("2022-01-01 04:30:00"), new Date(timer.mPreviousTickedMs));
    Assert.assertEquals(formatter.parse("2022-01-01 04:30:00"), new Date(mFakeClock.millis()));
  }
}
