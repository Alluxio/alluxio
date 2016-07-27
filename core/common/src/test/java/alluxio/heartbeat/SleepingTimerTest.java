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

import alluxio.Constants;
import alluxio.time.ScheduledSleeper;
import alluxio.time.ThreadSleeper;
import alluxio.util.CommonUtils;

import com.google.common.base.Throwables;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link SleepingTimer}. It tests three scenarios listed below:
 * 1. Sleep more than the interval of SleepingTimer and see if the SleepingTimer warns correctly;
 * 2. Tick continuously for several times and see if the time interval is correct;
 * 3. Sleep less than the interval of SleepingTimer and see if the time interval is correct.
 */

public final class SleepingTimerTest {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final String THREAD_NAME = "sleepingtimer-test-thread-name";
  private static final long INTERVAL_MS = 10;

  /**
   * This is a test to make sure that SleepingTimer should warn when execution time is longer than
   * interval.
   */
  @Test
  public void executeLongerThanIntervalTest() throws Exception {
    Logger logger = Mockito.mock(Logger.class);
    SleepingTimer stimer = new SleepingTimer(THREAD_NAME, INTERVAL_MS, logger, new ThreadSleeper());

    stimer.tick();
    CommonUtils.sleepMs(5 * INTERVAL_MS);
    stimer.tick();

    Mockito.verify(logger)
        .warn(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());
  }

  /**
   * This test checks the correctness of the sleeping interval.
   */
  @Test
  public void tickIntervalTest() throws Exception {
    ScheduledSleeper sleeper = new ScheduledSleeper();
    final SleepingTimer stimer = new SleepingTimer(THREAD_NAME, INTERVAL_MS, LOG, sleeper);
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          stimer.tick();
        } catch (InterruptedException e) {
          Throwables.propagate(e);
        }
      }
    }).start();
    // Wait for tick to run in the other thread.
    while (sleeper.getSleepPeriod() < 0) {
      CommonUtils.sleepMs(10);
    }
    Assert.assertEquals(INTERVAL_MS, sleeper.getSleepPeriod());
    sleeper.wakeUp();
  }

  /**
   * This test sleeps for a time period shorter than the interval of SleepingTimer, and checks
   * whether the SleepingTimer works correctly after that.
   */
  @Test
  public void executeShorterThanIntervalTest() throws Exception {
    SleepingTimer stimer = new SleepingTimer(THREAD_NAME, INTERVAL_MS);

    stimer.tick();
    long timeBeforeMs = Whitebox.getInternalState(stimer, "mPreviousTickMs");

    CommonUtils.sleepMs(INTERVAL_MS / 2);
    stimer.tick();
    long timeIntervalMs = System.currentTimeMillis() - timeBeforeMs;
    Assert.assertTrue(timeIntervalMs >= INTERVAL_MS);
  }
}
