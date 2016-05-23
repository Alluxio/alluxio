/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.heartbeat;

import alluxio.util.CommonUtils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;

/**
 * Unit tests for {@link SleepingTimer}. It tests three scenarios listed below:
 * 1. Sleep more than the interval of SleepingTimer and see if the SleepingTimer warns correctly;
 * 2. Tick continuously for several times and see if the time interval is correct;
 * 3. Sleep less than the interval of SleepingTimer and see if the time interval is correct.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({SleepingTimer.class})
public final class SleepingTimerTest {
  private static final String THREAD_NAME = "sleepingtimer-test-thread-name";
  private static final long INTERVAL_MS = 500;

  /**
   *  This is a test to make sure that SleepingTimer should warn when execution time
   *  is longer than interval.
   */
  @Test
  public void executeLongerThanIntervalTest() throws Exception {
    SleepingTimer stimer = new SleepingTimer(THREAD_NAME, INTERVAL_MS);

    Logger logger = Mockito.mock(Logger.class);
    Whitebox.setInternalState(SleepingTimer.class, "LOG", logger);

    stimer.tick();
    CommonUtils.sleepMs(5 * INTERVAL_MS);
    stimer.tick();

    Mockito.verify(logger)
        .warn(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());
  }

  /**
   *  This test ticks three times continuously and checks the correctness of the interval.
   */
  @Test
  public void continuousTickTest() throws Exception {
    SleepingTimer stimer = new SleepingTimer(THREAD_NAME, INTERVAL_MS);
    stimer.tick();
    long timeBeforeMs = Whitebox.getInternalState(stimer, "mPreviousTickMs");
    stimer.tick();
    stimer.tick();
    stimer.tick();
    long timeIntervalMs = System.currentTimeMillis() - timeBeforeMs;
    Assert.assertTrue(timeIntervalMs >= 3 * INTERVAL_MS);
  }

  /**
   *  This test sleeps for a time period shorter than the interval of SleepingTimer, and checks
   *  whether the SleepingTimer works correctly after that.
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
