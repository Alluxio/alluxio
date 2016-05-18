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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;

import alluxio.util.CommonUtils;

/**
 * Unit tests for {@link SleepingTimer}. It tests three scenarios listed below:
 * 1. Sleep more than the interval of SleepingTimer and see if the SleepingTimer warns correctly
 * 2. Tick continuously for several time and see if the time interval is correct
 * 3. Sleep less than the interval of SleepingTimer and see if the time interval is correct
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest({SleepingTimer.class})
public class SleepingTimerTest {
  private static final String THREAD_NAME = "sleepingtimer-test-thread-name";
  private static final long INTERVAL_MS = 500;

  @Test
  public void sleepingTimerTest() throws Exception {
    SleepingTimer stimer = new SleepingTimer(THREAD_NAME, INTERVAL_MS);

    Logger logger = Mockito.mock(Logger.class);
    long PreTickMs = 0;

    Whitebox.setInternalState(SleepingTimer.class, "LOG", logger);
    Whitebox.setInternalState(stimer, "mPreviousTickMs", PreTickMs);

    //first case, SleepingTimer should warn when excution time is longer than interval
    stimer.tick();
    CommonUtils.sleepMs(5 * INTERVAL_MS);
    stimer.tick();

    Mockito.verify(logger).warn(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(),
        Mockito.anyLong());

    //second case, tick three times and check the interval correctness
    long timeBeforeMs = PreTickMs;
    stimer.tick();
    stimer.tick();
    stimer.tick();
    long timeIntervalMs = System.currentTimeMillis() - timeBeforeMs;
    Assert.assertTrue(timeIntervalMs >= 3 * INTERVAL_MS);

    //third case, sleep less than interval and check theinterval correctness
    timeBeforeMs = PreTickMs;
    CommonUtils.sleepMs(INTERVAL_MS / 2);
    stimer.tick();
    timeIntervalMs = System.currentTimeMillis() - timeBeforeMs;
    Assert.assertTrue(timeIntervalMs >= INTERVAL_MS);
  }
}
