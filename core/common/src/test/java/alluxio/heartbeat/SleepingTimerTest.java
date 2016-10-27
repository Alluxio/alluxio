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

import alluxio.clock.ManualClock;
import alluxio.time.Sleeper;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

/**
 * Unit tests for {@link SleepingTimer}.
 */
public final class SleepingTimerTest {
  private static final String THREAD_NAME = "sleepingtimer-test-thread-name";
  private static final long INTERVAL_MS = 500;
  private Logger mMockLogger;
  private ManualClock mFakeClock;
  private Sleeper mMockSleeper;

  @Before
  public void before() {
    mMockLogger = Mockito.mock(Logger.class);
    mFakeClock = new ManualClock();
    mMockSleeper = Mockito.mock(Sleeper.class);
  }

  @Test
  public void warnWhenExecutionTakesLongerThanInterval() throws Exception {
    SleepingTimer timer =
        new SleepingTimer(THREAD_NAME, INTERVAL_MS, mMockLogger, mFakeClock, mMockSleeper);

    timer.tick();
    mFakeClock.addTimeMs(5 * INTERVAL_MS);
    timer.tick();

    Mockito.verify(mMockLogger).warn(Mockito.anyString(), Mockito.anyString(), Mockito.anyLong(),
        Mockito.anyLong());
  }

  @Test
  public void sleepForSpecifiedInterval() throws Exception {
    final SleepingTimer timer =
        new SleepingTimer(THREAD_NAME, INTERVAL_MS, mMockLogger, mFakeClock, mMockSleeper);
    timer.tick(); // first tick won't sleep
    Mockito.verify(mMockSleeper, Mockito.times(0)).sleep(Mockito.anyLong());
    timer.tick();
    Mockito.verify(mMockSleeper).sleep(INTERVAL_MS);
  }

  /**
   * Tests that the sleeping timer will attempt to run at the same interval, independently of how
   * long the execution between ticks takes. For example, if the interval is 100ms and execution
   * takes 80ms, the timer should sleep for only 20ms to maintain the regular interval of 100ms.
   */
  @Test
  public void maintainInterval() throws Exception {
    SleepingTimer stimer =
        new SleepingTimer(THREAD_NAME, INTERVAL_MS, mMockLogger, mFakeClock, mMockSleeper);

    stimer.tick();
    mFakeClock.addTimeMs(INTERVAL_MS / 3);
    stimer.tick();
    Mockito.verify(mMockSleeper).sleep(INTERVAL_MS - (INTERVAL_MS / 3));
  }
}
