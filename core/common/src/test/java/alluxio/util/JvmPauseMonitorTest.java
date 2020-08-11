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

package alluxio.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;

import alluxio.TestLoggerRule;

import org.apache.log4j.Level;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.reflect.Whitebox;

import java.util.concurrent.CyclicBarrier;

public final class JvmPauseMonitorTest {

  @Rule
  public TestLoggerRule mLogRule = new TestLoggerRule();

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Test
  public void pauseMonitorStartStopTest() {
    JvmPauseMonitor mon = new JvmPauseMonitor(100, 1000, 500);
    mon.start();
    assertTrue(mon.isStarted());
    mon.stop();
    assertFalse(mon.isStarted());
  }

  @Test
  public void interruptOnStop() throws Exception {
    JvmPauseMonitor mon = Mockito.spy(new JvmPauseMonitor(1000, 900000, 90000));
    mon.start();
    Thread pmThread = Whitebox.getInternalState(mon, "mJvmMonitorThread");
    Thread spied = Mockito.spy(pmThread);
    doThrow(InterruptedException.class).when(spied).join();
    Whitebox.setInternalState(mon, "mJvmMonitorThread", spied);
    mon.stop();
    assertTrue(Thread.currentThread().isInterrupted());
    try {
      pmThread.interrupt();
      pmThread.join();
    } catch (InterruptedException e) {
      // ok, join was a cleanup operation
    }
  }

  @Test
  public void testNegativeGcSleep() {
    mException.expect(IllegalArgumentException.class);
    new JvmPauseMonitor(-1, 100, 500);
  }

  @Test
  public void testNegativeInfoThreshold() {
    mException.expect(IllegalArgumentException.class);
    new JvmPauseMonitor(100, 500, -1);
  }

  @Test
  public void testNegativeWarnThreshold() {
    mException.expect(IllegalArgumentException.class);
    new JvmPauseMonitor(100, -1, 500);
  }

  @Test
  public void testTinyWarnThreshold() {
    mException.expect(IllegalArgumentException.class);
    new JvmPauseMonitor(100, 50, 5000);
  }

  /**
   * This test mocks the {@link JvmPauseMonitor#sleepMillis(long)} (long)} method to simulate a
   * pause which logs at the INFO level.
   */
  @Test
  public void testMockedInfoPause() throws Exception {
    JvmPauseMonitor mon = Mockito.spy(new JvmPauseMonitor(100, 500, 250));
    CyclicBarrier before = new CyclicBarrier(2);
    doAnswer((Answer<Void>) invocation -> {
      Thread.sleep(250);
      invocation.callRealMethod();
      before.await();
      return null;
    }).when(mon).sleepMillis(any(Long.class));
    mon.start();
    before.await(); // runs the monitor once
    // wait until it reaches the barrier again
    while (before.getNumberWaiting() < 1) {
      Thread.sleep(20);
    }
    assertEquals(1, mon.getInfoTimeExceeded());
    assertEquals(0, mon.getWarnTimeExceeded());
    assertEquals(250, mon.getTotalExtraTime(), 25);
    assertTrue(mLogRule.wasLoggedWithLevel("JVM paused.*\n.*\n.*", Level.INFO));
    assertEquals(1, mLogRule.logCount("JVM paused.*\n.*\n.*"));
    before.await(); // runs the monitor once
    // wait until it reaches the barrier again
    while (before.getNumberWaiting() < 1) {
      Thread.sleep(20);
    }
    assertEquals(2, mon.getInfoTimeExceeded());
    assertEquals(0, mon.getWarnTimeExceeded());
    assertEquals(500, mon.getTotalExtraTime(), 50);
    assertEquals(2, mLogRule.logCount("JVM paused.*\n.*\n.*"));
    before.await(); // runs the monitor once
    // wait until it reaches the barrier again
    while (before.getNumberWaiting() < 1) {
      Thread.sleep(20);
    }
    assertEquals(3, mon.getInfoTimeExceeded());
    assertEquals(0, mon.getWarnTimeExceeded());
    assertEquals(750, mon.getTotalExtraTime(), 75);
    assertEquals(3, mLogRule.logCount("JVM paused.*\n.*\n.*"));
  }

  /**
   * This test mocks the {@link JvmPauseMonitor#sleepMillis(long)} (long)} method to simulate a
   * pause which logs at the WARN level.
   */
  @Test
  public void testMockedWarnPause() throws Exception {
    JvmPauseMonitor mon = Mockito.spy(new JvmPauseMonitor(100, 200, 150));
    CyclicBarrier before = new CyclicBarrier(2);
    doAnswer((Answer<Void>) invocation -> {
      Thread.sleep(250);
      invocation.callRealMethod();
      before.await();
      return null;
    }).when(mon).sleepMillis(any(Long.class));
    mon.start();
    before.await(); // runs the monitor once
    // wait until it reaches the barrier again
    while (before.getNumberWaiting() < 1) {
      Thread.sleep(20);
    }
    assertEquals(1, mon.getInfoTimeExceeded());
    assertEquals(1, mon.getWarnTimeExceeded());
    assertEquals(250, mon.getTotalExtraTime(), 25);
    assertTrue(mLogRule.wasLoggedWithLevel("JVM paused.*\n.*\n.*", Level.WARN));
    assertEquals(1, mLogRule.logCount("JVM paused.*\n.*\n.*"));
    before.await(); // runs the monitor once
    // wait until it reaches the barrier again
    while (before.getNumberWaiting() < 1) {
      Thread.sleep(20);
    }
    assertEquals(2, mon.getInfoTimeExceeded());
    assertEquals(2, mon.getWarnTimeExceeded());
    assertEquals(500, mon.getTotalExtraTime(), 50);
    assertEquals(2, mLogRule.logCount("JVM paused.*\n.*\n.*"));
    before.await(); // runs the monitor once
    // wait until it reaches the barrier again
    while (before.getNumberWaiting() < 1) {
      Thread.sleep(20);
    }
    assertEquals(3, mon.getInfoTimeExceeded());
    assertEquals(3, mon.getWarnTimeExceeded());
    assertEquals(750, mon.getTotalExtraTime(), 75);
    assertEquals(3, mLogRule.logCount("JVM paused.*\n.*\n.*"));
  }
}
