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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import alluxio.TestLoggerRule;

import com.google.common.base.Stopwatch;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.WriterAppender;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.reflect.Whitebox;

import java.util.Enumeration;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class JvmPauseMonitorTest {

  @Rule
  public TestLoggerRule mLogRule = new TestLoggerRule();

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Before
  public void before() {
    Enumeration appenders = Logger.getLogger(JvmPauseMonitor.class).getAllAppenders();
    while (appenders.hasMoreElements()) {
      Object appender = appenders.nextElement();
      if (appender instanceof WriterAppender) {
        ((WriterAppender) appender).setImmediateFlush(true);
      }
    }
  }

  @Test
  public void pauseMonitorStartStopTest() {
    JvmPauseMonitor mon = new JvmPauseMonitor(100, 1000, 500);
    Supplier<Thread> getThread = () -> Whitebox.getInternalState(mon, "mJvmMonitorThread");

    mon.start();
    assertTrue(mon.isStarted());
    assertNotNull(getThread.get());

    mon.stop();
    assertFalse(mon.isStarted());
    assertNull(getThread.get());
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
    JvmPauseMonitor mon = Mockito.spy(new JvmPauseMonitor(100, Long.MAX_VALUE, 250));
    CyclicBarrier before = new CyclicBarrier(2);
    doAnswer((Answer<Void>) invocation -> {
      Stopwatch sw = Stopwatch.createUnstarted();
      sw.start();
      while (sw.elapsed(TimeUnit.MILLISECONDS) < 251) {
        Thread.sleep(Math.abs(251 - sw.elapsed(TimeUnit.MILLISECONDS)));
      }
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
    assertTrue(mon.getInfoTimeExceeded() >= 1);
    assertEquals(0, mon.getWarnTimeExceeded());
    assertThat(mon.getTotalExtraTime(), greaterThanOrEqualTo(250L));
    assertTrue(mLogRule.wasLoggedWithLevel("JVM paused.*\n.*\n", Level.INFO));
    assertTrue(mLogRule.logCount("JVM paused.*\n.*\n.*") >= 1);
    before.await(); // runs the monitor once
    // wait until it reaches the barrier again
    while (before.getNumberWaiting() < 1) {
      Thread.sleep(20);
    }
    assertTrue(mon.getInfoTimeExceeded() >= 2);
    assertEquals(0, mon.getWarnTimeExceeded());
    assertThat(mon.getTotalExtraTime(), greaterThanOrEqualTo(500L));
    assertTrue(mLogRule.logCount("JVM paused.*\n.*\n.*") >= 2);
    System.gc();
    before.await(); // runs the monitor once
    // wait until it reaches the barrier again
    while (before.getNumberWaiting() < 1) {
      Thread.sleep(20);
    }
    assertTrue(mon.getInfoTimeExceeded() >= 3);
    assertEquals(0, mon.getWarnTimeExceeded());
    assertThat(mon.getTotalExtraTime(), greaterThanOrEqualTo(750L));
    assertTrue(mLogRule.logCount("JVM paused.*\n.*\n.*") >= 3);
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
    assertTrue(mon.getInfoTimeExceeded() >= 1);
    assertTrue(mon.getWarnTimeExceeded() >= 1);
    assertThat(mon.getTotalExtraTime(), greaterThanOrEqualTo(250L));
    assertTrue(mLogRule.wasLoggedWithLevel("JVM paused.*\n.*\n.*", Level.WARN));
    assertTrue(mLogRule.logCount("JVM paused.*\n.*\n.*") >= 1);
    before.await(); // runs the monitor once
    // wait until it reaches the barrier again
    while (before.getNumberWaiting() < 1) {
      Thread.sleep(20);
    }
    assertTrue(mon.getInfoTimeExceeded() >= 2);
    assertTrue(mon.getWarnTimeExceeded() >= 2);
    assertThat(mon.getTotalExtraTime(), greaterThanOrEqualTo(500L));
    assertTrue(mLogRule.logCount("JVM paused.*\n.*\n.*") >= 2);
    before.await(); // runs the monitor once
    System.gc();
    // wait until it reaches the barrier again
    while (before.getNumberWaiting() < 1) {
      Thread.sleep(20);
    }
    assertTrue(mon.getInfoTimeExceeded() >= 3);
    assertTrue(mon.getWarnTimeExceeded() >= 3);
    assertThat(mon.getTotalExtraTime(), greaterThanOrEqualTo(750L));
    assertTrue(mLogRule.logCount("JVM paused.*\n.*\n.*") >= 3);
  }
}
