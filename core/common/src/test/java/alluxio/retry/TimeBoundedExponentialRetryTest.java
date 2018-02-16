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

package alluxio.retry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import alluxio.Constants;
import alluxio.clock.ManualClock;
import alluxio.time.ManualSleeper;
import alluxio.time.TimeContext;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;

/**
 * Unit tests for {@link TimeBoundedExponentialRetry}.
 */
public final class TimeBoundedExponentialRetryTest {
  private ManualClock mClock;
  private ManualSleeper mSleeper;
  private Thread mTestThread;

  @Before
  public void before() {
    mClock = new ManualClock();
    mSleeper = new ManualSleeper();
  }

  @After
  public void after() throws InterruptedException {
    if (mTestThread != null) {
      mTestThread.interrupt();
      mTestThread.join(10 * Constants.SECOND_MS);
    }
  }

  @Test
  public void exponentialBackoff() throws InterruptedException {
    TimeBoundedExponentialRetry retry = TimeBoundedExponentialRetry.builder()
        .withTimeCtx(new TimeContext(mClock, mSleeper))
        .withMaxDuration(Duration.ofMillis(500))
        .withInitialSleep(Duration.ofMillis(10))
        .withMaxSleep(Duration.ofMillis(100))
        .build();

    mTestThread = new Thread(() -> {
      while (retry.attemptRetry()) {
        continue;
      }
    });
    mTestThread.setDaemon(true);
    mTestThread.start();

    for (int sleepTimeMs : Arrays.asList(10, 20, 40, 80, 100, 100, 100, 50)) {
      assertEquals(Duration.ofMillis(sleepTimeMs), mSleeper.waitForSleep());
      mClock.addTimeMs(sleepTimeMs);
      mSleeper.wakeUp();
    }
    mTestThread.join(10 * Constants.SECOND_MS);
    assertFalse(mTestThread.isAlive());
  }

  @Test
  public void taskTakesTime() throws InterruptedException {
    TimeBoundedExponentialRetry retry = TimeBoundedExponentialRetry.builder()
        .withTimeCtx(new TimeContext(mClock, mSleeper))
        .withMaxDuration(Duration.ofMillis(500))
        .withInitialSleep(Duration.ofMillis(10))
        .withMaxSleep(Duration.ofMillis(100))
        .build();

    // Now the task takes 20ms.
    mTestThread = new Thread(() -> {
      do {
        mClock.addTimeMs(20);
      } while (retry.attemptRetry());
    });
    mTestThread.setDaemon(true);
    mTestThread.start();

    // Clock times after each sleep: 50, 90, 150, 250, 370, 490, 520
    for (int sleepTimeMs : Arrays.asList(10, 20, 40, 80, 100, 100, 10)) {
      assertEquals(Duration.ofMillis(sleepTimeMs), mSleeper.waitForSleep());
      mClock.addTimeMs(sleepTimeMs);
      mSleeper.wakeUp();
    }
    mTestThread.join(10 * Constants.SECOND_MS);
    assertFalse(mTestThread.isAlive());
  }

  @Test
  public void border() throws InterruptedException {
    TimeBoundedExponentialRetry retry = TimeBoundedExponentialRetry.builder()
        .withTimeCtx(new TimeContext(mClock, mSleeper))
        .withMaxDuration(Duration.ofMillis(450)) // 10 + 20 + 40 + 80 + 100 + 100 + 100 = 450
        .withInitialSleep(Duration.ofMillis(10))
        .withMaxSleep(Duration.ofMillis(100))
        .build();

    mTestThread = new Thread(() -> {
      while (retry.attemptRetry()) {
        continue;
      }
    });
    mTestThread.setDaemon(true);
    mTestThread.start();

    for (int sleepTimeMs : Arrays.asList(10, 20, 40, 80, 100, 100, 100)) {
      assertEquals(Duration.ofMillis(sleepTimeMs), mSleeper.waitForSleep());
      mClock.addTimeMs(sleepTimeMs);
      mSleeper.wakeUp();
    }
    mTestThread.join(10 * Constants.SECOND_MS);
    assertFalse(mTestThread.isAlive());
  }
}
