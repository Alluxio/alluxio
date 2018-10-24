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

package alluxio.time;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import alluxio.Constants;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for {@link ManualSleeper}.
 */
public final class ManualSleeperTest {
  private AtomicInteger mSleepTime;
  private ManualSleeper mSleeper;
  private Thread mTestThread;

  @Before
  public void before() {
    mSleepTime = new AtomicInteger(0);
    mSleeper = new ManualSleeper();
    mTestThread = new Thread(() -> {
      while (true) {
        try {
          mSleeper.sleep(Duration.ofMillis(mSleepTime.incrementAndGet()));
        } catch (InterruptedException e) {
          return;
        }
      }
    });
    mTestThread.setDaemon(true);
    mTestThread.start();
  }

  @After
  public void after() throws InterruptedException {
    mTestThread.interrupt();
    mTestThread.join(Constants.SECOND_MS);
  }

  @Test
  public void checkSleepTime() throws InterruptedException {
    for (int i = 1; i < 100; i++) {
      assertEquals(i, mSleeper.waitForSleep().toMillis());
      mSleeper.wakeUp();
    }
  }

  @Test
  public void propagateInterrupt() throws InterruptedException {
    mTestThread.interrupt();
    mTestThread.join(Constants.SECOND_MS);
    assertFalse(mTestThread.isAlive());
  }
}
