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

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import alluxio.Constants;
import alluxio.clock.ManualClock;
import alluxio.time.ManualSleeper;
import alluxio.time.TimeContext;

import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Unit tests for {@link ExponentialTimeBoundedRetry}.
 */
public final class ExponentialTimeBoundedRetryTest {
  @Test
  public void exponentialBackoff() throws InterruptedException {
    // Run the test multiple times to cover more cases due to randomness of jitter.
    for (int i = 0; i < 100; i++) {
      ManualClock clock = new ManualClock();
      ManualSleeper sleeper = new ManualSleeper();
      long maxDurationMs = 500;
      long taskTimeMs = 20;
      ExponentialTimeBoundedRetry retry = ExponentialTimeBoundedRetry.builder()
          .withTimeCtx(new TimeContext(clock, sleeper))
          .withMaxDuration(Duration.ofMillis(maxDurationMs))
          .withInitialSleep(Duration.ofMillis(10))
          .withMaxSleep(Duration.ofMillis(100))
          .build();

      Thread thread = new Thread(() -> {
        while (retry.attempt()) {
          clock.addTimeMs(taskTimeMs);
        }
      });
      thread.setDaemon(true);
      thread.setName("time-bounded-exponential-backoff-test");
      thread.start();

      long timeRemainingMs = maxDurationMs - taskTimeMs;
      Iterator<Long> expectedBaseTimes = Arrays.asList(10L, 20L, 40L, 80L, 100L).iterator();
      long expectedTime = expectedBaseTimes.next();
      while (timeRemainingMs > 0) {
        Duration actualSleep = sleeper.waitForSleep();
        // Account for 10% added jitter.
        checkBetween(expectedTime, expectedTime * 1.1, actualSleep);
        timeRemainingMs -= actualSleep.toMillis() + taskTimeMs;
        clock.addTime(actualSleep);
        sleeper.wakeUp();
        if (expectedBaseTimes.hasNext()) {
          expectedTime = expectedBaseTimes.next();
        }
        if (timeRemainingMs < 100) {
          expectedTime = timeRemainingMs;
        }
      }
      thread.interrupt();
      thread.join(10 * Constants.SECOND_MS);
      assertFalse(retry.attempt());
    }
  }

  private void checkBetween(double start, double end, Duration time) {
    assertThat((double) time.toMillis(), greaterThanOrEqualTo(start));
    assertThat((double) time.toMillis(), lessThanOrEqualTo(end));
  }
}
