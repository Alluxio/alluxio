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

import java.time.Duration;
import java.util.function.Function;

/**
 * A light sleeping utility which delegates to Thread.sleep().
 */
public final class LightThreadSleeper implements Sleeper {
  public static final LightThreadSleeper INSTANCE = new LightThreadSleeper();

  private LightThreadSleeper() {} // Use ThreadSleeper.INSTANCE instead.

  @Override
  public void sleep(Duration duration) throws InterruptedException {
    Thread.sleep(duration.toMillis());
  }

  public void sleep(Duration duration, Function<Long, Long> function) throws InterruptedException {
    long startSleepMs = System.currentTimeMillis();
    long sleepTo = startSleepMs + duration.toMillis();
    while (System.currentTimeMillis() < sleepTo) {
      long newInterval = function.apply(duration.toMillis());
      if (newInterval >= 0) {
        sleepTo = startSleepMs + newInterval;
      }
      // TODO(baoloongmao): Make sure we need to config it.
      Thread.sleep(60000);
    }
  }
}
