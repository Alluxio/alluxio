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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Sleep utilities shared by all components in Alluxio.
 */
@ThreadSafe
public final class SleepUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SleepUtils.class);

  /**
   * Sleeps for the given number of milliseconds.
   *
   * @param timeMs sleep duration in milliseconds
   */
  public static void sleepMs(long timeMs) {
    sleepMs(null, timeMs);
  }

  /**
   * Sleeps for the given number of milliseconds, reporting interruptions using the given logger.
   *
   * Unlike Thread.sleep(), this method responds to interrupts by setting the thread interrupt
   * status. This means that callers must check the interrupt status if they need to handle
   * interrupts.
   *
   * @param logger logger for reporting interruptions; no reporting is done if the logger is null
   * @param timeMs sleep duration in milliseconds
   */
  public static void sleepMs(Logger logger, long timeMs) {
    try {
      Thread.sleep(timeMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      if (logger != null) {
        logger.warn(e.getMessage(), e);
      }
    }
  }

  private SleepUtils() {} // prevent instantiation
}
