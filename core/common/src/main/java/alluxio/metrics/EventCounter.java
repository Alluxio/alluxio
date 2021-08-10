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

package alluxio.metrics;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

/**
 * A log4J Appender that simply counts logging events in four levels:
 * FATAL, ERROR WARN and INFO. The class name is used in log4j.properties
 */
public class EventCounter extends AppenderSkeleton {
  private static final int FATAL = 0;
  private static final int ERROR = 1;
  private static final int WARN = 2;
  private static final int INFO = 3;
  private static EventCounter.EventCounts sCount = new EventCounter.EventCounts();

  /**
   * The constructor of EventCounter.
   */
  public EventCounter() {
  }

  /**
   * Gets the number of fatal log.
   *
   * @return the number of fatal log
   */
  public static long getFatal() {
    return sCount.get(FATAL);
  }

  /**
   * Gets the number of error log.
   *
   * @return the number of error log
   */
  public static long getError() {
    return sCount.get(ERROR);
  }

  /**
   * Gets the number of warn log.
   *
   * @return the number of warn log
   */
  public static long getWarn() {
    return sCount.get(WARN);
  }

  /**
   * Gets the number of info log.
   *
   * @return the number of info log
   */
  public static long getInfo() {
    return sCount.get(INFO);
  }

  /**
   * Add the number of corresponding level log.
   *
   * @param event event of generating log
   */
  public void append(LoggingEvent event) {
    Level level = event.getLevel();
    if (level.equals(Level.INFO)) {
      sCount.incr(INFO);
    } else if (level.equals(Level.WARN)) {
      sCount.incr(WARN);
    } else if (level.equals(Level.ERROR)) {
      sCount.incr(ERROR);
    } else if (level.equals(Level.FATAL)) {
      sCount.incr(FATAL);
    }
  }

  /**
   * Release any resources allocated within the appender such as file
   * handles, network connections, etc.
   */
  public void close() {
  }

  /**
   * Configurators call this method to determine if the appender
   * requires a layout.
   *
   * @return if the appender requires a layout
   */
  public boolean requiresLayout() {
    return false;
  }

  private static class EventCounts {
    private final long[] mCounts;

    private EventCounts() {
      mCounts = new long[]{0L, 0L, 0L, 0L};
    }

    private synchronized void incr(int i) {
      mCounts[i]++;
    }

    private synchronized long get(int i) {
      return mCounts[i];
    }
  }
}
