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


public class EventCounter extends AppenderSkeleton {
  private static final int FATAL = 0;
  private static final int ERROR = 1;
  private static final int WARN = 2;
  private static final int INFO = 3;
  private static EventCounter.EventCounts mCounts = new EventCounter.EventCounts();

  public EventCounter() { }
  public static long getFatal() {
    return mCounts.get(0);
  }
  public static long getError() {
    return mCounts.get(1);
    }
  public static long getWarn() {
    return mCounts.get(2);
    }
  public static long getInfo() {
    return mCounts.get(3);
    }
  public void append(LoggingEvent event) {
      Level level = event.getLevel();
      if (level.equals(Level.INFO)) {
          mCounts.incr(3);
      } else if (level.equals(Level.WARN)) {
          mCounts.incr(2);
      } else if (level.equals(Level.ERROR)) {
          mCounts.incr(1);
      } else if (level.equals(Level.FATAL)) {
          mCounts.incr(0);
      }

  }
  public void close() {
  }
  public boolean requiresLayout() {
    return false;
  }

  private static class EventCounts {
  private final long[] counts;

  private EventCounts() {
        this.counts = new long[]{0L, 0L, 0L, 0L};
    }

  private synchronized void incr(int i) {
      this.counts[i]++;
  }
  private synchronized long get(int i) {
     return this.counts[i];
    }
  }


}
