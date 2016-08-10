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

package alluxio.clock;

/**
 * A manually set clock useful for testing.
 */
public final class ManualClock implements Clock {
  private long mTimeMs;

  /**
   * Constructs a {@link ManualClock} set to the current system time.
   */
  public ManualClock() {
    this(System.currentTimeMillis());
  }

  /**
   * Constructs a {@link ManualClock} set to the specified time.
   *
   * @param time the time to set the clock to
   */
  public ManualClock(long time) {
    mTimeMs = time;
  }

  /**
   * Sets the clock to the specified time.
   *
   * @param timeMs the time to set the clock to
   */
  public synchronized void setTimeMs(long timeMs) {
    mTimeMs = timeMs;
  }

  /**
   * Moves the clock forward the specified amount of time.
   *
   * @param timeMs the time to add in milliseconds
   */
  public synchronized void addTimeMs(long timeMs) {
    mTimeMs += timeMs;
  }

  @Override
  public synchronized long millis() {
    return mTimeMs;
  }
}
