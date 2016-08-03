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
 * A modifiable clock useful for testing.
 */
public final class TestClock implements Clock {
  private long mTime;

  /**
   * Constructs a TestClock set to the current system time.
   */
  public TestClock() {
    this(System.currentTimeMillis());
  }

  /**
   * Constructs a TestClock set to the specified time.
   *
   * @param time the time to set the clock to
   */
  public TestClock(long time) {
    mTime = time;
  }

  /**
   * Sets the clock to the specified time.
   *
   * @param time the time to set the clock to
   */
  public synchronized void setTimeMs(long time) {
    mTime = time;
  }

  @Override
  public synchronized long millis() {
    return mTime;
  }
}
