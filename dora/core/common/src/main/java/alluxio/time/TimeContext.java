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

import java.time.Clock;

/**
 * Context for managing time.
 */
public final class TimeContext {
  public static final TimeContext SYSTEM =
      new TimeContext(Clock.systemUTC(), ThreadSleeper.INSTANCE);

  private final Clock mClock;
  private final Sleeper mSleeper;

  /**
   * @param clock the clock for this context
   * @param sleeper the sleeper for this context
   */
  public TimeContext(Clock clock, Sleeper sleeper) {
    mClock = clock;
    mSleeper = sleeper;
  }

  /**
   * @return the clock for this context
   */
  public Clock getClock() {
    return mClock;
  }

  /**
   * @return the sleeper for thix context
   */
  public Sleeper getSleeper() {
    return mSleeper;
  }
}
