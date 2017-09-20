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

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * A simplified StopWatch implementation which can measure times in nanoseconds.
 */
public class StopWatch implements Closeable {
  private boolean mIsStarted;
  private long mStartNanos;
  private long mCurrentElapsedNanos;

  /**
   * StopWatch.
   */
  public StopWatch() {}

  /**
   * The method is used to find out if the StopWatch is started.
   * @return boolean If the StopWatch is started
   */
  public boolean isRunning() {
    return mIsStarted;
  }

  /**
   * Start to measure times and make the state of stopwatch running.
   * @return this instance of StopWatch
   */
  public StopWatch start() {
    if (mIsStarted) {
      throw new IllegalStateException("StopWatch is already running");
    }
    mIsStarted = true;
    mStartNanos = System.nanoTime();
    return this;
  }

  /**
   * Stop elapsed time and make the state of stopwatch stop.
   * @return this instance of StopWatch
   */
  public StopWatch stop() {
    if (!mIsStarted) {
      throw new IllegalStateException("StopWatch is already stopped");
    }
    long now = System.nanoTime();
    mIsStarted = false;
    mCurrentElapsedNanos += now - mStartNanos;
    return this;
  }

  /**
   * Reset elapsed time to zero and make the state of stopwatch stop.
   * @return this instance of StopWatch
   */
  public StopWatch reset() {
    mCurrentElapsedNanos = 0;
    mIsStarted = false;
    return this;
  }

  /**
   * @param timeUnit timeUnit
   * @return current elapsed time in specified timeunit
   */
  public long now(TimeUnit timeUnit) {
    return timeUnit.convert(now(), TimeUnit.NANOSECONDS);

  }

  /**
   * @return current elapsed time in nanosecond
   */
  public long now() {
    return mIsStarted
        ? System.nanoTime() - mStartNanos + mCurrentElapsedNanos :
        mCurrentElapsedNanos;
  }

  @Override
  public String toString() {
    return String.valueOf(now());
  }

  @Override
  public void close() {
    if (mIsStarted) {
      stop();
    }
  }
}
