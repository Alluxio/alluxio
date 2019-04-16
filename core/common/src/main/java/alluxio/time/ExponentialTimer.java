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

import alluxio.retry.RetryPolicy;
import alluxio.retry.ExponentialBackoffRetry;

/**
 * The {@link ExponentialTimer} can be used for generating a sequence of events that are
 * exponentially distributed in time.
 *
 * The intended usage is to associate the timer with an operation that should to be re-attempted
 * if it fails using exponential back-off. For instance, an event loop iterates over scheduled
 * operations each of which is associated with a timer and the event loop can use the timer to check
 * if the operation is ready to be attempted and whether it should be re-attempted again in the
 * future in case it fails.
 *
 * <pre>
 * while (true) {
 *   Operation op = operations.pop();
 *   switch (op.getTimer().tick()) {
 *   case EXPIRED:
 *     // operation will not be re-attempted
 *     break;
 *   case NOT_READY:
 *     // operation is not ready to be re-attempted
 *     operations.push(op);
 *     break;
 *   case READY:
 *     boolean success = op.run();
 *     if (!success) {
 *       operations.push(op);
 *     }
 *   }
 *   Thread.sleep(10);
 * }
 * </pre>
 *
 * Note that in the above scenario, the {@link ExponentialBackoffRetry} policy cannot be used
 * because the {@link RetryPolicy#attemptRetry()} is blocking.
 */
public class ExponentialTimer {

  /**
   * Represents the result of {@link #tick()}.
   */
  public enum Result {
    EXPIRED,
    NOT_READY,
    READY,
  }

  /** The maximum interval time between events (in milliseconds). */
  private final long mMaxIntervalMs;
  /** The last event horizon (in milliseconds). */
  private final long mLastEventHorizonMs;

  /** The number of generated events. */
  private long mNumEvents;
  /** The time of the next event. */
  private long mNextEventMs;
  /** The current interval between events (in milliseconds). */
  private long mIntervalMs;

  /**
   * Creates a new instance of {@link ExponentialTimer}.
   *
   * @param initialIntervalMs the initial interval between events (in milliseconds)
   * @param maxIntervalMs the maximum interval between events (in milliseconds)
   * @param initialWaitTimeMs the initial wait time before first event (in milliseconds)
   * @param maxTotalWaitTimeMs the maximum total wait time (in milliseconds)
   */
  public ExponentialTimer(long initialIntervalMs, long maxIntervalMs, long initialWaitTimeMs,
      long maxTotalWaitTimeMs) {
    mMaxIntervalMs = maxIntervalMs;
    mLastEventHorizonMs = System.currentTimeMillis() + maxTotalWaitTimeMs;
    mNextEventMs = System.currentTimeMillis() + initialWaitTimeMs;
    mIntervalMs = Math.min(initialIntervalMs, maxIntervalMs);
    mNumEvents = 0;
  }

  /**
   * @return the number of generated events so far
   */
  public long getNumEvents() {
    return mNumEvents;
  }

  /**
   * Attempts to perform a timer tick. One of three scenarios can be encountered:
   *
   * 1. the timer has expired
   * 2. the next event is not ready
   * 3. the next event is generated
   *
   * The last option has the side-effect of updating the internal state of the timer to reflect
   * the generation of the event.
   *
   * @return the {@link Result} that indicates which scenario was encountered
   */
  public Result tick() {
    if (System.currentTimeMillis() >= mLastEventHorizonMs) {
      return Result.EXPIRED;
    }
    if (System.currentTimeMillis() < mNextEventMs) {
      return Result.NOT_READY;
    }
    mNextEventMs = System.currentTimeMillis() + mIntervalMs;
    long next = Math.min(mIntervalMs * 2, mMaxIntervalMs);
    // Account for overflow.
    if (next < mIntervalMs) {
      next = Integer.MAX_VALUE;
    }
    mIntervalMs = next;
    mNumEvents++;
    return Result.READY;
  }
}
