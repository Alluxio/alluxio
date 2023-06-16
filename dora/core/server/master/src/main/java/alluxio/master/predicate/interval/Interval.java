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

package alluxio.master.predicate.interval;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This represents a time interval, start (inclusive) to end (exclusive).
 */
public class Interval implements Comparable<Interval> {
  private static final Logger LOG = LoggerFactory.getLogger(Interval.class);
  private static final long MIN_MS = 0;
  private static final long MAX_MS = Long.MAX_VALUE;

  /** The undefined ms value for start or end. */
  public static final long UNDEFINED_MS = -1;
  /** The interval that is never possible. */
  public static final Interval NEVER = between(MAX_MS, MIN_MS);
  /** The interval that is always possible. */
  public static final Interval ALWAYS = after(MIN_MS);

  /** The start of the interval, in ms. */
  private final long mStartMs;
  /** The end of the interval, in ms. */
  private final long mEndMs;

  private Interval(long startMs, long endMs) {
    if (startMs >= endMs) {
      // the interval is impossible, so set it as undefined.
      startMs = UNDEFINED_MS;
      endMs = UNDEFINED_MS;
    }
    mStartMs = startMs;
    mEndMs = endMs;
  }

  /**
   * @param startMs the start of the interval, in ms
   * @param endMs the end of the interval, in ms
   * @return the Interval instance, representing the interval between the start and end
   */
  public static Interval between(long startMs, long endMs) {
    return new Interval(startMs, endMs);
  }

  /**
   * @param endMs the end of the interval, in ms
   * @return the Interval instance, representing the interval before the end
   */
  public static Interval before(long endMs) {
    return new Interval(MIN_MS, endMs);
  }

  /**
   * @param startMs the start of the interval, in ms
   * @return the Interval instance, representing the interval after the start
   */
  public static Interval after(long startMs) {
    return new Interval(startMs, MAX_MS);
  }

  /**
   * @return the start of the interval, in ms
   */
  public long getStartMs() {
    return mStartMs;
  }

  /**
   * @return the end of the interval, in ms
   */
  public long getEndMs() {
    return mEndMs;
  }

  /**
   * @return if the interval is valid/possible
   */
  public boolean isValid() {
    return mStartMs != UNDEFINED_MS;
  }

  /**
   * @param other the other Interval
   * @return a new interval which represents the intersection with the other interval
   */
  public Interval intersect(Interval other) {
    return between(Math.max(mStartMs, other.mStartMs), Math.min(mEndMs, other.mEndMs));
  }

  /**
   * @return an IntervalSet which represent the negation of this interval
   */
  public IntervalSet negate() {
    if (!isValid()) {
      return IntervalSet.ALWAYS;
    }
    if (mStartMs == MIN_MS) {
      if (mEndMs == MAX_MS) {
        // this is ALWAYS, so the negation is never
        return IntervalSet.NEVER;
      }
      return new IntervalSet(after(mEndMs));
    }
    // start is after min
    if (mEndMs == MAX_MS) {
      return new IntervalSet(before(mStartMs));
    }

    // start is after min, and end is before max. This requires 2 intervals.
    return new IntervalSet(Lists.newArrayList(before(mStartMs), after(mEndMs)));
  }

  /**
   * Add an offset to the interval. Endpoints with MIN_VALUE and MAX_VALUE will not change.
   *
   * @param value the value to add
   * @return the new interval with the offset
   * @throws ArithmeticException if the result overflows
   */
  public Interval add(long value) {
    if (equals(ALWAYS) || equals(NEVER) || !isValid()) {
      return this;
    }
    if (mStartMs == MIN_MS) {
      return Interval.before(Math.addExact(mEndMs, value));
    }
    if (mEndMs == MAX_MS) {
      return Interval.after(Math.addExact(mStartMs, value));
    }
    return Interval.between(Math.addExact(mStartMs, value), Math.addExact(mEndMs, value));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mStartMs, mEndMs);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Interval other = (Interval) o;
    return Objects.equal(mStartMs, other.mStartMs) && Objects.equal(mEndMs, other.mEndMs);
  }

  @Override
  public int compareTo(Interval o) {
    if (mStartMs < o.mStartMs) {
      return -1;
    }
    if (mStartMs > o.mStartMs) {
      return 1;
    }
    // same start time
    if (mEndMs < o.mEndMs) {
      return -1;
    }
    if (mEndMs > o.mEndMs) {
      return 1;
    }
    return 0;
  }

  @Override
  public String toString() {
    if (!isValid()) {
      return "Interval[invalid]";
    }
    if (mEndMs == MAX_MS) {
      return "Interval[" + mStartMs + ", inf)";
    }
    return "Interval[" + mStartMs + ", " + mEndMs + ")";
  }
}
