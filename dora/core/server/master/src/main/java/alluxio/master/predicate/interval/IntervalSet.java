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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This represents a set of time intervals.
 */
public class IntervalSet {
  private static final Logger LOG = LoggerFactory.getLogger(IntervalSet.class);

  /** The interval set that is never possible. */
  public static final IntervalSet NEVER = new IntervalSet(Interval.NEVER);
  /** The interval set that is always possible. */
  public static final IntervalSet ALWAYS = new IntervalSet(Interval.ALWAYS);

  /** The list of intervals. */
  private final List<Interval> mIntervals;

  /**
   * Creates a new instance.
   *
   * @param intervals the list of intervals
   */
  public IntervalSet(List<Interval> intervals) {
    if (intervals == null || intervals.isEmpty()) {
      mIntervals = Collections.singletonList(Interval.NEVER);
      return;
    }
    mIntervals = new ArrayList<>(intervals);
  }

  /**
   * Creates a new instance, with a single Interval.
   *
   * @param interval the interval
   */
  public IntervalSet(Interval interval) {
    if (interval == null) {
      mIntervals = Collections.singletonList(Interval.NEVER);
      return;
    }
    mIntervals = Collections.singletonList(interval);
  }

  /**
   * @return the list of intervals
   */
  public List<Interval> getIntervals() {
    return mIntervals;
  }

  /**
   * @return true if the interval set is valid/possible
   */
  public boolean isValid() {
    if (mIntervals.isEmpty()) {
      return false;
    }
    if (mIntervals.size() == 1 && mIntervals.get(0).equals(Interval.NEVER)) {
      return false;
    }
    return true;
  }

  /**
   * @param other the other Interval set
   * @return a new interval set which represents the intersection with the other interval set
   */
  public IntervalSet intersect(IntervalSet other) {
    List<Interval> intersection = new ArrayList<>(2);
    for (Interval int1 : getIntervals()) {
      for (Interval int2 : other.getIntervals()) {
        Interval i = int1.intersect(int2);
        if (i.isValid()) {
          intersection.add(i);
        }
      }
    }
    if (intersection.isEmpty()) {
      return NEVER;
    }
    return new IntervalSet(IntervalUtils.normalize(intersection));
  }

  /**
   * @return the interval set which represents the negation of this interval set
   */
  public IntervalSet negate() {
    List<IntervalSet> negations = new ArrayList<>();

    for (Interval i : getIntervals()) {
      negations.add(i.negate());
    }

    if (negations.isEmpty()) {
      return NEVER;
    }

    if (negations.size() == 1) {
      return negations.get(0);
    }

    return IntervalUtils.intersect(negations);
  }
}
