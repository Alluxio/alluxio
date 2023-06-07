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
import java.util.stream.Collectors;

/**
 * This represents a time interval.
 */
public class IntervalUtils {
  private static final Logger LOG = LoggerFactory.getLogger(IntervalUtils.class);

  private IntervalUtils() {} // prevent instantiation

  /**
   * @param intervalSets a list of intervals sets
   * @return the interval set representing the intersection of all the interval sets
   */
  public static IntervalSet intersect(List<IntervalSet> intervalSets) {
    if (intervalSets.isEmpty()) {
      return IntervalSet.NEVER;
    }
    if (intervalSets.size() == 1) {
      return intervalSets.get(0);
    }

    // at least 2 lists of intervals
    IntervalSet intersection = intervalSets.get(0);

    // scan entire list from the second interval
    for (int i = 1; i < intervalSets.size(); i++) {
      intersection = intersection.intersect(intervalSets.get(i));
    }
    return intersection;
  }

  /**
   * @param intervals the list of intervals to normalize
   * @return the normalized list of intervals
   */
  public static List<Interval> normalize(List<Interval> intervals) {
    if (intervals.size() <= 1) {
      return intervals;
    }

    List<Interval> valid =
        intervals.stream().filter(Interval::isValid).collect(Collectors.toList());
    if (valid.size() <= 1) {
      return valid;
    }
    // 2 or more intervals
    List<Interval> result = new ArrayList<>(valid.size());
    Collections.sort(valid);

    long start = valid.get(0).getStartMs();
    long end = valid.get(0).getEndMs();
    // scan entire list from the second interval
    for (int i = 1; i < valid.size(); i++) {
      Interval interval = valid.get(i);
      if (interval.getStartMs() <= end) {
        // continue with the same interval
        end = Math.max(end, interval.getEndMs());
      } else {
        // These are disjoint. add the previous interval
        result.add(Interval.between(start, end));
        start = interval.getStartMs();
        end = interval.getEndMs();
      }
    }
    // add the last interval
    result.add(Interval.between(start, end));
    if (result.isEmpty()) {
      return Collections.emptyList();
    }
    return result;
  }
}
