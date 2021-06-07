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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Bucket Counter Utility.
 */
public class BucketCounter {
  // Counts the number of values less or equal to the key
  private Map<Long, LongAdder> mCounter;
  private List<Long> mIntervals;

  /**
   * Bucket Counter constructor.
   *
   * @param intervals sorted intervals for bucket counting
   */
  public BucketCounter(List<Long> intervals) {
    mCounter = new HashMap<>();
    mIntervals = new ArrayList<>(intervals);
    mIntervals.add(Long.MAX_VALUE);
    for (Long interval : intervals) {
      mCounter.put(interval, new LongAdder());
    }
    mCounter.put(Long.MAX_VALUE, new LongAdder());
  }

  private LongAdder getStartInterval(Long value) {
    for (int i = 0; i < mIntervals.size(); i++) {
      if (mIntervals.get(i) >= value) {
        return mCounter.get(mIntervals.get(i));
      }
    }
    return mCounter.get(Long.MAX_VALUE);
  }

  /**
   * insert a number to be counted.
   * @param number the number to be counted
   */
  public void insert(Long number) {
    getStartInterval(number).increment();
  }

  /**
   * remove a number to be counted.
   * @param number the number to be counted
   */
  public void remove(Long number) {
    getStartInterval(number).decrement();
  }

  /**
   * @return counters
   */
  public Map<Long, Number> getCounters() {
    return new HashMap<>(mCounter);
  }
}
