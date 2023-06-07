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

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Tests for {@link Interval}.
 */
public final class IntervalTest {
  @Test
  public void createAfter() {
    for (int i = 0; i < 100; i++) {
      long startMs = ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
      Interval interval = Interval.after(startMs);
      Assert.assertTrue(interval.isValid());
      Assert.assertEquals(startMs, interval.getStartMs());
      Assert.assertEquals(Long.MAX_VALUE, interval.getEndMs());
    }
  }

  @Test
  public void createBefore() {
    for (int i = 0; i < 100; i++) {
      long endMs = ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
      Interval interval = Interval.before(endMs);
      Assert.assertTrue(interval.isValid());
      Assert.assertEquals(endMs, interval.getEndMs());
      Assert.assertEquals(0, interval.getStartMs());
    }
  }

  @Test
  public void createBetween() {
    for (int i = 0; i < 100; i++) {
      List<Long> sortedTimes =
          ThreadLocalRandom.current().longs(0, Long.MAX_VALUE).limit(2).sorted().boxed()
              .collect(Collectors.toList());
      long startMs = sortedTimes.get(0);
      long endMs = sortedTimes.get(1);
      Interval interval = Interval.between(startMs, endMs);
      Assert.assertTrue(interval.isValid());
      Assert.assertEquals(startMs, interval.getStartMs());
      Assert.assertEquals(endMs, interval.getEndMs());
    }
  }

  @Test
  public void createBetweenInvalid() {
    for (int i = 0; i < 100; i++) {
      List<Long> sortedTimes =
          ThreadLocalRandom.current().longs(0, Long.MAX_VALUE).distinct().limit(2).sorted().boxed()
              .collect(Collectors.toList());
      Interval interval = Interval.between(sortedTimes.get(1), sortedTimes.get(0));
      Assert.assertFalse(interval.isValid());
      Assert.assertEquals(Interval.UNDEFINED_MS, interval.getStartMs());
      Assert.assertEquals(Interval.UNDEFINED_MS, interval.getEndMs());
    }
  }

  @Test
  public void createAlways() {
    Assert.assertTrue(Interval.ALWAYS.isValid());
    Assert.assertEquals(0, Interval.ALWAYS.getStartMs());
    Assert.assertEquals(Long.MAX_VALUE, Interval.ALWAYS.getEndMs());
  }

  @Test
  public void createNever() {
    Assert.assertFalse(Interval.NEVER.isValid());
    Assert.assertEquals(Interval.UNDEFINED_MS, Interval.NEVER.getStartMs());
    Assert.assertEquals(Interval.UNDEFINED_MS, Interval.NEVER.getEndMs());
  }

  @Test
  public void intersectAlways() {
    for (int i = 0; i < 100; i++) {
      List<Long> sortedTimes =
          ThreadLocalRandom.current().longs(0, Long.MAX_VALUE).limit(2).sorted().boxed()
              .collect(Collectors.toList());
      long startMs = sortedTimes.get(0);
      long endMs = sortedTimes.get(1);
      Interval interval = Interval.between(startMs, endMs);
      Interval intersect = interval.intersect(Interval.ALWAYS);
      Assert.assertTrue(intersect.isValid());
      Assert.assertEquals(interval, intersect);
      intersect = Interval.ALWAYS.intersect(interval);
      Assert.assertTrue(intersect.isValid());
      Assert.assertEquals(interval, intersect);
    }
  }

  @Test
  public void intersectNever() {
    for (int i = 0; i < 100; i++) {
      List<Long> sortedTimes =
          ThreadLocalRandom.current().longs(0, Long.MAX_VALUE).distinct().limit(2).sorted().boxed()
              .collect(Collectors.toList());
      Interval interval = Interval.between(sortedTimes.get(1), sortedTimes.get(0));
      Interval intersect = interval.intersect(Interval.NEVER);
      Assert.assertFalse(intersect.isValid());
      intersect = Interval.NEVER.intersect(interval);
      Assert.assertFalse(intersect.isValid());
    }
  }

  @Test
  public void intersect() {
    for (int i = 0; i < 100; i++) {
      List<Long> sortedTimes =
          ThreadLocalRandom.current().longs(0, Long.MAX_VALUE).limit(4).sorted().boxed()
              .collect(Collectors.toList());
      // the intersection does exist
      Interval i1 = Interval.between(sortedTimes.get(0), sortedTimes.get(2));
      Interval i2 = Interval.between(sortedTimes.get(1), sortedTimes.get(3));

      Interval intersect = i1.intersect(i2);
      Assert.assertTrue(intersect.isValid());
      Assert.assertEquals(sortedTimes.get(1).longValue(), intersect.getStartMs());
      Assert.assertEquals(sortedTimes.get(2).longValue(), intersect.getEndMs());
      intersect = i2.intersect(i1);
      Assert.assertTrue(intersect.isValid());
      Assert.assertEquals(sortedTimes.get(1).longValue(), intersect.getStartMs());
      Assert.assertEquals(sortedTimes.get(2).longValue(), intersect.getEndMs());

      // the intersection does not exist
      i1 = Interval.between(sortedTimes.get(0), sortedTimes.get(1));
      i2 = Interval.between(sortedTimes.get(2), sortedTimes.get(3));

      intersect = i1.intersect(i2);
      Assert.assertFalse(intersect.isValid());
      intersect = i2.intersect(i1);
      Assert.assertFalse(intersect.isValid());

      // the intervals share the same value, intersection does not exist
      i1 = Interval.between(sortedTimes.get(0), sortedTimes.get(1));
      i2 = Interval.between(sortedTimes.get(1), sortedTimes.get(3));

      intersect = i1.intersect(i2);
      Assert.assertFalse(intersect.isValid());
      intersect = i2.intersect(i1);
      Assert.assertFalse(intersect.isValid());
    }
  }

  @Test
  public void intersectTouch() {
    List<Long> sortedTimes =
        ThreadLocalRandom.current().longs(0, Long.MAX_VALUE).limit(3).sorted().boxed()
            .collect(Collectors.toList());

    // the intervals share the same value, intersection does not exist
    Interval i1 = Interval.between(sortedTimes.get(0), sortedTimes.get(1));
    Interval i2 = Interval.between(sortedTimes.get(1), sortedTimes.get(2));

    Interval intersect = i1.intersect(i2);
    Assert.assertFalse(intersect.isValid());
    intersect = i2.intersect(i1);
    Assert.assertFalse(intersect.isValid());
  }

  @Test
  public void negateAlways() {
    List<Interval> neg = Interval.ALWAYS.negate().getIntervals();
    Assert.assertTrue(neg.size() == 1);
    Interval in = neg.get(0);
    Assert.assertEquals(Interval.NEVER, in);
  }

  @Test
  public void negateNever() {
    List<Interval> neg = Interval.NEVER.negate().getIntervals();
    Assert.assertTrue(neg.size() == 1);
    Interval in = neg.get(0);
    Assert.assertEquals(Interval.ALWAYS, in);
  }

  @Test
  public void negateBefore() {
    for (int i = 0; i < 100; i++) {
      long endMs = ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
      Interval interval = Interval.before(endMs);
      List<Interval> neg = interval.negate().getIntervals();

      Assert.assertTrue(neg.size() == 1);
      Interval in = neg.get(0);

      Assert.assertTrue(in.isValid());
      Assert.assertEquals(endMs, in.getStartMs());
    }
  }

  @Test
  public void negateAfter() {
    for (int i = 0; i < 100; i++) {
      long startMs = ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);
      Interval interval = Interval.after(startMs);
      List<Interval> neg = interval.negate().getIntervals();

      Assert.assertTrue(neg.size() == 1);
      Interval in = neg.get(0);

      Assert.assertTrue(in.isValid());
      Assert.assertEquals(startMs, in.getEndMs());
    }
  }

  @Test
  public void negate() {
    for (int i = 0; i < 100; i++) {
      List<Long> sortedTimes =
          ThreadLocalRandom.current().longs(1, Long.MAX_VALUE - 1).distinct().limit(2).sorted()
              .boxed().collect(Collectors.toList());
      long startMs = sortedTimes.get(0);
      long endMs = sortedTimes.get(1);
      Interval interval = Interval.between(startMs, endMs);
      Set<Interval> neg = Sets.newHashSet(interval.negate().getIntervals());

      Assert.assertTrue(neg.size() == 2);
      Assert.assertTrue(
          neg.containsAll(Arrays.asList(Interval.before(startMs), Interval.after(endMs))));
    }
  }

  @Test
  public void addBetween() {
    for (int i = 0; i < 100; i++) {
      List<Long> sortedTimes =
          ThreadLocalRandom.current().longs(1, Long.MAX_VALUE - 1).distinct().limit(2).sorted()
              .boxed().collect(Collectors.toList());
      long startMs = sortedTimes.get(0);
      long endMs = sortedTimes.get(1);
      Interval interval = Interval.between(startMs, endMs);
      long addTime = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - endMs - 1);
      Interval newInterval = interval.add(addTime);
      Assert.assertTrue(newInterval.isValid());
      Assert.assertEquals(startMs + addTime, newInterval.getStartMs());
      Assert.assertEquals(endMs + addTime, newInterval.getEndMs());
    }
  }

  @Test
  public void addBefore() {
    for (int i = 0; i < 100; i++) {
      long endMs = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE - 1);
      Interval interval = Interval.before(endMs);
      long addTime = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - endMs - 1);
      Interval newInterval = interval.add(addTime);
      Assert.assertTrue(newInterval.isValid());
      Assert.assertEquals(interval.getStartMs(), newInterval.getStartMs());
      Assert.assertEquals(endMs + addTime, newInterval.getEndMs());
    }
  }

  @Test
  public void addAfter() {
    for (int i = 0; i < 100; i++) {
      long startMs = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE - 1);
      Interval interval = Interval.after(startMs);
      long addTime = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - startMs - 1);
      Interval newInterval = interval.add(addTime);
      Assert.assertTrue(newInterval.isValid());
      Assert.assertEquals(interval.getEndMs(), newInterval.getEndMs());
      Assert.assertEquals(startMs + addTime, newInterval.getStartMs());
    }
  }

  @Test
  public void addOverflow() {
    for (int i = 0; i < 100; i++) {
      List<Long> sortedTimes =
          ThreadLocalRandom.current().longs(1, Long.MAX_VALUE - 1).distinct().limit(2).sorted()
              .boxed().collect(Collectors.toList());
      long startMs = sortedTimes.get(0);
      long endMs = sortedTimes.get(1);
      Interval interval = Interval.between(startMs, endMs);
      long addTime = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE - endMs, Long.MAX_VALUE);
      try {
        interval.add(addTime);
      } catch (ArithmeticException e) {
        // expect exception to throw for add overflow
        continue;
      }
      Assert.fail("Add overflow should throw an ArithmeticException.");
    }
  }
}
