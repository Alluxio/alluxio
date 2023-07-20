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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Tests for {@link IntervalSet}.
 */
public final class IntervalSetTest {
  @Test
  public void isValid() {
    Assert.assertTrue(IntervalSet.ALWAYS.isValid());
    Assert.assertFalse(IntervalSet.NEVER.isValid());

    Assert.assertTrue(new IntervalSet(Interval.between(1, 2)).isValid());
    Assert.assertTrue(new IntervalSet(Interval.before(10)).isValid());
    Assert.assertTrue(new IntervalSet(Interval.after(10)).isValid());

    Assert.assertFalse(new IntervalSet(Interval.between(2, 1)).isValid());
    Assert.assertFalse(new IntervalSet(Collections.emptyList()).isValid());
  }

  @Test
  public void intersectNever() {
    IntervalSet is = IntervalSet.NEVER.intersect(new IntervalSet(Interval.between(1, 2)));
    Set<Interval> s = Sets.newHashSet(is.getIntervals());
    Assert.assertEquals(1, s.size());
    Assert.assertTrue(s.contains(Interval.NEVER));
  }

  @Test
  public void intersectAlways() {
    for (int i = 0; i < 100; i++) {
      List<Long> sortedTimes =
          ThreadLocalRandom.current().longs(0, Long.MAX_VALUE).distinct().limit(2).sorted()
              .boxed().collect(Collectors.toList());
      long startMs = sortedTimes.get(0);
      long endMs = sortedTimes.get(1);

      Interval interval = Interval.between(startMs, endMs);

      IntervalSet is = IntervalSet.ALWAYS.intersect(new IntervalSet(interval));
      Set<Interval> s = Sets.newHashSet(is.getIntervals());
      Assert.assertEquals(1, s.size());
      Assert.assertTrue(s.contains(interval));
    }
  }

  @Test
  public void intersect() {
    for (int i = 0; i < 100; i++) {
      List<Long> sortedTimes =
          ThreadLocalRandom.current().longs(0, Long.MAX_VALUE).distinct().limit(4).sorted()
              .boxed().collect(Collectors.toList());
      Interval int1 = Interval.between(sortedTimes.get(0), sortedTimes.get(2));
      Interval int2 = Interval.between(sortedTimes.get(1), sortedTimes.get(3));

      IntervalSet is = new IntervalSet(int1).intersect(new IntervalSet(int2));
      Set<Interval> s = Sets.newHashSet(is.getIntervals());
      Assert.assertEquals(1, s.size());
      Assert.assertTrue(s.contains(Interval.between(sortedTimes.get(1), sortedTimes.get(2))));

      is = new IntervalSet(Interval.after(sortedTimes.get(2)));
      Assert.assertFalse(is.intersect(is.negate()).isValid());

      is = new IntervalSet(Interval.between(sortedTimes.get(1), sortedTimes.get(2)));
      Assert.assertFalse(is.intersect(is.negate()).isValid());
    }
  }

  @Test
  public void intersectStart() {
    long start = 10;
    IntervalSet is = new IntervalSet(Interval.between(start, start + 20));
    IntervalSet other = new IntervalSet(Interval.between(start, start + 1));

    IntervalSet result = is.intersect(other);
    Assert.assertTrue(result.isValid());
    Assert.assertEquals(1, result.getIntervals().size());
    Assert.assertEquals(other.getIntervals().get(0), result.getIntervals().get(0));

    // negate the first interval, and the resulting one should not intersect with the other interval
    IntervalSet negate = is.negate();
    result = negate.intersect(other);
    Assert.assertFalse(result.isValid());
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
      List<Interval> negationList = new IntervalSet(interval).negate().getIntervals();

      Assert.assertTrue(negationList.size() == 2);
      Assert.assertTrue(Sets.newHashSet(negationList)
          .containsAll(Arrays.asList(Interval.before(startMs), Interval.after(endMs))));

      // negate again
      List<Interval> negationList2 = new IntervalSet(negationList).negate().getIntervals();
      Assert.assertTrue(negationList2.size() == 1);
      Assert.assertTrue(Sets.newHashSet(negationList2).contains(interval));
    }
  }

  @Test
  public void negateBefore() {
    for (int i = 0; i < 100; i++) {
      long endMs = ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);

      Interval interval = Interval.before(endMs);
      List<Interval> negationList = new IntervalSet(interval).negate().getIntervals();

      Assert.assertTrue(negationList.size() == 1);
      Assert.assertTrue(Sets.newHashSet(negationList).contains(Interval.after(endMs)));

      // negate again
      List<Interval> negationList2 = new IntervalSet(negationList).negate().getIntervals();
      Assert.assertTrue(negationList2.size() == 1);
      Assert.assertTrue(Sets.newHashSet(negationList2).contains(interval));
    }
  }

  @Test
  public void negateAfter() {
    for (int i = 0; i < 100; i++) {
      long startMs = ThreadLocalRandom.current().nextLong(0, Long.MAX_VALUE);

      Interval interval = Interval.after(startMs);
      List<Interval> negationList = new IntervalSet(interval).negate().getIntervals();

      Assert.assertTrue(negationList.size() == 1);
      Assert.assertTrue(Sets.newHashSet(negationList).contains(Interval.before(startMs)));

      // negate again
      List<Interval> negationList2 = new IntervalSet(negationList).negate().getIntervals();
      Assert.assertTrue(negationList2.size() == 1);
      Assert.assertTrue(Sets.newHashSet(negationList2).contains(interval));
    }
  }

  @Test
  public void negateMultiple() {
    for (int i = 0; i < 100; i++) {
      List<Long> sortedTimes =
          ThreadLocalRandom.current().longs(1, Long.MAX_VALUE - 1).distinct().limit(6).sorted()
              .boxed().collect(Collectors.toList());

      Interval int1 = Interval.between(sortedTimes.get(0), sortedTimes.get(1));
      Interval int2 = Interval.between(sortedTimes.get(2), sortedTimes.get(3));
      Interval int3 = Interval.between(sortedTimes.get(4), sortedTimes.get(5));

      List<Interval> negationList =
          new IntervalSet(Arrays.asList(int3, int2, int1)).negate().getIntervals();

      Assert.assertTrue(negationList.size() == 4);
      Assert.assertTrue(Sets.newHashSet(negationList).containsAll(Arrays.asList(
          Interval.before(int1.getStartMs()),
          Interval.between(int1.getEndMs(), int2.getStartMs()),
          Interval.between(int2.getEndMs(), int3.getStartMs()),
          Interval.after(int3.getEndMs())
      )));

      // negate again
      List<Interval> negationList2 = new IntervalSet(negationList).negate().getIntervals();
      Assert.assertTrue(negationList2.size() == 3);
      Assert
          .assertTrue(Sets.newHashSet(negationList2).containsAll(Arrays.asList(int3, int2, int1)));
    }
  }
}
