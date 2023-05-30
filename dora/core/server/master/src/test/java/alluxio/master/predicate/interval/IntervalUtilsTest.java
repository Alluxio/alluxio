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

/**
 * Tests for {@link IntervalUtils}.
 */
public final class IntervalUtilsTest {
  @Test
  public void normalizeSimple() {
    List<Interval> i;

    i = IntervalUtils.normalize(Collections.emptyList());
    Assert.assertTrue(i.isEmpty());

    i = IntervalUtils.normalize(Arrays.asList(Interval.NEVER, Interval.NEVER, Interval.NEVER));
    Assert.assertTrue(i.isEmpty());

    i = IntervalUtils.normalize(Arrays.asList(Interval.ALWAYS, Interval.ALWAYS, Interval.ALWAYS));
    Assert.assertEquals(1, i.size());
    Assert.assertEquals(Interval.ALWAYS, i.get(0));

    i = IntervalUtils.normalize(Arrays.asList(Interval.NEVER, Interval.ALWAYS));
    Assert.assertEquals(1, i.size());
    Assert.assertEquals(Interval.ALWAYS, i.get(0));

    i = IntervalUtils.normalize(Arrays.asList(Interval.between(1, 2)));
    Assert.assertEquals(1, i.size());
    Assert.assertEquals(Interval.between(1, 2), i.get(0));

    i = IntervalUtils.normalize(Arrays.asList(Interval.NEVER, Interval.between(1, 2)));
    Assert.assertEquals(1, i.size());
    Assert.assertEquals(Interval.between(1, 2), i.get(0));
  }

  @Test
  public void normalizeMerge() {
    List<Interval> i;

    i = IntervalUtils.normalize(Arrays.asList(Interval.between(1, 3), Interval.between(2, 4)));
    Assert.assertEquals(1, i.size());
    Assert.assertEquals(Interval.between(1, 4), i.get(0));

    i = IntervalUtils.normalize(Arrays.asList(Interval.between(2, 4), Interval.between(1, 3)));
    Assert.assertEquals(1, i.size());
    Assert.assertEquals(Interval.between(1, 4), i.get(0));

    i = IntervalUtils.normalize(Arrays.asList(Interval.between(2, 3), Interval.between(1, 4)));
    Assert.assertEquals(1, i.size());
    Assert.assertEquals(Interval.between(1, 4), i.get(0));

    i = IntervalUtils.normalize(Arrays.asList(Interval.between(2, 4), Interval.between(1, 2)));
    Assert.assertEquals(1, i.size());
    Assert.assertEquals(Interval.between(1, 4), i.get(0));

    i = IntervalUtils.normalize(Arrays.asList(Interval.ALWAYS, Interval.NEVER));
    Assert.assertEquals(1, i.size());
    Assert.assertEquals(Interval.ALWAYS, i.get(0));

    i = IntervalUtils.normalize(Arrays.asList(Interval.ALWAYS, Interval.between(2, 4),
        Interval.NEVER));
    Assert.assertEquals(1, i.size());
    Assert.assertEquals(Interval.ALWAYS, i.get(0));
  }

  @Test
  public void normalizeDisjoint() {
    List<Interval> l;
    Set<Interval> s;

    l = Arrays.asList(Interval.between(1, 3), Interval.between(11, 13));
    s = Sets.newHashSet(IntervalUtils.normalize(l));
    Assert.assertEquals(l.size(), s.size());
    Assert.assertTrue(s.containsAll(l));

    l = Arrays.asList(Interval.between(11, 13), Interval.between(1, 3));
    s = Sets.newHashSet(IntervalUtils.normalize(l));
    Assert.assertEquals(l.size(), s.size());
    Assert.assertTrue(s.containsAll(l));

    l = Arrays.asList(Interval.between(21, 23), Interval.between(11, 13), Interval.between(1, 3));
    s = Sets.newHashSet(IntervalUtils.normalize(l));
    Assert.assertEquals(l.size(), s.size());
    Assert.assertTrue(s.containsAll(l));
  }

  @Test
  public void intersect() {
    IntervalSet s1;
    IntervalSet s2;
    Set<Interval> s;

    s1 = new IntervalSet(Arrays.asList(Interval.between(1, 3), Interval.between(11, 13)));
    s = Sets.newHashSet(
        IntervalUtils.intersect(Arrays.asList(s1, IntervalSet.NEVER)).getIntervals());
    Assert.assertEquals(1, s.size());
    Assert.assertTrue(s.contains(Interval.NEVER));

    s1 = new IntervalSet(Arrays.asList(Interval.between(1, 3), Interval.between(11, 13)));
    s2 = new IntervalSet(Arrays.asList(Interval.between(22, 24), Interval.between(32, 34)));
    s = Sets.newHashSet(IntervalUtils.intersect(Arrays.asList(s1, s2)).getIntervals());
    Assert.assertEquals(1, s.size());
    Assert.assertTrue(s.contains(Interval.NEVER));

    s1 = new IntervalSet(Arrays.asList(Interval.between(1, 3), Interval.between(11, 13)));
    s2 = new IntervalSet(Arrays.asList(Interval.between(2, 4), Interval.between(12, 14)));
    s = Sets.newHashSet(IntervalUtils.intersect(Arrays.asList(s1, s2)).getIntervals());
    Assert.assertEquals(2, s.size());
    Assert
        .assertTrue(s.containsAll(Arrays.asList(Interval.between(2, 3), Interval.between(12, 13))));

    s1 = new IntervalSet(Arrays.asList(Interval.between(11, 13), Interval.between(1, 3)));
    s2 = new IntervalSet(Arrays.asList(Interval.between(12, 14), Interval.between(2, 4)));
    s = Sets.newHashSet(IntervalUtils.intersect(Arrays.asList(s1, s2)).getIntervals());
    Assert.assertEquals(2, s.size());
    Assert
        .assertTrue(s.containsAll(Arrays.asList(Interval.between(2, 3), Interval.between(12, 13))));

    s1 = new IntervalSet(
        Arrays.asList(Interval.between(11, 13), Interval.between(1, 3), Interval.between(21, 23)));
    s2 = new IntervalSet(
        Arrays.asList(Interval.between(12, 14), Interval.between(2, 4), Interval.between(31, 33)));
    s = Sets.newHashSet(IntervalUtils.intersect(Arrays.asList(s1, s2)).getIntervals());
    Assert.assertEquals(2, s.size());
    Assert
        .assertTrue(s.containsAll(Arrays.asList(Interval.between(2, 3), Interval.between(12, 13))));

    s1 = new IntervalSet(Arrays.asList(Interval.after(8), Interval.after(18)));
    s2 = new IntervalSet(Arrays.asList(Interval.before(10), Interval.before(2)));
    s = Sets.newHashSet(IntervalUtils.intersect(Arrays.asList(s1, s2)).getIntervals());
    Assert.assertEquals(1, s.size());
    Assert.assertTrue(s.containsAll(Arrays.asList(Interval.between(8, 10))));
  }
}
