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

package alluxio.collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class LatencyReportingQueueTest {

  @Test
  public void testEMAUpdate() throws InterruptedException {
    // 1-second EMA
    LatencyReportingLinkedBlockingQueue.EMA x =
        new LatencyReportingLinkedBlockingQueue.EMA(1.0f / 60);
    long utime = System.currentTimeMillis();
    // Add about a second, the EMA is expected to be around 2/3 of the value provided (100)
    x.update(utime + 1000, 100);
    double v = x.get();
    assertThat(v, Matchers.closeTo(63, 5.0));
    // Add another second and we should be ~ 2/3 of the way from the last value to 100
    x.update(utime + 2000, 100);
    // calculate the percentage difference from the new EMA compared to the old EMA based on
    // the difference from the original EMA to 100.
    v = 100 * (x.get() - v) / (100 - v);
    assertThat(v, Matchers.closeTo(63, 5.0));
  }

  @Test
  public void testValueReporting() throws Exception {
    List<List<LatencyReportingLinkedBlockingQueue.EMA>> emas = new LinkedList<>();
    LatencyReportingLinkedBlockingQueue<Integer> q = new LatencyReportingLinkedBlockingQueue<>(10,
        emas::add, 1, 1.0f / 60);

    q.add(1);
    q.offer(2);
    q.offer(3, 1000, TimeUnit.SECONDS);
    q.put(4);
    assertEquals(4, q.size());
    assertEquals(6, q.remainingCapacity());
    q.addAll(Arrays.asList(5, 6, 7));
    assertEquals(1, q.remove().intValue());
    assertEquals(1, emas.size());
    assertTrue(q.remove(2));
    assertEquals(2, emas.size());
    assertEquals(3, Objects.requireNonNull(q.poll()).intValue());
    assertEquals(3, emas.size());
    assertEquals(4, Objects.requireNonNull(q.poll(1000, TimeUnit.SECONDS)).intValue());
    assertEquals(4, emas.size());
    assertEquals(5, q.take().intValue());
    assertEquals(5, emas.size());
    assertNotNull(q.peek());
    assertEquals(6, q.peek().intValue());
    q.drainTo(new LinkedList<>());
    assertEquals(6, emas.size());
    assertNull(q.poll());
    assertNull(q.poll(10, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testReportOnDequeue() throws Exception {
    List<List<LatencyReportingLinkedBlockingQueue.EMA>> emas = new LinkedList<>();
    LatencyReportingLinkedBlockingQueue<Integer> q = new LatencyReportingLinkedBlockingQueue<>(10,
        emas::add, 1, 1.0f / 60);

    q.add(1);
    q.offer(2);
    q.offer(3, 1000, TimeUnit.SECONDS);
    q.put(4);
    assertEquals(4, q.size());
    assertEquals(6, q.remainingCapacity());
    q.addAll(Arrays.asList(5, 6, 7));
    assertEquals(1, q.remove().intValue());
    assertEquals(1, emas.size());
    assertTrue(q.remove(2));
    assertEquals(2, emas.size());
    assertEquals(3, Objects.requireNonNull(q.poll()).intValue());
    assertEquals(3, emas.size());
    assertEquals(4, Objects.requireNonNull(q.poll(1000, TimeUnit.SECONDS)).intValue());
    assertEquals(4, emas.size());
    assertEquals(5, q.take().intValue());
    assertEquals(5, emas.size());
    assertNotNull(q.peek());
    assertEquals(6, q.peek().intValue());
    q.drainTo(new LinkedList<>());
    assertEquals(6, emas.size());
    assertNull(q.poll());
    assertNull(q.poll(10, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testReportFreq() throws Exception {
    int reportFreq = 10;
    List<List<LatencyReportingLinkedBlockingQueue.EMA>> emas = new LinkedList<>();
    LatencyReportingLinkedBlockingQueue<Integer> q = new LatencyReportingLinkedBlockingQueue<>(1000,
        emas::add, reportFreq, 1.0f / 60);
    int multiplier = 2;
    for (int i = 0; i < multiplier * reportFreq; i++) {
      q.add(i);
      assertEquals(i, q.take().intValue());
    }
    assertEquals(multiplier, emas.size());
  }

  @Test
  public void testIteratorRemoval() {
    int reportFreq = 10;
    List<List<LatencyReportingLinkedBlockingQueue.EMA>> emas = new LinkedList<>();
    LatencyReportingLinkedBlockingQueue<Integer> q = new LatencyReportingLinkedBlockingQueue<>(1000,
        emas::add, reportFreq, 1.0f / 60);
    int multiplier = 2;
    for (int i = 0; i < multiplier * reportFreq; i++) {
      q.add(i);
    }

    Iterator<Integer> i = q.iterator();
    try {
      i.remove();
      fail("Should not have been able to remove on the iterator first");
    } catch (IllegalStateException e) {
      // Expected
    }
    int count = 0;
    while (i.hasNext()) {
      assertEquals(count, i.next().intValue());
      i.remove();
      count++;
    }
    assertEquals(multiplier, emas.size());
  }

  @Test
  public void testIterForEachRemaining() {
    int reportFreq = 10;
    List<List<LatencyReportingLinkedBlockingQueue.EMA>> emas = new LinkedList<>();
    LatencyReportingLinkedBlockingQueue<Integer> q = new LatencyReportingLinkedBlockingQueue<>(1000,
        emas::add, reportFreq, 1.0f / 60);
    int multiplier = 2;
    for (int i = 0; i < multiplier * reportFreq; i++) {
      q.add(i);
    }
    Iterator<Integer> i = q.iterator();
    i.forEachRemaining((x) -> { });
    assertEquals(0, emas.size());
    assertEquals(multiplier * reportFreq, q.size());
  }
}
