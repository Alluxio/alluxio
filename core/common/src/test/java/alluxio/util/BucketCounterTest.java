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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.util.Arrays;

/**
 * Unit tests for {@link BucketCounterTest}.
 */
public final class BucketCounterTest {
  @Test
  public void insertandremove() {
    BucketCounter counter = new BucketCounter(Arrays.asList(4L, 8L, 12L));
    counter.insert(3L);
    assertEquals(1, counter.getCounters().get(4L).get());
    counter.insert(5L);
    assertEquals(1, counter.getCounters().get(4L).get());
    assertEquals(1, counter.getCounters().get(8L).get());
    counter.insert(6L);
    assertEquals(1, counter.getCounters().get(4L).get());
    assertEquals(2, counter.getCounters().get(8L).get());
    counter.remove(6L);
    assertEquals(1, counter.getCounters().get(4L).get());
    assertEquals(1, counter.getCounters().get(8L).get());
  }
}
