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

package alluxio.master.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.util.CommonUtils;

import org.junit.Test;

/**
 * Tests for {@link TimeSeriesStore}.
 */
public class TimeSeriesStoreTest {
  @Test
  public void recordTimeSeries() {
    TimeSeriesStore store = new TimeSeriesStore();
    String metric1 = "test_metric";
    long value1 = 10;
    long value2 = 20;
    store.record(metric1, value1);
    assertTrue(store.getTimeSeries().containsKey(metric1));
    assertEquals(1, store.getTimeSeries().get(metric1).size());
    CommonUtils.sleepMs(10); // To prevent the two records from being placed in the same ms.
    store.record(metric1, value2);
    assertEquals(2, store.getTimeSeries().get(metric1).size());
  }

  @Test
  public void recordMultipleTimeSeries() {
    TimeSeriesStore store = new TimeSeriesStore();
    String metric1 = "test_metric_1";
    String metric2 = "test_metric_2";
    long value1 = 10;
    long value2 = 20;
    store.record(metric1, value1);
    store.record(metric2, value2);
    assertTrue(store.getTimeSeries().containsKey(metric1));
    assertTrue(store.getTimeSeries().containsKey(metric2));
    assertTrue(store.getTimeSeries().get(metric1).containsValue(value1));
    assertTrue(store.getTimeSeries().get(metric2).containsValue(value2));
  }

  @Test
  public void orderedTimeSeries() {
    TimeSeriesStore store = new TimeSeriesStore();
    String metric1 = "test_metric";
    Long value1 = 10L;
    Long value2 = 20L;
    store.record(metric1, value1);
    CommonUtils.sleepMs(10); // To prevent the two records from being placed in the same ms.
    store.record(metric1, value2);
    assertEquals(value1, store.getTimeSeries().get(metric1).firstEntry().getValue());
    assertEquals(value2, store.getTimeSeries().get(metric1).lastEntry().getValue());
  }
}
