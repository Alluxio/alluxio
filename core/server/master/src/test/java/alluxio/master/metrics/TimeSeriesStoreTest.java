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
import static org.junit.Assert.assertNotNull;

import alluxio.metrics.TimeSeries;
import alluxio.util.CommonUtils;

import org.junit.Assert;
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
    assertEquals(metric1, store.getTimeSeries().get(0).getName());
    assertEquals(1, store.getTimeSeries().get(0).getDataPoints().size());
    CommonUtils.sleepMs(10); // To prevent the two records from being placed in the same ms.
    store.record(metric1, value2);
    assertEquals(2, store.getTimeSeries().get(0).getDataPoints().size());
  }

  @Test
  public void recordMultipleTimeSeries() {
    TimeSeriesStore store = new TimeSeriesStore();
    String metric1 = "test_metric_1";
    String metric2 = "test_metric_2";
    double value1 = 10;
    double value2 = 20;
    store.record(metric1, value1);
    store.record(metric2, value2);
    TimeSeries ts1 = null;
    TimeSeries ts2 = null;
    for (TimeSeries ts : store.getTimeSeries()) {
      if (ts.getName().equals(metric1)) {
        ts1 = ts;
      } else if (ts.getName().equals(metric2)) {
        ts2 = ts;
      } else {
        Assert.fail("Invalid Timeseries " + ts.getName());
      }
    }
    assertNotNull(ts1);
    assertNotNull(ts2);
    assertEquals(value1, ts1.getDataPoints().get(0).getValue(), 0);
    assertEquals(value2, ts2.getDataPoints().get(0).getValue(), 0);
  }

  @Test
  public void orderedTimeSeries() {
    TimeSeriesStore store = new TimeSeriesStore();
    String metric1 = "test_metric";
    double value1 = 10;
    double value2 = 20;
    store.record(metric1, value1);
    CommonUtils.sleepMs(10); // To prevent the two records from being placed in the same ms.
    store.record(metric1, value2);
    assertEquals(value1, store.getTimeSeries().get(0).getDataPoints().get(0).getValue(), 0);
    assertEquals(value2, store.getTimeSeries().get(0).getDataPoints().get(1).getValue(), 0);
  }
}
