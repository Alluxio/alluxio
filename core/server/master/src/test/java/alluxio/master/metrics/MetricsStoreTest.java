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

import alluxio.metrics.Metric;
import alluxio.metrics.MetricsSystem;

import com.google.common.collect.Lists;
import jersey.repackaged.com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class MetricsStoreTest {
  private MetricsStore mMetricStore;

  @Before
  public void before() {
    mMetricStore = new MetricsStore();
  }

  @Test
  public void putWorkerMetrics() {
    List<Metric> metrics1 = Lists.newArrayList(Metric.from("worker.192_1_1_1.metric1", 10),
        Metric.from("worker.192_1_1_1.metric2", 20));
    mMetricStore.putWorkerMetrics("192_1_1_1", metrics1);
    List<Metric> metrics2 = Lists.newArrayList(Metric.from("worker.192_1_1_2.metric1", 1));
    mMetricStore.putWorkerMetrics("192_1_1_2", metrics2);
    assertEquals(
        Sets.newHashSet(Metric.from("worker.192_1_1_1.metric1", 10),
            Metric.from("worker.192_1_1_2.metric1", 1)),
        mMetricStore.getMetricsByInstanceTypeAndName(MetricsSystem.InstanceType.WORKER, "metric1"));
  }

  @Test
  public void putClientMetrics() {
    List<Metric> metrics1 = Lists.newArrayList(Metric.from("client.192_1_1_1:A.metric1", 10),
        Metric.from("client.192_1_1_1:A.metric2", 20));
    mMetricStore.putClientMetrics("192_1_1_1", "A", metrics1);
    List<Metric> metrics2 = Lists.newArrayList(Metric.from("client.192_1_1_2:C.metric1", 1));
    mMetricStore.putClientMetrics("192_1_1_2", "C", metrics2);
    List<Metric> metrics3 = Lists.newArrayList(Metric.from("client.192_1_1_1:B.metric1", 15),
        Metric.from("client.192_1_1_1:B.metric2", 25));
    mMetricStore.putClientMetrics("192_1_1_1", "B", metrics3);
    assertEquals(
        Sets.newHashSet(Metric.from("client.192_1_1_1:A.metric1", 10),
            Metric.from("client.192_1_1_2:C.metric1", 1),
            Metric.from("client.192_1_1_1:B.metric1", 15)),
        mMetricStore.getMetricsByInstanceTypeAndName(MetricsSystem.InstanceType.CLIENT, "metric1"));
  }
}
