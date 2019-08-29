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

import alluxio.grpc.MetricType;
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
    List<Metric> metrics1 = Lists.newArrayList(
        Metric.from("worker.192_1_1_1.metric1", 10, MetricType.GAUGE),
        Metric.from("worker.192_1_1_1.metric2", 20, MetricType.GAUGE));
    mMetricStore.putWorkerMetrics("192_1_1_1", metrics1);
    List<Metric> metrics2 = Lists.newArrayList(
        Metric.from("worker.192_1_1_2.metric1", 1, MetricType.GAUGE));
    mMetricStore.putWorkerMetrics("192_1_1_2", metrics2);
    assertEquals(Sets.newHashSet(
            Metric.from("worker.192_1_1_1.metric1", 10, MetricType.GAUGE),
            Metric.from("worker.192_1_1_2.metric1", 1, MetricType.GAUGE)),
        mMetricStore.getMetricsByInstanceTypeAndName(MetricsSystem.InstanceType.WORKER, "metric1"));
  }

  @Test
  public void putClientMetrics() {
    List<Metric> metrics1 = Lists.newArrayList(
        Metric.from("client.192_1_1_1:A.metric1", 10, MetricType.GAUGE),
        Metric.from("client.192_1_1_1:A.metric2", 20, MetricType.GAUGE));
    mMetricStore.putClientMetrics("192_1_1_1", "A", metrics1);
    List<Metric> metrics2 = Lists.newArrayList(
        Metric.from("client.192_1_1_2:C.metric1", 1, MetricType.GAUGE));
    mMetricStore.putClientMetrics("192_1_1_2", "C", metrics2);
    List<Metric> metrics3 = Lists.newArrayList(
        Metric.from("client.192_1_1_1:B.metric1", 15, MetricType.GAUGE),
        Metric.from("client.192_1_1_1:B.metric2", 25, MetricType.GAUGE));
    mMetricStore.putClientMetrics("192_1_1_1", "B", metrics3);
    assertEquals(Sets.newHashSet(
        Metric.from("client.192_1_1_1:A.metric1", 10, MetricType.GAUGE),
        Metric.from("client.192_1_1_2:C.metric1", 1, MetricType.GAUGE),
        Metric.from("client.192_1_1_1:B.metric1", 15, MetricType.GAUGE)),
        mMetricStore.getMetricsByInstanceTypeAndName(MetricsSystem.InstanceType.CLIENT, "metric1"));
  }

  @Test
  public void putTaggedMetrics() {
    String name = "test";
    Metric metric1 =
        new Metric(MetricsSystem.InstanceType.WORKER, "host", MetricType.COUNTER, name, 1.0);
    metric1.addTag("Tag", "1");
    Metric metric2 =
        new Metric(MetricsSystem.InstanceType.WORKER, "host", MetricType.COUNTER, name, 2.0);
    metric2.addTag("Tag", "2");
    mMetricStore.putWorkerMetrics("host", Lists.newArrayList(metric1, metric2));
    assertEquals(mMetricStore.getMetricsByInstanceTypeAndName(MetricsSystem.InstanceType.WORKER,
        name), Sets.newHashSet(metric1, metric2));
  }
}
