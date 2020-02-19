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
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class MetricsStoreTest {
  private MetricsStore mMetricStore;

  @Before
  public void before() {
    mMetricStore = new MetricsStore();
    mMetricStore.init();
  }

  @Test
  public void putWorkerMetrics() {
    String workerHost1 = "192_1_1_1";
    List<Metric> metrics1 = Lists.newArrayList(
        Metric.from(MetricKey.WORKER_BYTES_READ_ALLUXIO.getName() + "." + workerHost1,
            10, MetricType.COUNTER),
        Metric.from(MetricKey.WORKER_BYTES_READ_DOMAIN.getName() + "." + workerHost1,
            20, MetricType.COUNTER));
    mMetricStore.putWorkerMetrics(workerHost1, metrics1);

    String workerHost2 = "192_1_1_2";
    List<Metric> metrics2 = Lists.newArrayList(
        Metric.from(MetricKey.WORKER_BYTES_READ_ALLUXIO.getName() + "." + workerHost2,
            1, MetricType.COUNTER));
    mMetricStore.putWorkerMetrics(workerHost2, metrics2);
    assertEquals(11,
        MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_ALLUXIO.getName()).getCount());
  }

  @Test
  public void putClientMetrics() {
    List<Metric> metrics1 = Lists.newArrayList(
        Metric.from(MetricKey.CLIENT_BYTES_READ_LOCAL.getName() + ".192_1_1_1:A",
            10, MetricType.COUNTER),
        Metric.from(MetricKey.CLIENT_BYTES_WRITTEN_LOCAL.getName() + ".192_1_1_1:A",
            20, MetricType.COUNTER));
    mMetricStore.putClientMetrics("192_1_1_1", metrics1);
    List<Metric> metrics2 = Lists.newArrayList(
        Metric.from(MetricKey.CLIENT_BYTES_READ_LOCAL.getName() + ".192_1_1_2:C",
            1, MetricType.COUNTER));
    mMetricStore.putClientMetrics("192_1_1_2", metrics2);
    List<Metric> metrics3 = Lists.newArrayList(
        Metric.from(MetricKey.CLIENT_BYTES_READ_LOCAL.getName() + ".192_1_1_1:B",
            15, MetricType.COUNTER),
        Metric.from(MetricKey.CLIENT_BYTES_WRITTEN_LOCAL.getName() + ".192_1_1_1:B",
            25, MetricType.COUNTER));
    mMetricStore.putClientMetrics("192_1_1_1", metrics3);
    assertEquals(26,
        MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_LOCAL.getName()).getCount());
    assertEquals(45,
        MetricsSystem.counter(MetricKey.CLUSTER_BYTES_WRITTEN_LOCAL.getName()).getCount());
  }
}
