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

import alluxio.AlluxioURI;
import alluxio.clock.SystemClock;
import alluxio.grpc.MetricType;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricInfo;
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
    MetricsSystem.resetAllMetrics();
    mMetricStore = new MetricsStore(new SystemClock());
    mMetricStore.initMetricKeys();
  }

  @Test
  public void putWorkerMetrics() {
    String workerHost1 = "192_1_1_1";
    List<Metric> metrics1 = Lists.newArrayList(
        Metric.from(MetricKey.WORKER_BYTES_READ_REMOTE.getName() + "." + workerHost1,
            10, MetricType.COUNTER),
        Metric.from(MetricKey.WORKER_BYTES_READ_DOMAIN.getName() + "." + workerHost1,
            20, MetricType.COUNTER));
    mMetricStore.putWorkerMetrics(workerHost1, metrics1);

    String workerHost2 = "192_1_1_2";
    List<Metric> metrics2 = Lists.newArrayList(
        Metric.from(MetricKey.WORKER_BYTES_READ_REMOTE.getName() + "." + workerHost2,
            1, MetricType.COUNTER));
    mMetricStore.putWorkerMetrics(workerHost2, metrics2);
    assertEquals(11,
        MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_REMOTE.getName()).getCount());
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

  @Test
  public void putWorkerUfsMetrics() {
    String readBytes = MetricKey.WORKER_BYTES_READ_UFS.getName();
    String writtenBytes = MetricKey.WORKER_BYTES_WRITTEN_UFS.getName();

    String ufsName1 = MetricsSystem.escape(new AlluxioURI("/my/local/folder"));
    String readBytesUfs1 = Metric.getMetricNameWithTags(readBytes, MetricInfo.TAG_UFS, ufsName1);
    String writtenBytesUfs1 = Metric
        .getMetricNameWithTags(writtenBytes, MetricInfo.TAG_UFS, ufsName1);

    String ufsName2 = MetricsSystem.escape(new AlluxioURI("s3://my/s3/bucket/"));
    String readBytesUfs2 = Metric.getMetricNameWithTags(readBytes, MetricInfo.TAG_UFS, ufsName2);
    String writtenBytesUfs2 = Metric
        .getMetricNameWithTags(writtenBytes, MetricInfo.TAG_UFS, ufsName2);

    String host1 = "192_1_1_1";
    List<Metric> metrics1 = Lists.newArrayList(
        Metric.from(readBytesUfs1 + "." + host1, 10, MetricType.COUNTER),
        Metric.from(writtenBytesUfs1 + "." + host1, 20, MetricType.COUNTER),
        Metric.from(writtenBytesUfs2 + "." + host1, 7, MetricType.COUNTER));
    mMetricStore.putWorkerMetrics(host1, metrics1);

    String host2 = "192_1_1_2";
    List<Metric> metrics2 = Lists.newArrayList(
        Metric.from(readBytesUfs1 + "." + host2, 5, MetricType.COUNTER),
        Metric.from(writtenBytesUfs1 + "." + host2, 12, MetricType.COUNTER),
        Metric.from(readBytesUfs2 + "." + host2, 33, MetricType.COUNTER));
    mMetricStore.putWorkerMetrics(host2, metrics2);

    assertEquals(15, MetricsSystem.counter(
        Metric.getMetricNameWithTags(MetricKey.CLUSTER_BYTES_READ_UFS.getName(),
            MetricInfo.TAG_UFS, ufsName1)).getCount());
    assertEquals(33, MetricsSystem.counter(
        Metric.getMetricNameWithTags(MetricKey.CLUSTER_BYTES_READ_UFS.getName(),
            MetricInfo.TAG_UFS, ufsName2)).getCount());
    assertEquals(48,
        MetricsSystem.counter(MetricKey.CLUSTER_BYTES_READ_UFS_ALL.getName()).getCount());

    assertEquals(32, MetricsSystem.counter(
        Metric.getMetricNameWithTags(MetricKey.CLUSTER_BYTES_WRITTEN_UFS.getName(),
            MetricInfo.TAG_UFS, ufsName1)).getCount());
    assertEquals(7, MetricsSystem.counter(
        Metric.getMetricNameWithTags(MetricKey.CLUSTER_BYTES_WRITTEN_UFS.getName(),
            MetricInfo.TAG_UFS, ufsName2)).getCount());
    assertEquals(39,
        MetricsSystem.counter(MetricKey.CLUSTER_BYTES_WRITTEN_UFS_ALL.getName()).getCount());
  }

  @Test
  public void clearAndGetClearTime() throws Exception {
    long clearTime = mMetricStore.getLastClearTime();
    String workerHost1 = "192_1_1_1";
    List<Metric> metrics1 = Lists.newArrayList(
        Metric.from(MetricKey.WORKER_BYTES_WRITTEN_REMOTE.getName() + "." + workerHost1,
            10, MetricType.COUNTER),
        Metric.from(MetricKey.WORKER_BYTES_WRITTEN_DOMAIN.getName() + "." + workerHost1,
            20, MetricType.COUNTER));
    mMetricStore.putWorkerMetrics(workerHost1, metrics1);

    List<Metric> metrics2 = Lists.newArrayList(
        Metric.from(MetricKey.CLIENT_BYTES_WRITTEN_LOCAL.getName() + ".192_1_1_2:C",
            1, MetricType.COUNTER));
    mMetricStore.putClientMetrics("192_1_1_2", metrics2);

    assertEquals(10,
        MetricsSystem.counter(MetricKey.CLUSTER_BYTES_WRITTEN_REMOTE.getName()).getCount());
    assertEquals(1,
        MetricsSystem.counter(MetricKey.CLUSTER_BYTES_WRITTEN_LOCAL.getName()).getCount());
    // Make sure the test does not execute too fast
    // so that new clear time will be definitely different from the previous one
    Thread.sleep(10);
    mMetricStore.clear();
    assertEquals(0,
        MetricsSystem.counter(MetricKey.CLUSTER_BYTES_WRITTEN_REMOTE.getName()).getCount());
    assertEquals(0,
        MetricsSystem.counter(MetricKey.CLUSTER_BYTES_WRITTEN_LOCAL.getName()).getCount());
    long newClearTime = mMetricStore.getLastClearTime();
    assertTrue(newClearTime > clearTime);
  }
}
