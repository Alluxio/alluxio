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

import alluxio.clock.ManualClock;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.aggregator.SingleTagValueAggregator;
import alluxio.metrics.aggregator.SumInstancesAggregator;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Unit tests for {@link MetricsMaster}.
 */
public class MetricsMasterTest {
  private DefaultMetricsMaster mMetricsMaster;
  private MasterRegistry mRegistry;
  private ManualClock mClock;
  private ExecutorService mExecutorService;

  @Before
  public void before() throws Exception {
    mRegistry = new MasterRegistry();
    mClock = new ManualClock();
    mExecutorService =
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestMetricsMaster-%d", true));
    mMetricsMaster = new DefaultMetricsMaster(MasterTestUtils.testMasterContext(), mClock,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
    mRegistry.add(MetricsMaster.class, mMetricsMaster);
    mRegistry.start(true);
  }

  /**
   * Stops the master after a test ran.
   */
  @After
  public void after() throws Exception {
    mRegistry.stop();
  }

  @Test
  public void testAggregator() {
    mMetricsMaster.addAggregator(
        new SumInstancesAggregator("metricA", MetricsSystem.InstanceType.WORKER, "metricA"));
    mMetricsMaster.addAggregator(
        new SumInstancesAggregator("metricB", MetricsSystem.InstanceType.WORKER, "metricB"));
    List<Metric> metrics1 = Lists.newArrayList(Metric.from("worker.192_1_1_1.metricA", 10),
        Metric.from("worker.192_1_1_1.metricB", 20));
    mMetricsMaster.workerHeartbeat("192_1_1_1", metrics1);
    List<Metric> metrics2 = Lists.newArrayList(Metric.from("worker.192_1_1_2.metricA", 1),
        Metric.from("worker.192_1_1_2.metricB", 2));
    mMetricsMaster.workerHeartbeat("192_1_1_2", metrics2);
    assertEquals(11L, getGauge("metricA"));
    assertEquals(22L, getGauge("metricB"));
    // override metrics from hostname 192_1_1_2
    List<Metric> metrics3 = Lists.newArrayList(Metric.from("worker.192_1_1_2.metricA", 3));
    mMetricsMaster.workerHeartbeat("192_1_1_2", metrics3);
    assertEquals(13L, getGauge("metricA"));
    assertEquals(20L, getGauge("metricB"));
  }

  @Test
  public void testMultiValueAggregator() {
    mMetricsMaster.addAggregator(
        new SingleTagValueAggregator("metric", MetricsSystem.InstanceType.WORKER, "metric", "tag"));
    List<Metric> metrics1 = Lists.newArrayList(Metric.from("worker.192_1_1_1.metric.tag:v1", 10),
        Metric.from("worker.192_1_1_1.metric.tag:v2", 20));
    mMetricsMaster.workerHeartbeat("192_1_1_1", metrics1);
    List<Metric> metrics2 = Lists.newArrayList(Metric.from("worker.192_1_1_2.metric.tag:v1", 1),
        Metric.from("worker.192_1_1_2.metric.tag:v2", 2));
    mMetricsMaster.workerHeartbeat("192_1_1_2", metrics2);
    assertEquals(11L, getGauge("metric.v1"));
    assertEquals(22L, getGauge("metric.v2"));
  }

  @Test
  public void testClientHeartbeat() {
    mMetricsMaster.addAggregator(
        new SumInstancesAggregator("metric1", MetricsSystem.InstanceType.CLIENT, "metric1"));
    mMetricsMaster.addAggregator(
        new SumInstancesAggregator("metric2", MetricsSystem.InstanceType.CLIENT, "metric2"));
    List<Metric> metrics1 = Lists.newArrayList(Metric.from("client.192_1_1_1:A.metric1", 10),
        Metric.from("client.192_1_1_1:A.metric2", 20));
    mMetricsMaster.clientHeartbeat("A", "192.1.1.1", metrics1);
    List<Metric> metrics2 = Lists.newArrayList(Metric.from("client.192_1_1_1:B.metric1", 15),
        Metric.from("client.192_1_1_1:B.metric2", 25));
    mMetricsMaster.clientHeartbeat("B", "192.1.1.1", metrics2);
    List<Metric> metrics3 = Lists.newArrayList(Metric.from("client.192_1_1_2:C.metric1", 1),
        Metric.from("client.192_1_1_2:C.metric2", 2));
    mMetricsMaster.clientHeartbeat("C", "192.1.1.2", metrics3);
    assertEquals(26L, getGauge("metric1"));
    assertEquals(47L, getGauge("metric2"));
  }

  private Object getGauge(String name) {
    return MetricsSystem.METRIC_REGISTRY.getGauges().get(MetricsSystem.getClusterMetricName(name))
        .getValue();
  }
}
