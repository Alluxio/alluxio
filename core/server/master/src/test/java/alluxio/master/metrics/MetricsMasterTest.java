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

import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.clock.ManualClock;
import alluxio.grpc.MetricType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.aggregator.SingleTagValueAggregator;
import alluxio.metrics.aggregator.SumInstancesAggregator;
import alluxio.util.CommonUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.executor.ExecutorServiceFactories;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Unit tests for {@link MetricsMaster}.
 */
public class MetricsMasterTest {
  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallyScheduleRule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.MASTER_CLUSTER_METRICS_UPDATER);

  private static final int TIMEOUT_MS = 5 * Constants.SECOND_MS;

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
  public void testAggregator() throws Exception {
    mMetricsMaster.addAggregator(
        new SumInstancesAggregator("metricA", MetricsSystem.InstanceType.WORKER, "metricA"));
    mMetricsMaster.addAggregator(
        new SumInstancesAggregator("metricB", MetricsSystem.InstanceType.WORKER, "metricB"));

    List<Metric> metrics1 = Lists.newArrayList(
        Metric.from("worker.192_1_1_1.metricA", 10, MetricType.GAUGE),
        Metric.from("worker.192_1_1_1.metricB", 20, MetricType.GAUGE));
    mMetricsMaster.workerHeartbeat("192_1_1_1", metrics1);

    List<Metric> metrics2 = Lists.newArrayList(
        Metric.from("worker.192_1_1_2.metricA", 1, MetricType.GAUGE),
        Metric.from("worker.192_1_1_2.metricB", 2, MetricType.GAUGE));
    mMetricsMaster.workerHeartbeat("192_1_1_2", metrics2);
    checkMetricValue("metricA", 11L);
    checkMetricValue("metricB", 22L);

    // override metrics from hostname 192_1_1_2
    List<Metric> metrics3 = Lists.newArrayList(
        Metric.from("worker.192_1_1_2.metricA", 3, MetricType.GAUGE));
    mMetricsMaster.workerHeartbeat("192_1_1_2", metrics3);
    checkMetricValue("metricA", 13L);
    checkMetricValue("metricB", 20L);
  }

  @Test
  public void testMultiValueAggregator() throws Exception {
    mMetricsMaster.addAggregator(
        new SingleTagValueAggregator("metric", MetricsSystem.InstanceType.WORKER, "metric", "tag"));
    List<Metric> metrics1 = Lists.newArrayList(
        Metric.from("worker.192_1_1_1.metric.tag:v1", 10, MetricType.GAUGE),
        Metric.from("worker.192_1_1_1.metric.tag:v2", 20, MetricType.GAUGE));
    mMetricsMaster.workerHeartbeat("192_1_1_1", metrics1);
    List<Metric> metrics2 = Lists.newArrayList(
        Metric.from("worker.192_1_1_2.metric.tag:v1", 1, MetricType.GAUGE),
        Metric.from("worker.192_1_1_2.metric.tag:v2", 2, MetricType.GAUGE));
    mMetricsMaster.workerHeartbeat("192_1_1_2", metrics2);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_CLUSTER_METRICS_UPDATER);
    checkMetricValue("metric", 11L, "tag", "v1");
    checkMetricValue("metric", 22L, "tag", "v2");
  }

  @Test
  public void testClientHeartbeat() throws Exception {
    mMetricsMaster.addAggregator(
        new SumInstancesAggregator("metric1", MetricsSystem.InstanceType.CLIENT, "metric1"));
    mMetricsMaster.addAggregator(
        new SumInstancesAggregator("metric2", MetricsSystem.InstanceType.CLIENT, "metric2"));
    List<Metric> metrics1 = Lists.newArrayList(
        Metric.from("client.192_1_1_1:A.metric1", 10, MetricType.GAUGE),
        Metric.from("client.192_1_1_1:A.metric2", 20, MetricType.GAUGE));
    mMetricsMaster.clientHeartbeat("A", "192.1.1.1", metrics1);
    List<Metric> metrics2 = Lists.newArrayList(
        Metric.from("client.192_1_1_1:B.metric1", 15, MetricType.GAUGE),
        Metric.from("client.192_1_1_1:B.metric2", 25, MetricType.GAUGE));
    mMetricsMaster.clientHeartbeat("B", "192.1.1.1", metrics2);
    List<Metric> metrics3 = Lists.newArrayList(
        Metric.from("client.192_1_1_2:C.metric1", 1, MetricType.GAUGE),
        Metric.from("client.192_1_1_2:C.metric2", 2, MetricType.GAUGE));
    mMetricsMaster.clientHeartbeat("C", "192.1.1.2", metrics3);
    checkMetricValue("metric1", 26L);
    checkMetricValue("metric2", 47L);
  }

  private Object getGauge(String name) {
    return MetricsSystem.METRIC_REGISTRY.getGauges().get(MetricsSystem.getClusterMetricName(name))
        .getValue();
  }

  private Object getGauge(String metricName, String tagName, String tagValue) {
    return MetricsSystem.METRIC_REGISTRY.getGauges()
        .get(MetricsSystem
            .getClusterMetricName(Metric.getMetricNameWithTags(metricName, tagName, tagValue)))
        .getValue();
  }

  private void checkMetricValue(String metricsName, Long value) throws Exception {
    checkMetricValue(metricsName, value, null, null);
  }

  private void checkMetricValue(String metricsName, Long value,
      String tagName, String tagValue) throws Exception {
    Supplier<Boolean> condition;
    if (tagName == null) {
      condition = () -> getGauge(metricsName) == value;
    } else {
      condition = () -> getGauge(metricsName, tagName, tagValue) == value;
    }
    if (!condition.get()) {
      // Wait for the async metrics updater to finish
      CommonUtils.waitFor("metrics processed", condition,
          WaitForOptions.defaults().setTimeoutMs(TIMEOUT_MS));
      assertTrue(condition.get());
    }
  }
}
