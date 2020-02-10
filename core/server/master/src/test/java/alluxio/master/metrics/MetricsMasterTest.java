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
import alluxio.grpc.MetricType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.aggregator.SingleTagValueAggregator;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Unit tests for {@link MetricsMaster}.
 */
public class MetricsMasterTest {
  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallyScheduleRule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.MASTER_CLUSTER_METRICS_UPDATER);

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
  public void testMultiValueAggregator() throws Exception {
    mMetricsMaster.addAggregator(
        new SingleTagValueAggregator("metric", MetricsSystem.InstanceType.WORKER, "metric", "tag"));
    List<Metric> metrics1 = Lists.newArrayList(
        Metric.from("worker.metric.tag:v1.192_1_1_1", 10, MetricType.GAUGE),
        Metric.from("worker.metric.tag:v2.192_1_1_1", 20, MetricType.GAUGE));
    mMetricsMaster.workerHeartbeat("192_1_1_1", metrics1);
    List<Metric> metrics2 = Lists.newArrayList(
        Metric.from("worker.metric.tag:v1.192_1_1_2", 1, MetricType.GAUGE),
        Metric.from("worker.metric.tag:v2.192_1_1_2", 2, MetricType.GAUGE));
    mMetricsMaster.workerHeartbeat("192_1_1_2", metrics2);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_CLUSTER_METRICS_UPDATER);
    assertEquals(11L, getGauge("metric", "tag", "v1"));
    assertEquals(22L, getGauge("metric", "tag", "v2"));
  }

  private Object getGauge(String metricName, String tagName, String tagValue) {
    return MetricsSystem.METRIC_REGISTRY.getGauges()
        .get(Metric.getMetricNameWithTags(metricName, tagName, tagValue))
        .getValue();
  }
}
