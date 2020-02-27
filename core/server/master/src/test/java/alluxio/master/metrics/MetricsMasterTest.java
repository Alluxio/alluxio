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
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.clock.ManualClock;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.aggregator.SingleTagValueAggregator;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

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
    MetricsSystem.clearAllMetrics();
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
  public void testThroughputGauge() throws Exception {
    String counterName = "Master.counter";
    String throughputName = "Cluster.counterThroughput";
    mMetricsMaster.registerThroughputGauge("Master.counter", "Cluster.counterThroughput");
    Metric metric = MetricsSystem.getMetricValue(throughputName);
    assertNotNull(metric);
    assertEquals(0, (long) metric.getValue());

    Counter masterCounter = MetricsSystem.counter(counterName);
    masterCounter.inc(100);
    Gauge gauge = MetricsSystem.METRIC_REGISTRY.getGauges().get(throughputName);
    assertNotNull(gauge);
    assertEquals(100, (long) gauge.getValue());

    masterCounter.inc(200);
    mClock.addTimeMs(2 * Constants.MINUTE_MS);
    System.out.println(gauge.getValue());
    assertEquals(150, (long) gauge.getValue());
  }

  @Test
  public void testRegisteredAggregator() throws Exception {
    String ufsOp = MetricInfo.UfsOps.values().clone()[0].toString();
    String masterUfsOpName = MetricsSystem.getMasterMetricName(ufsOp);
    String ufs = MetricsSystem.escape(new AlluxioURI("hdfs://name:9000/alluxio_storage"));

    String counterName = Metric.getMetricNameWithTags(masterUfsOpName,
        MetricInfo.TAG_UFS, ufs, MetricInfo.TAG_UFS_TYPE, "hdfs");
    MetricsSystem.counter(counterName).inc(2333);

    HeartbeatScheduler.execute(HeartbeatContext.MASTER_CLUSTER_METRICS_UPDATER);

    String clusterMetricName = Metric.getMetricNameWithTags(
        MetricInfo.UFS_OP_PREFIX + ufsOp, MetricInfo.TAG_UFS, ufs);
    Metric clusterMetric = MetricsSystem.getMetricValue(clusterMetricName);
    assertNotNull(clusterMetric);
    assertEquals(2333, (long) clusterMetric.getValue());

    String timerNameOne = Metric.getMetricNameWithTags(masterUfsOpName,
        MetricInfo.TAG_UFS, ufs, MetricInfo.TAG_USER, "userA");
    MetricsSystem.timer(timerNameOne).time().close();
    String timerNameTwo = Metric.getMetricNameWithTags(masterUfsOpName,
        MetricInfo.TAG_UFS, ufs, MetricInfo.TAG_USER, "userB");
    MetricsSystem.timer(timerNameTwo).time().close();

    HeartbeatScheduler.execute(HeartbeatContext.MASTER_CLUSTER_METRICS_UPDATER);

    clusterMetric = MetricsSystem.getMetricValue(clusterMetricName);
    assertNotNull(clusterMetric);
    assertEquals(2335, (long) clusterMetric.getValue());
  }

  @Test
  public void testMultiValueAggregator() throws Exception {
    // Add user tag
    String masterMetricName = "Master.TestMetric";
    String clusterMetricName = "Cluster.TestMetric";
    mMetricsMaster.addAggregator(
        new SingleTagValueAggregator(clusterMetricName, masterMetricName, MetricInfo.TAG_UFS));

    String ufsOne = MetricsSystem.escape(new AlluxioURI("/path/to/ufs"));
    String counterNameOne = Metric
        .getMetricNameWithTags(masterMetricName, MetricInfo.TAG_UFS, ufsOne);
    Counter counterOne = MetricsSystem.counter(counterNameOne);
    counterOne.inc(10);
    String clusterMetricNameOne = Metric
        .getMetricNameWithTags(clusterMetricName, MetricInfo.TAG_UFS, ufsOne);

    assertTrue(MetricsSystem.getMetricValue(clusterMetricNameOne) == null);

    HeartbeatScheduler.execute(HeartbeatContext.MASTER_CLUSTER_METRICS_UPDATER);

    Metric clusterMetricOne = MetricsSystem.getMetricValue(clusterMetricNameOne);
    assertNotNull(clusterMetricOne);
    assertEquals(10, (long) clusterMetricOne.getValue());

    counterOne.inc(7);

    String ufsTwo = MetricsSystem.escape(new AlluxioURI("s3://alluxio-test-metrics/metrics"));
    String counterNameTwo = Metric
        .getMetricNameWithTags(masterMetricName, MetricInfo.TAG_UFS, ufsTwo);
    Counter counterTwo = MetricsSystem.counter(counterNameTwo);
    counterTwo.inc(50);

    HeartbeatScheduler.execute(HeartbeatContext.MASTER_CLUSTER_METRICS_UPDATER);

    Metric clusterMetricOne2 = MetricsSystem.getMetricValue(clusterMetricNameOne);
    assertNotNull(clusterMetricOne2);
    assertEquals(17, (long) clusterMetricOne2.getValue());

    Metric clusterMetricTwo = MetricsSystem.getMetricValue(Metric
        .getMetricNameWithTags(clusterMetricName, MetricInfo.TAG_UFS, ufsTwo));
    assertNotNull(clusterMetricTwo);
    assertEquals(50, (long) clusterMetricTwo.getValue());
  }
}
