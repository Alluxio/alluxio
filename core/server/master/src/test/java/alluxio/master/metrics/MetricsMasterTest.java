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
import alluxio.master.MasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.SafeModeManager;
import alluxio.master.TestSafeModeManager;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.noop.NoopJournalSystem;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsStore;
import alluxio.metrics.MetricsSystem;
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
  private SafeModeManager mSafeModeManager;
  private MetricsStore mMetricsStore;
  private ManualClock mClock;
  private ExecutorService mExecutorService;

  @Before
  public void before() throws Exception {
    mRegistry = new MasterRegistry();
    mSafeModeManager = new TestSafeModeManager();
    mMetricsStore = new MetricsStore();
    mClock = new ManualClock();
    mExecutorService =
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("TestMetricsMaster-%d", true));
    JournalSystem journalSystem = new NoopJournalSystem();
    mMetricsMaster =
        new DefaultMetricsMaster(new MasterContext(journalSystem, mSafeModeManager, mMetricsStore),
            mClock, ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
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
    mMetricsMaster.addAggregator(new SumInstancesAggregator("worker", "metric1"));
    mMetricsMaster.addAggregator(new SumInstancesAggregator("worker", "metric2"));
    List<Metric> metrics1 = Lists.newArrayList(Metric.from("worker.192_1_1_1.metric1", 10),
        Metric.from("worker.192_1_1_1.metric2", 20));
    mMetricsStore.putWorkerMetrics("192_1_1_1", metrics1);
    List<Metric> metrics2 = Lists.newArrayList(Metric.from("worker.192_1_1_2.metric1", 1),
        Metric.from("worker.192_1_1_2.metric2", 2));
    mMetricsStore.putWorkerMetrics("192_1_1_2", metrics2);
    assertEquals(11L, getGauge("metric1"));
    assertEquals(22L, getGauge("metric2"));
    // override metrics from hostname 192_1_1_2
    List<Metric> metrics3 = Lists.newArrayList(Metric.from("worker.192_1_1_2.metric1", 3));
    mMetricsStore.putWorkerMetrics("192_1_1_2", metrics3);
    assertEquals(13L, getGauge("metric1"));
    assertEquals(20L, getGauge("metric2"));
  }

  private Object getGauge(String name) {
    return MetricsSystem.METRIC_REGISTRY.getGauges().get(MetricsSystem.getClusterMetricName(name))
        .getValue();
  }
}
