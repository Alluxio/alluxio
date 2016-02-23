/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.metrics;

import alluxio.Configuration;
import alluxio.master.MasterSource;
import alluxio.worker.WorkerSource;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * Unit tests for {@link MetricsSystem}.
 */
public class MetricsSystemTest {
  private MetricsConfig mMetricsConfig;
  private Configuration mConfiguration;

  /**
   * Sets up the properties for the configuration of the metrics before a test runs.
   */
  @Before
  public final void before() {
    mConfiguration = new Configuration();
    Properties metricsProps = new Properties();
    metricsProps.setProperty("*.sink.console.class", "alluxio.metrics.sink.ConsoleSink");
    metricsProps.setProperty("*.sink.console.period", "15");
    metricsProps.setProperty("*.source.jvm.class", "alluxio.metrics.source.JvmSource");
    metricsProps.setProperty("master.sink.console.period", "20");
    metricsProps.setProperty("master.sink.console.unit", "minutes");
    metricsProps.setProperty("master.sink.jmx.class", "alluxio.metrics.sink.JmxSink");
    mMetricsConfig = new MetricsConfig(metricsProps);
  }

  /**
   * Tests the metrics for a master and a worker.
   */
  @Test
  public void metricsSystemTest() {
    MetricsSystem masterMetricsSystem = new MetricsSystem("master", mMetricsConfig, mConfiguration);
    masterMetricsSystem.start();

    Assert.assertNotNull(masterMetricsSystem.getServletHandler());
    Assert.assertEquals(2, masterMetricsSystem.getSinks().size());
    Assert.assertEquals(1, masterMetricsSystem.getSources().size());
    masterMetricsSystem.registerSource(new MasterSource());
    Assert.assertEquals(2, masterMetricsSystem.getSources().size());
    masterMetricsSystem.stop();

    MetricsSystem workerMetricsSystem = new MetricsSystem("worker", mMetricsConfig, mConfiguration);
    workerMetricsSystem.start();

    Assert.assertNotNull(workerMetricsSystem.getServletHandler());
    Assert.assertEquals(1, workerMetricsSystem.getSinks().size());
    Assert.assertEquals(1, workerMetricsSystem.getSources().size());
    workerMetricsSystem.registerSource(new WorkerSource());
    Assert.assertEquals(2, workerMetricsSystem.getSources().size());
    workerMetricsSystem.stop();
  }
}
