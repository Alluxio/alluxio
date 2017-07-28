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

package alluxio.metrics;

import com.codahale.metrics.Counter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * Unit tests for {@link MetricsSystem}.
 */
public final class MetricsSystemTest {
  private MetricsConfig mMetricsConfig;
  private static Counter sCounter =
      MetricsSystem.METRIC_REGISTRY.counter(MetricsSystem.getMasterMetricName("counter"));

  /**
   * Sets up the properties for the configuration of the metrics before a test runs.
   */
  @Before
  public final void before() {
    Properties metricsProps = new Properties();
    metricsProps.setProperty("sink.console.class", "alluxio.metrics.sink.ConsoleSink");
    metricsProps.setProperty("sink.console.period", "20");
    metricsProps.setProperty("sink.console.period", "20");
    metricsProps.setProperty("sink.console.unit", "minutes");
    metricsProps.setProperty("sink.jmx.class", "alluxio.metrics.sink.JmxSink");
    mMetricsConfig = new MetricsConfig(metricsProps);
  }

  /**
   * Tests the metrics for a master and a worker.
   */
  @Test
  public void metricsSystem() {
    MetricsSystem.startSinksFromConfig(mMetricsConfig);

    Assert.assertEquals(2, MetricsSystem.getNumSinks());

    // Make sure it doesn't crash.
    sCounter.inc();
    MetricsSystem.stopSinks();
  }
}
