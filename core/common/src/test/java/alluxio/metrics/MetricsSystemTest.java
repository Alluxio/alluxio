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

import alluxio.metrics.source.Source;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * Unit tests for {@link MetricsSystem}.
 */
public class MetricsSystemTest {
  private MetricsConfig mMetricsConfig;

  /**
   * Sets up the properties for the configuration of the metrics before a test runs.
   */
  @Before
  public final void before() {
    Properties metricsProps = new Properties();
    metricsProps.setProperty("*.sink.console.class", "alluxio.metrics.sink.ConsoleSink");
    metricsProps.setProperty("*.sink.console.period", "15");
    metricsProps.setProperty("*.source.jvm.class", "alluxio.metrics.source.JvmSource");
    metricsProps.setProperty("dummy.sink.console.period", "20");
    metricsProps.setProperty("dummy.sink.console.unit", "minutes");
    metricsProps.setProperty("dummy.sink.jmx.class", "alluxio.metrics.sink.JmxSink");
    mMetricsConfig = new MetricsConfig(metricsProps);
  }

  /**
   * A dummy source to test MetricsSystem.
   */
  public final class DummySource implements Source {
    private MetricRegistry mMetricRegistry;
    private final Counter mCounter;

    /**
     * Creates a DummySource instance.
     */
    public DummySource() {
      mMetricRegistry = new MetricRegistry();
      mCounter = mMetricRegistry.counter("dummyCounter");
      mCounter.inc();
    }

    @Override
    public String getName() {
      return "dummy";
    }

    @Override
    public MetricRegistry getMetricRegistry() {
      return mMetricRegistry;
    }
  }

  /**
   * Tests the metrics for a master and a worker.
   */
  @Test
  public void metricsSystemTest() {
    MetricsSystem masterMetricsSystem = new MetricsSystem("dummy", mMetricsConfig);
    masterMetricsSystem.start();

    Assert.assertEquals(2, masterMetricsSystem.getSinks().size());
    Assert.assertEquals(1, masterMetricsSystem.getSources().size());
    masterMetricsSystem.registerSource(new DummySource());
    Assert.assertEquals(2, masterMetricsSystem.getSources().size());
    masterMetricsSystem.stop();
  }
}
