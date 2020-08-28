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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

/**
 * Unit tests for {@link MetricsConfig}.
 */
public final class MetricsConfigTest {
  private Properties mMetricsProps;

  /**
   * Sets up the properties for the configuration of the metrics before a test runs.
   */
  @Before
  public final void before() {
    mMetricsProps = new Properties();
    mMetricsProps.setProperty("sink.console.class", "alluxio.metrics.sink.ConsoleSink");
    mMetricsProps.setProperty("sink.console.period", "15");
    mMetricsProps.setProperty("*.sink.console.unit", "minutes");
    mMetricsProps.setProperty("sink.jmx.class", "alluxio.metrics.sink.JmxSink");
  }

  /**
   * Tests that the {@link MetricsConfig#MetricsConfig(Properties)} constructor sets the properties
   * correctly.
   */
  @Test
  public void setProperties() {
    MetricsConfig config = new MetricsConfig(mMetricsProps);

    Properties masterProp = config.getProperties();
    assertEquals(4, masterProp.size());
    assertEquals("alluxio.metrics.sink.ConsoleSink",
        masterProp.getProperty("sink.console.class"));
    assertEquals("15", masterProp.getProperty("sink.console.period"));
    assertEquals("minutes", masterProp.getProperty("sink.console.unit"));
    assertEquals("alluxio.metrics.sink.JmxSink", masterProp.getProperty("sink.jmx.class"));
  }

  /**
   * Tests the {@link MetricsConfig#subProperties(Properties, String)} method.
   */
  @Test
  public void subProperties() {
    MetricsConfig config = new MetricsConfig(mMetricsProps);

    Properties properties = config.getProperties();

    Map<String, Properties> sinkProps =
        MetricsConfig.subProperties(properties, MetricsSystem.SINK_REGEX);
    assertEquals(2, sinkProps.size());
    assertTrue(sinkProps.containsKey("console"));
    assertTrue(sinkProps.containsKey("jmx"));

    Properties consoleProp = sinkProps.get("console");
    assertEquals(3, consoleProp.size());
    assertEquals("alluxio.metrics.sink.ConsoleSink", consoleProp.getProperty("class"));

    Properties jmxProp = sinkProps.get("jmx");
    assertEquals(1, jmxProp.size());
    assertEquals("alluxio.metrics.sink.JmxSink", jmxProp.getProperty("class"));
  }
}
