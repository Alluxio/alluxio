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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

/**
 * Unit tests for {@link MetricsConfig}.
 */
public class MetricsConfigTest {
  private Properties mMetricsProps;

  /**
   * Sets up the properties for the configuration of the metrics before a test runs.
   */
  @Before
  public final void before() {
    mMetricsProps = new Properties();
    mMetricsProps.setProperty("*.sink.console.class", "alluxio.metrics.sink.ConsoleSink");
    mMetricsProps.setProperty("*.sink.console.period", "15");
    mMetricsProps.setProperty("*.source.jvm.class", "alluxio.metrics.source.JvmSource");
    mMetricsProps.setProperty("master.sink.console.period", "20");
    mMetricsProps.setProperty("master.sink.console.unit", "minutes");
    mMetricsProps.setProperty("master.sink.jmx.class", "alluxio.metrics.sink.JmxSink");
  }

  /**
   * Tests that the {@link MetricsConfig#MetricsConfig(Properties)} constructor sets the properties
   * correctly.
   */
  @Test
  public void setPropertiesTest() {
    MetricsConfig config = new MetricsConfig(mMetricsProps);

    Properties masterProp = config.getInstanceProperties("master");
    Assert.assertEquals(7, masterProp.size());
    Assert.assertEquals("alluxio.metrics.sink.ConsoleSink",
        masterProp.getProperty("sink.console.class"));
    Assert.assertEquals("20", masterProp.getProperty("sink.console.period"));
    Assert.assertEquals("minutes", masterProp.getProperty("sink.console.unit"));
    Assert.assertEquals("alluxio.metrics.source.JvmSource",
        masterProp.getProperty("source.jvm.class"));
    Assert.assertEquals("alluxio.metrics.sink.JmxSink", masterProp.getProperty("sink.jmx.class"));
    Assert.assertEquals("alluxio.metrics.sink.MetricsServlet",
        masterProp.getProperty("sink.servlet.class"));
    Assert.assertEquals("/metrics/json", masterProp.getProperty("sink.servlet.path"));

    Properties workerProp = config.getInstanceProperties("worker");
    Assert.assertEquals(5, workerProp.size());
    Assert.assertEquals("alluxio.metrics.sink.ConsoleSink",
        workerProp.getProperty("sink.console.class"));
    Assert.assertEquals("15", workerProp.getProperty("sink.console.period"));
    Assert.assertEquals("alluxio.metrics.source.JvmSource",
        workerProp.getProperty("source.jvm.class"));
    Assert.assertEquals("alluxio.metrics.sink.MetricsServlet",
        workerProp.getProperty("sink.servlet.class"));
    Assert.assertEquals("/metrics/json", workerProp.getProperty("sink.servlet.path"));
  }

  /**
   * Tests the {@link MetricsConfig#subProperties(Properties, String)} method.
   */
  @Test
  public void subPropertiesTest() {
    MetricsConfig config = new MetricsConfig(mMetricsProps);

    Map<String, Properties> propertyCategories = config.getPropertyCategories();
    Assert.assertEquals(2, propertyCategories.size());

    Properties masterProp = config.getInstanceProperties("master");
    Map<String, Properties> sourceProps =
        config.subProperties(masterProp, MetricsSystem.SOURCE_REGEX);
    Assert.assertEquals(1, sourceProps.size());
    Assert.assertEquals("alluxio.metrics.source.JvmSource",
        sourceProps.get("jvm").getProperty("class"));

    Map<String, Properties> sinkProps = config.subProperties(masterProp, MetricsSystem.SINK_REGEX);
    Assert.assertEquals(3, sinkProps.size());
    Assert.assertTrue(sinkProps.containsKey("servlet"));
    Assert.assertTrue(sinkProps.containsKey("console"));
    Assert.assertTrue(sinkProps.containsKey("jmx"));

    Properties servletProp = sinkProps.get("servlet");
    Assert.assertEquals(2, servletProp.size());
    Assert.assertEquals("alluxio.metrics.sink.MetricsServlet", servletProp.getProperty("class"));

    Properties consoleProp = sinkProps.get("console");
    Assert.assertEquals(3, consoleProp.size());
    Assert.assertEquals("alluxio.metrics.sink.ConsoleSink", consoleProp.getProperty("class"));

    Properties jmxProp = sinkProps.get("jmx");
    Assert.assertEquals(1, jmxProp.size());
    Assert.assertEquals("alluxio.metrics.sink.JmxSink", jmxProp.getProperty("class"));
  }
}
