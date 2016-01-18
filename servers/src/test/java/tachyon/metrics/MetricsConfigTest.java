/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.metrics;

import java.util.Map;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
    mMetricsProps.setProperty("*.sink.console.class", "tachyon.metrics.sink.ConsoleSink");
    mMetricsProps.setProperty("*.sink.console.period", "15");
    mMetricsProps.setProperty("*.source.jvm.class", "tachyon.metrics.source.JvmSource");
    mMetricsProps.setProperty("master.sink.console.period", "20");
    mMetricsProps.setProperty("master.sink.console.unit", "minutes");
    mMetricsProps.setProperty("master.sink.jmx.class", "tachyon.metrics.sink.JmxSink");
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
    Assert.assertEquals("tachyon.metrics.sink.ConsoleSink",
        masterProp.getProperty("sink.console.class"));
    Assert.assertEquals("20", masterProp.getProperty("sink.console.period"));
    Assert.assertEquals("minutes", masterProp.getProperty("sink.console.unit"));
    Assert.assertEquals("tachyon.metrics.source.JvmSource",
        masterProp.getProperty("source.jvm.class"));
    Assert.assertEquals("tachyon.metrics.sink.JmxSink", masterProp.getProperty("sink.jmx.class"));
    Assert.assertEquals("tachyon.metrics.sink.MetricsServlet",
        masterProp.getProperty("sink.servlet.class"));
    Assert.assertEquals("/metrics/json", masterProp.getProperty("sink.servlet.path"));

    Properties workerProp = config.getInstanceProperties("worker");
    Assert.assertEquals(5, workerProp.size());
    Assert.assertEquals("tachyon.metrics.sink.ConsoleSink",
        workerProp.getProperty("sink.console.class"));
    Assert.assertEquals("15", workerProp.getProperty("sink.console.period"));
    Assert.assertEquals("tachyon.metrics.source.JvmSource",
        workerProp.getProperty("source.jvm.class"));
    Assert.assertEquals("tachyon.metrics.sink.MetricsServlet",
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
    Assert.assertEquals("tachyon.metrics.source.JvmSource",
        sourceProps.get("jvm").getProperty("class"));

    Map<String, Properties> sinkProps = config.subProperties(masterProp, MetricsSystem.SINK_REGEX);
    Assert.assertEquals(3, sinkProps.size());
    Assert.assertTrue(sinkProps.containsKey("servlet"));
    Assert.assertTrue(sinkProps.containsKey("console"));
    Assert.assertTrue(sinkProps.containsKey("jmx"));

    Properties servletProp = sinkProps.get("servlet");
    Assert.assertEquals(2, servletProp.size());
    Assert.assertEquals("tachyon.metrics.sink.MetricsServlet", servletProp.getProperty("class"));

    Properties consoleProp = sinkProps.get("console");
    Assert.assertEquals(3, consoleProp.size());
    Assert.assertEquals("tachyon.metrics.sink.ConsoleSink", consoleProp.getProperty("class"));

    Properties jmxProp = sinkProps.get("jmx");
    Assert.assertEquals(1, jmxProp.size());
    Assert.assertEquals("tachyon.metrics.sink.JmxSink", jmxProp.getProperty("class"));
  }
}
