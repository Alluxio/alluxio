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

import alluxio.AlluxioURI;

import com.codahale.metrics.Counter;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * Unit tests for {@link MetricsSystem}.
 */
public final class MetricsSystemTest {
  private MetricsConfig mMetricsConfig;
  private static Counter sCounter =
      MetricsSystem.METRIC_REGISTRY.counter(MetricsSystem.getMetricName("counter"));

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

    assertEquals(2, MetricsSystem.getNumSinks());

    // Make sure it doesn't crash.
    sCounter.inc();
    MetricsSystem.stopSinks();
  }

  /**
   * Tests the "/" "." and "%" special characters can be escaped.
   */
  @Test
  public void testEscape() {
    AlluxioURI localUri1 = new AlluxioURI("/foo/alluxio/underFSStorage");
    String localUriEscaped1 = MetricsSystem.escape(localUri1);
    assertEquals("%2Ffoo%2Falluxio%2FunderFSStorage",
            localUriEscaped1);

    AlluxioURI localUri2 = new AlluxioURI("/.alluxio.wololo/alluxio/underFSStorage");
    String localUriEscaped2 = MetricsSystem.escape(localUri2);
    assertEquals("%2F%2Ealluxio%2Ewololo%2Falluxio%2FunderFSStorage",
            localUriEscaped2);

    AlluxioURI localUri3 = new AlluxioURI("/%25alluxio%20user%2Ffoo%2Ebar/alluxio/underFSStorage");
    String localUriEscaped3 = MetricsSystem.escape(localUri3);
    assertEquals("%2F%2525alluxio%2520user%252Ffoo%252Ebar%2Falluxio%2FunderFSStorage",
            localUriEscaped3);

    AlluxioURI localUri4 = new AlluxioURI("s3a://test/Tasks+Export+%282017–11–05+06%3A10+PM%2Ecsv");
    String localUriEscaped4 = MetricsSystem.escape(localUri4);
    assertEquals("s3a:%2F%2Ftest%2FTasks+Export+%25282017–11–05+06%253A10+PM%252Ecsv",
            localUriEscaped4);
  }

  /**
   * Tests the escaped strings can be safely unescaped.
   */
  @Test
  public void testUnescape() {
    AlluxioURI localUri1 = new AlluxioURI("/foo/alluxio/underFSStorage");
    String localUriEscaped1 = MetricsSystem.escape(localUri1);
    String localUriUnescaped1 = MetricsSystem.unescape(localUriEscaped1);
    assertEquals(localUri1.toString(), localUriUnescaped1);

    AlluxioURI localUri2 = new AlluxioURI("/.alluxio.wololo/alluxio/underFSStorage");
    String localUriEscaped2 = MetricsSystem.escape(localUri2);
    String localUriUnescaped2 = MetricsSystem.unescape(localUriEscaped2);
    assertEquals(localUri2.toString(), localUriUnescaped2);

    AlluxioURI localUri3 = new AlluxioURI("/%25alluxio%20user%2Ffoo%2Ebar/alluxio/underFSStorage");
    String localUriEscaped3 = MetricsSystem.escape(localUri3);
    String localUriUnescaped3 = MetricsSystem.unescape(localUriEscaped3);
    assertEquals(localUri3.toString(), localUriUnescaped3);

    AlluxioURI localUri4 = new AlluxioURI("s3a://test/Tasks+Export+%282017–11–05+06%3A10+PM%2Ecsv");
    String localUriEscaped4 = MetricsSystem.escape(localUri4);
    String localUriUnescaped4 = MetricsSystem.unescape(localUriEscaped4);
    assertEquals(localUri4.toString(), localUriUnescaped4);
  }
}
