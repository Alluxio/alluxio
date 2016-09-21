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

package alluxio.metrics.sink;

import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A sink which publishes metric values to a Graphite server.
 */
@ThreadSafe
public class GraphiteSink implements Sink {
  private static final int GRAPHITE_DEFAULT_PERIOD = 10;
  private static final String GRAPHITE_DEFAULT_UNIT = "SECONDS";
  private static final String GRAPHITE_DEFAULT_PREFIX = "";

  private static final String GRAPHITE_KEY_HOST = "host";
  private static final String GRAPHITE_KEY_PORT = "port";
  private static final String GRAPHITE_KEY_PERIOD = "period";
  private static final String GRAPHITE_KEY_UNIT = "unit";
  private static final String GRAPHITE_KEY_PREFIX = "prefix";

  private GraphiteReporter mReporter;
  private Properties mProperties;

  /**
   * Creates a new {@link GraphiteSink} with a {@link Properties} and {@link MetricRegistry}.
   *
   * @param properties the properties which may contain polling period and unit properties
   * @param registry the metric registry to register
   * @throws IllegalArgumentException if the {@code host} or {@code port} property is missing
   */
  public GraphiteSink(Properties properties, MetricRegistry registry)
      throws IllegalArgumentException {
    mProperties = properties;
    String host = properties.getProperty(GRAPHITE_KEY_HOST);
    String port = properties.getProperty(GRAPHITE_KEY_PORT);
    if (host == null || port == null) {
      throw new IllegalArgumentException("Graphite sink requires 'host' and 'port' properties");
    }
    String prefix = properties.getProperty(GRAPHITE_KEY_PREFIX);
    if (prefix == null) {
      prefix = GRAPHITE_DEFAULT_PREFIX;
    }
    Graphite graphite = new Graphite(host, Integer.parseInt(port));
    mReporter =
        GraphiteReporter.forRegistry(registry).convertDurationsTo(TimeUnit.MILLISECONDS)
            .convertRatesTo(TimeUnit.SECONDS).prefixedWith(prefix).build(graphite);
    MetricsSystem.checkMinimalPollingPeriod(getPollUnit(), getPollPeriod());
  }

  @Override
  public void start() {
    mReporter.start(getPollPeriod(), getPollUnit());
  }

  @Override
  public void stop() {
    mReporter.stop();
  }

  @Override
  public void report() {
    mReporter.report();
  }

  /**
   * Gets the polling period.
   *
   * @return the polling period set by properties. If it is not set, a default value 10 is
   *         returned.
   */
  private int getPollPeriod() {
    String period = mProperties.getProperty(GRAPHITE_KEY_PERIOD);
    return period != null ? Integer.parseInt(period) : GRAPHITE_DEFAULT_PERIOD;
  }

  /**
   * Gets the polling time unit.
   *
   * @return the polling time unit set by properties, If it is not set, a default value SECONDS is
   *         returned.
   */
  private TimeUnit getPollUnit() {
    String unit = mProperties.getProperty(GRAPHITE_KEY_UNIT);
    if (unit == null) {
      unit = GRAPHITE_DEFAULT_UNIT;
    }
    return TimeUnit.valueOf(unit.toUpperCase());
  }
}
