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

package tachyon.metrics.sink;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import tachyon.metrics.MetricsSystem;

/**
 * A sink which publishes metric values to a Graphite server.
 */
@NotThreadSafe
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

  /**
   * Gets the polling period.
   *
   * @return the polling period set by properties. If it is not set, a default value 10 is
   *         returned.
   */
  public int getPollPeriod() {
    String period = mProperties.getProperty(GRAPHITE_KEY_PERIOD);
    return period != null ? Integer.parseInt(period) : GRAPHITE_DEFAULT_PERIOD;
  }

  /**
   * Gets the polling time unit.
   *
   * @return the polling time unit set by properties, If it is not set, a default value SECONDS is
   *         returned.
   */
  public TimeUnit getPollUnit() {
    String unit = mProperties.getProperty(GRAPHITE_KEY_UNIT);
    if (unit == null) {
      unit = GRAPHITE_DEFAULT_UNIT;
    }
    return TimeUnit.valueOf(unit.toUpperCase());
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
}
