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
import com.codahale.metrics.ganglia.GangliaReporter;

import info.ganglia.gmetric4j.gmetric.GMetric;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A sink which publishes metric values to a Ganglia.
 */
@ThreadSafe
public class GangliaSink implements Sink {
  private static final String GANGLIA_DEFAULT_PORT = "8649";
  private static final int GANGLIA_DEFAULT_PERIOD = 10;
  private static final String GANGLIA_DEFAULT_UNIT = "SECONDS";
  private static final String GANGLIA_DEFAULT_TTL = "1";

  private static final String GANGLIA_KEY_HOST = "host";
  private static final String GANGLIA_KEY_PORT = "port";
  private static final String GANGLIA_KEY_PERIOD = "period";
  private static final String GANGLIA_KEY_UNIT = "unit";
  private static final String GANGLIA_KEY_TTL = "ttl";

  private GangliaReporter mReporter;
  private Properties mProperties;

  /**
   * Creates a new {@link GangliaSink} with a {@link Properties} and {@link MetricRegistry}.
   *
   * @param properties the properties which may contain polling period and unit properties
   * @param registry the metric registry to register
   * @throws IllegalArgumentException if the {@code host} or {@code port} property is missing
   * @throws IOException if the ganglia metric initialize fail
   */
  public GangliaSink(Properties properties, MetricRegistry registry)
          throws IllegalArgumentException, IOException {
    mProperties = properties;
    String host = properties.getProperty(GANGLIA_KEY_HOST);
    if (host == null) {
      throw new IllegalArgumentException("Ganglia sink requires 'host' properties");
    }
    String port = properties.getProperty(GANGLIA_KEY_PORT);
    if (port == null) {
      port = GANGLIA_DEFAULT_PORT;
    }
    String ttl = properties.getProperty(GANGLIA_KEY_TTL);
    if (ttl == null) {
      ttl = GANGLIA_DEFAULT_TTL;
    }
    final GMetric ganglia = new GMetric(host,
            Integer.parseInt(port),
            GMetric.UDPAddressingMode.MULTICAST, Integer.parseInt(ttl));

    mReporter = GangliaReporter.forRegistry(registry)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build(ganglia);
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
    String period = mProperties.getProperty(GANGLIA_KEY_PERIOD);
    return period != null ? Integer.parseInt(period) : GANGLIA_DEFAULT_PERIOD;
  }

  /**
   * Gets the polling time unit.
   *
   * @return the polling time unit set by properties, If it is not set, a default value SECONDS is
   *         returned.
   */
  private TimeUnit getPollUnit() {
    String unit = mProperties.getProperty(GANGLIA_KEY_UNIT);
    if (unit == null) {
      unit = GANGLIA_DEFAULT_UNIT;
    }
    return TimeUnit.valueOf(unit.toUpperCase());
  }
}
