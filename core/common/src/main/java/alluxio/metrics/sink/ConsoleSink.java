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

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A sink which outputs metric values to the console.
 */
@ThreadSafe
public final class ConsoleSink implements Sink {
  private static final int CONSOLE_DEFAULT_PERIOD = 10;
  private static final String CONSOLE_DEFAULT_UNIT = "SECONDS";

  private static final String CONSOLE_KEY_PERIOD = "period";
  private static final String CONSOLE_KEY_UNIT = "unit";

  private ConsoleReporter mReporter;
  private Properties mProperties;

  /**
   * Creates a new {@link ConsoleSink} with a {@link Properties} and {@link MetricRegistry}.
   *
   * @param properties the properties which may contain polling period and unit properties
   * @param registry the metric registry to register
   */
  public ConsoleSink(Properties properties, MetricRegistry registry) {
    mProperties = properties;
    mReporter =
        ConsoleReporter.forRegistry(registry).convertDurationsTo(TimeUnit.MILLISECONDS)
            .convertRatesTo(TimeUnit.SECONDS).build();
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
    String period = mProperties.getProperty(CONSOLE_KEY_PERIOD);
    return period != null ? Integer.parseInt(period) : CONSOLE_DEFAULT_PERIOD;
  }

  /**
   * Gets the polling time unit.
   *
   * @return the polling time unit set by properties, If it is not set, a default value SECONDS is
   *         returned.
   */
  private TimeUnit getPollUnit() {
    String unit = mProperties.getProperty(CONSOLE_KEY_UNIT);
    if (unit == null) {
      unit = CONSOLE_DEFAULT_UNIT;
    }
    return TimeUnit.valueOf(unit.toUpperCase());
  }
}
