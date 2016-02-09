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

package alluxio.metrics.sink;

import alluxio.metrics.MetricsSystem;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A sink which outputs metric values to the console.
 */
@NotThreadSafe
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

  /**
   * Gets the polling period.
   *
   * @return the polling period set by properties. If it is not set, a default value 10 is
   *         returned.
   */
  public int getPollPeriod() {
    String period = mProperties.getProperty(CONSOLE_KEY_PERIOD);
    return period != null ? Integer.parseInt(period) : CONSOLE_DEFAULT_PERIOD;
  }

  /**
   * Gets the polling time unit.
   *
   * @return the polling time unit set by properties, If it is not set, a default value SECONDS is
   *         returned.
   */
  public TimeUnit getPollUnit() {
    String unit = mProperties.getProperty(CONSOLE_KEY_UNIT);
    if (unit == null) {
      unit = CONSOLE_DEFAULT_UNIT;
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
