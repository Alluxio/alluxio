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

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A sink which outputs metric values to the metrics logger.
 */
@ThreadSafe
public class Slf4jSink implements Sink {
  private static final int SLF4J_DEFAULT_PERIOD = 10;
  private static final String SLF4J_DEFAULT_UNIT = "SECONDS";
  private static final String SLF4J_KEY_PERIOD = "period";
  private static final String SLF4J_KEY_UNIT = "unit";
  private static final String SLF4J_KEY_FILTER_CLASS = "filter-class";

  private final Slf4jReporter mReporter;
  private final Properties mProperties;

  /**
   * Creates a new {@link Slf4jSink} with a {@link Properties} and {@link MetricRegistry}.
   *
   * @param properties the properties
   * @param registry   the metric registry to register
   */
  public Slf4jSink(Properties properties, MetricRegistry registry) throws Exception {
    mProperties = properties;
    MetricFilter filter = getMetricFilter();
    mReporter = Slf4jReporter.forRegistry(registry).filter(filter).build();
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
   * returned.
   */
  private int getPollPeriod() {
    String period = mProperties.getProperty(SLF4J_KEY_PERIOD);
    return period != null ? Integer.parseInt(period) : SLF4J_DEFAULT_PERIOD;
  }

  /**
   * Gets the polling time unit.
   *
   * @return the polling time unit set by properties, If it is not set, a default value SECONDS is
   * returned.
   */
  private TimeUnit getPollUnit() {
    String unit = mProperties.getProperty(SLF4J_KEY_UNIT);
    if (unit == null) {
      unit = SLF4J_DEFAULT_UNIT;
    }
    return TimeUnit.valueOf(unit.toUpperCase());
  }

  /**
   * Gets the metrics filter.
   *
   * @return the metrics filter set by properties, If it is not set, it will return null
   * @throws Exception if any error happens
   */
  private MetricFilter getMetricFilter() throws Exception {
    String filterClass = mProperties.getProperty(SLF4J_KEY_FILTER_CLASS);
    if (filterClass == null) {
      return MetricFilter.ALL;
    }

    return (MetricFilter) Class.forName(filterClass)
        .getConstructor(Properties.class).newInstance(mProperties);
  }
}
