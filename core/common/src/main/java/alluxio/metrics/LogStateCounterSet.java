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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

import java.util.HashMap;
import java.util.Map;

/**
 * A set of counters for the log metric.
 */
public class LogStateCounterSet implements MetricSet {
  @Override
  public Map<String, Metric> getMetrics() {
    final Map<String, Metric> gauges = new HashMap<>();
    gauges.put("log.info.count", (Gauge<Long>) EventCounter::getInfo);
    gauges.put("log.warn.count", (Gauge<Long>) EventCounter::getWarn);
    gauges.put("log.error.count", (Gauge<Long>) EventCounter::getError);
    gauges.put("log.fatal.count", (Gauge<Long>) EventCounter::getFatal);
    return gauges;
  }
}
