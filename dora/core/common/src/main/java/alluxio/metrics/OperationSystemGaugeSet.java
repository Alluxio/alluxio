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

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.sun.management.OperatingSystemMXBean;
import com.sun.management.UnixOperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A set of counters for the os metric.
 */
public class OperationSystemGaugeSet implements MetricSet {

  private OperatingSystemMXBean mOsmxb;
  private UnixOperatingSystemMXBean mUnixb;

  @Override
  public Map<String, Metric> getMetrics() {
    final Map<String, Metric> gauges = new HashMap<>();
    try {
      mOsmxb = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
      mUnixb = (UnixOperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    } catch (Throwable e) {
      return gauges;
    }
    gauges.put("os.freePhysicalMemory", new CachedGauge(10, TimeUnit.MINUTES) {
      @Override
      protected Long loadValue() {
        return mOsmxb.getFreePhysicalMemorySize();
      }
    });
    gauges.put("os.totalPhysicalMemory", new CachedGauge<Long>(10, TimeUnit.MINUTES) {
      @Override
      protected Long loadValue() {
        return mOsmxb.getTotalPhysicalMemorySize();
      }
    });
    gauges.put("os.cpuLoad", new CachedGauge<Double>(10, TimeUnit.MINUTES) {
      @Override
      protected Double loadValue() {
        return mOsmxb.getSystemCpuLoad();
      }
    });
    gauges.put("os.maxFileCount", new CachedGauge<Long>(10, TimeUnit.MINUTES) {
      @Override
      protected Long loadValue() {
        return mUnixb.getMaxFileDescriptorCount();
      }
    });
    gauges.put("os.openFileCount", new CachedGauge<Long>(10, TimeUnit.MINUTES) {
      @Override
      protected Long loadValue() {
        return mUnixb.getOpenFileDescriptorCount();
      }
    });
    return gauges;
  }
}
