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
import com.sun.management.OperatingSystemMXBean;
import com.sun.management.UnixOperatingSystemMXBean;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * A set of counters for the os metric.
 */
public class OperationSystemCounterSet implements MetricSet {

  private OperatingSystemMXBean mOsmxb = (OperatingSystemMXBean)
          ManagementFactory.getOperatingSystemMXBean();
  private UnixOperatingSystemMXBean mUnixb = (UnixOperatingSystemMXBean)
          ManagementFactory.getOperatingSystemMXBean();

  @Override
  public Map<String, Metric> getMetrics() {
    final Map<String, Metric> gauges = new HashMap<>();
    gauges.put("os.freePhysicalMemory", (Gauge<Long>) mOsmxb::getFreePhysicalMemorySize);
    gauges.put("os.totalPhysicalMemory", (Gauge<Long>) mOsmxb::getTotalPhysicalMemorySize);
    gauges.put("os.cpuLoad", (Gauge<Double>) mOsmxb::getSystemCpuLoad);
    gauges.put("os.maxFileCount", (Gauge<Long>) mUnixb::getMaxFileDescriptorCount);
    gauges.put("os.openFileCount", (Gauge<Long>) mUnixb::getOpenFileDescriptorCount);
    return gauges;
  }
}
