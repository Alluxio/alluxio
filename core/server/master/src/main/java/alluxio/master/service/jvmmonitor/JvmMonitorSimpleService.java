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

package alluxio.master.service.jvmmonitor;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.service.NoopSimpleService;
import alluxio.master.service.SimpleService;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.util.JvmPauseMonitor;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Simple service to manage the behavior of the {@link alluxio.util.JvmPauseMonitor}.
 */
public class JvmMonitorSimpleService implements SimpleService {
  @Nullable @GuardedBy("this")
  private JvmPauseMonitor mJvmPauseMonitor = null;

  private JvmMonitorSimpleService() {}

  @Override
  public synchronized void start() {
    mJvmPauseMonitor = new JvmPauseMonitor(
        Configuration.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS),
        Configuration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS),
        Configuration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS));
    mJvmPauseMonitor.start();
    MetricsSystem.registerGaugeIfAbsent(
        MetricsSystem.getMetricName(MetricKey.TOTAL_EXTRA_TIME.getName()),
        mJvmPauseMonitor::getTotalExtraTime);
    MetricsSystem.registerGaugeIfAbsent(
        MetricsSystem.getMetricName(MetricKey.INFO_TIME_EXCEEDED.getName()),
        mJvmPauseMonitor::getInfoTimeExceeded);
    MetricsSystem.registerGaugeIfAbsent(
        MetricsSystem.getMetricName(MetricKey.WARN_TIME_EXCEEDED.getName()),
        mJvmPauseMonitor::getWarnTimeExceeded);
  }

  @Override
  public synchronized void promote() {}

  @Override
  public synchronized void demote() {}

  @Override
  public synchronized void stop() {
    if (mJvmPauseMonitor != null) {
      mJvmPauseMonitor.stop();
    }
  }

  /**
   * Factory that returns the appropriate {@link JvmMonitorSimpleService} based on configuration.
   */
  public static class Factory {
    /**
     * @return a simple service that manages the behavior of the {@link JvmPauseMonitor}
     */
    public static SimpleService create() {
      if (!Configuration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED)) {
        return new NoopSimpleService();
      }
      return new JvmMonitorSimpleService();
    }
  }
}
