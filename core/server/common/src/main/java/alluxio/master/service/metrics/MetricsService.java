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

package alluxio.master.service.metrics;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.service.SimpleService;
import alluxio.metrics.MetricsSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple service that manages the behavior of the metrics system.
 */
public abstract class MetricsService implements SimpleService {
  protected static final Logger LOG = LoggerFactory.getLogger(MetricsService.class);

  protected void startMetricsSystem() {
    LOG.info("Start metric sinks.");
    MetricsSystem.startSinks(Configuration.getString(PropertyKey.METRICS_CONF_FILE));
  }

  protected void stopMetricsSystem() {
    LOG.info("Stop metric sinks.");
    MetricsSystem.stopSinks();
  }

  /**
   * Factory that provides a simple service that manages the behavior of the metrics system.
   */
  public static class Factory {
    /**
     * @return a simple service that manages the behavior of the metrics system
     */
    public static MetricsService create() {
      if (Configuration.getBoolean(PropertyKey.STANDBY_MASTER_METRICS_SINK_ENABLED)) {
        return new AlwaysOnMetricsService();
      }
      return new PrimaryOnlyMetricsService();
    }
  }
}
