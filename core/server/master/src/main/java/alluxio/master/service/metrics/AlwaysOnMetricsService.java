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

/**
 * Created by {@link MetricsService.Factory}.
 * This service differs from {@link PrimaryOnlyMetricsService} because it reports metrics
 * after being started and stops reporting metrics after being stopped.
 */
class AlwaysOnMetricsService extends MetricsService {
  @Override
  public void start() {
    LOG.info("Starting {}", this.getClass().getSimpleName());
    startMetricsSystem();
  }

  @Override
  public void promote() {}

  @Override
  public void demote() {}

  @Override
  public void stop() {
    LOG.info("Stopping {}", this.getClass().getSimpleName());
    stopMetricsSystem();
  }
}
