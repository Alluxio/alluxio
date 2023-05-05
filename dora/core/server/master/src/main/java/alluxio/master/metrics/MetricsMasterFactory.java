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

package alluxio.master.metrics;

import alluxio.Constants;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterFactory;
import alluxio.master.MasterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link MetricsMaster} instance.
 */
@ThreadSafe
public final class MetricsMasterFactory implements MasterFactory<CoreMasterContext> {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsMasterFactory.class);

  /**
   * Constructs a new {@link MetricsMasterFactory}.
   */
  public MetricsMasterFactory() {}

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public String getName() {
    return Constants.METRICS_MASTER_NAME;
  }

  @Override
  public MetricsMaster create(MasterRegistry registry, CoreMasterContext context) {
    LOG.info("Creating {} ", MetricsMaster.class.getName());
    MetricsMaster master = new DefaultMetricsMaster(context);
    registry.add(MetricsMaster.class, master);
    return master;
  }
}
