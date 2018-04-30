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
import alluxio.master.MasterContext;
import alluxio.master.MasterFactory;
import alluxio.master.MasterRegistry;
import alluxio.master.SafeModeManager;
import alluxio.master.journal.JournalSystem;
import alluxio.metrics.MetricsStore;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link MetricsMaster} instance.
 */
@ThreadSafe
public final class MetricsMasterFactory implements MasterFactory {
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
  public MetricsMaster create(MasterRegistry registry, JournalSystem journalSystem,
      SafeModeManager safeModeManager, MetricsStore metricsStore) {
    Preconditions.checkNotNull(journalSystem, "journal");
    Preconditions.checkNotNull(metricsStore, "metrics store");
    LOG.info("Creating {} ", MetricsMaster.class.getName());
    MetricsMaster master =
        new DefaultMetricsMaster(new MasterContext(journalSystem, safeModeManager, metricsStore));
    registry.add(MetricsMaster.class, master);
    return master;
  }

}
