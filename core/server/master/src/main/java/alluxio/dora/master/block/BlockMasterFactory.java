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

package alluxio.dora.master.block;

import alluxio.dora.Constants;
import alluxio.dora.master.CoreMasterContext;
import alluxio.dora.master.metrics.MetricsMaster;
import alluxio.dora.master.MasterFactory;
import alluxio.dora.master.MasterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link BlockMaster} instance.
 */
@ThreadSafe
public final class BlockMasterFactory implements MasterFactory<CoreMasterContext> {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMasterFactory.class);

  /**
   * Constructs a new {@link BlockMasterFactory}.
   */
  public BlockMasterFactory() {}

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public String getName() {
    return Constants.BLOCK_MASTER_NAME;
  }

  @Override
  public BlockMaster create(MasterRegistry registry, CoreMasterContext context) {
    LOG.info("Creating {} ", BlockMaster.class.getName());
    MetricsMaster metricsMaster = registry.get(MetricsMaster.class);
    BlockMaster master = new DefaultBlockMaster(metricsMaster, context);
    registry.add(BlockMaster.class, master);
    return master;
  }
}
