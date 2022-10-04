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

package alluxio.master.cross.cluster;

import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterFactory;
import alluxio.master.MasterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory to create a {@link CrossClusterMaster} instance.
 */
public class CrossClusterMasterFactory  implements MasterFactory<CoreMasterContext> {
  private static final Logger LOG = LoggerFactory.getLogger(CrossClusterMasterFactory.class);

  /**
   * Constructs a new {@link CrossClusterMasterFactory}.
   */
  public CrossClusterMasterFactory() {}

  @Override
  public boolean isEnabled() {
    return Configuration.getBoolean(PropertyKey.MASTER_CROSS_CLUSTER_ENABLE)
        && !Configuration.getBoolean(PropertyKey.CROSS_CLUSTER_MASTER_STANDALONE);
  }

  @Override
  public String getName() {
    return Constants.CROSS_CLUSTER_MASTER_NAME;
  }

  @Override
  public CrossClusterMaster create(MasterRegistry registry, CoreMasterContext context) {
    LOG.info("Creating {} ", CrossClusterMaster.class.getName());
    CrossClusterMaster master = new DefaultCrossClusterMaster(context);
    registry.add(CrossClusterMaster.class, master);
    return master;
  }
}
