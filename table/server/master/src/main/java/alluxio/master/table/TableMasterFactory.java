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

package alluxio.master.table;

import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.client.job.JobMasterClient;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterFactory;
import alluxio.master.MasterRegistry;
import alluxio.worker.job.JobMasterClientContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link TableMaster} instance.
 */
@ThreadSafe
public final class TableMasterFactory implements MasterFactory<CoreMasterContext> {
  private static final Logger LOG = LoggerFactory.getLogger(TableMasterFactory.class);

  /**
   * Constructs a new {@link TableMasterFactory}.
   */
  public TableMasterFactory() {}

  @Override
  public boolean isEnabled() {
    return ServerConfiguration.getBoolean(PropertyKey.TABLE_ENABLED);
  }

  @Override
  public String getName() {
    return Constants.TABLE_MASTER_NAME;
  }

  @Override
  public TableMaster create(MasterRegistry registry, CoreMasterContext context) {
    LOG.info("Creating {} ", TableMaster.class.getName());

    JobMasterClient jobMasterClient = JobMasterClient.Factory.create(JobMasterClientContext
        .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
    TableMaster master = new DefaultTableMaster(context, jobMasterClient);
    registry.add(TableMaster.class, master);
    return master;
  }
}
