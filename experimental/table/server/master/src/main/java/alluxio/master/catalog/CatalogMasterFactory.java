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

package alluxio.master.catalog;

import alluxio.experimental.Constants;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterFactory;
import alluxio.master.MasterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link CatalogMaster} instance.
 */
@ThreadSafe
public final class CatalogMasterFactory implements MasterFactory<CoreMasterContext> {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogMasterFactory.class);

  /**
   * Constructs a new {@link CatalogMasterFactory}.
   */
  public CatalogMasterFactory() {}

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public String getName() {
    return Constants.CATALOG_MASTER_NAME;
  }

  @Override
  public CatalogMaster create(MasterRegistry registry, CoreMasterContext context) {
    LOG.info("Creating {} ", CatalogMaster.class.getName());
    CatalogMaster master = new DefaultCatalogMaster(context);
    registry.add(CatalogMaster.class, master);
    return master;
  }
}
