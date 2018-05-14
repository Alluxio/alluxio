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

package alluxio.master.meta;

import alluxio.Constants;
import alluxio.master.MasterContext;
import alluxio.master.MasterFactory;
import alluxio.master.MasterRegistry;
import alluxio.master.SafeModeManager;
import alluxio.master.block.BlockMaster;
import alluxio.master.journal.JournalSystem;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link MetaMaster} instance.
 */
@ThreadSafe
public final class MetaMasterFactory implements MasterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MetaMasterFactory.class);

  /**
   * Constructs a new {@link MetaMasterFactory}.
   */
  public MetaMasterFactory() {}

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public String getName() {
    return Constants.META_MASTER_NAME;
  }

  @Override
  public MetaMaster create(MasterRegistry registry, JournalSystem journalFactory,
      SafeModeManager safeModeManager) {
    Preconditions.checkArgument(journalFactory != null, "journal");
    LOG.info("Creating {} ", MetaMaster.class.getName());
    MetaMaster master = new DefaultMetaMaster(registry.get(BlockMaster.class),
        new MasterContext(journalFactory, safeModeManager));
    registry.add(MetaMaster.class, master);
    return master;
  }
}
