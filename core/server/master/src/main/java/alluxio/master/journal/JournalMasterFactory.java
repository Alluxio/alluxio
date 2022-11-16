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

package alluxio.master.journal;

import alluxio.Constants;
import alluxio.grpc.JournalDomain;
import alluxio.master.Master;
import alluxio.master.MasterContext;
import alluxio.master.MasterFactory;
import alluxio.master.MasterRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link JournalMaster} instance.
 */
@ThreadSafe
public class JournalMasterFactory implements MasterFactory<MasterContext> {
  private static final Logger LOG = LoggerFactory.getLogger(JournalMasterFactory.class);

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public String getName() {
    return Constants.JOURNAL_MASTER_NAME;
  }

  @Override
  public Master create(MasterRegistry registry, MasterContext context) {
    LOG.info("Creating {} ", JournalMaster.class.getName());
    JournalMaster journalMaster = new DefaultJournalMaster(JournalDomain.MASTER, context);
    registry.add(JournalMaster.class, journalMaster);
    return journalMaster;
  }
}
