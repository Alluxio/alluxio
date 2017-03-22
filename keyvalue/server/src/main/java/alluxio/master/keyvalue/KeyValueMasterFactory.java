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

package alluxio.master.keyvalue;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.master.Master;
import alluxio.master.MasterFactory;
import alluxio.master.MasterRegistry;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalFactory;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link KeyValueMaster} instance.
 */
@ThreadSafe
public final class KeyValueMasterFactory implements MasterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(KeyValueMasterFactory.class);

  /**
   * Constructs a new {@link KeyValueMasterFactory}.
   */
  public KeyValueMasterFactory() {}

  @Override
  public boolean isEnabled() {
    return Configuration.getBoolean(PropertyKey.KEY_VALUE_ENABLED);
  }

  @Override
  public String getName() {
    return Constants.KEY_VALUE_MASTER_NAME;
  }

  @Override
  public Master create(MasterRegistry registry, JournalFactory journalFactory) {
    if (!isEnabled()) {
      return null;
    }
    Preconditions.checkArgument(journalFactory != null, "journal factory may not be null");
    LOG.info("Creating {} ", KeyValueMaster.class.getName());
    Journal journal = journalFactory.create(getName());
    return new KeyValueMaster(registry, journal);
  }
}
