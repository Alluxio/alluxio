/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.keyvalue;

import alluxio.Constants;
import alluxio.master.Master;
import alluxio.master.MasterContext;
import alluxio.master.MasterFactory;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.ReadWriteJournal;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link KeyValueMaster} instance.
 */
@ThreadSafe
public final class KeyValueMasterFactory implements MasterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  @Override
  public boolean isEnabled() {
    return MasterContext.getConf().getBoolean(Constants.KEY_VALUE_ENABLED);
  }

  @Override
  public String getName() {
    return Constants.KEY_VALUE_MASTER_NAME;
  }

  @Override
  public KeyValueMaster create(List<? extends Master> masters, String journalDirectory) {
    if (!isEnabled()) {
      return null;
    }
    Preconditions.checkArgument(journalDirectory != null, "journal path may not be null");
    LOG.info("Creating {} ", KeyValueMaster.class.getName());

    ReadWriteJournal journal =
        new ReadWriteJournal(KeyValueMaster.getJournalDirectory(journalDirectory));

    for (Master master : masters) {
      if (master instanceof FileSystemMaster) {
        LOG.info("{} is created", KeyValueMaster.class.getName());
        return new KeyValueMaster((FileSystemMaster) master, journal);
      }
    }
    LOG.error("Fail to create {} due to missing {}", KeyValueMaster.class.getName(),
        FileSystemMaster.class.getName());
    return null;
  }
}
