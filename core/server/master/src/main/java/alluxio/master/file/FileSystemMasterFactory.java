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

package alluxio.master.file;

import alluxio.Constants;
import alluxio.master.Master;
import alluxio.master.MasterFactory;
import alluxio.master.MasterRegistry;
import alluxio.master.journal.JournalFactory;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory to create a {@link FileSystemMaster} instance.
 */
@ThreadSafe
public final class FileSystemMasterFactory implements MasterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemMasterFactory.class);

  /**
   * Constructs a new {@link FileSystemMasterFactory}.
   */
  public FileSystemMasterFactory() {}

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public String getName() {
    return Constants.FILE_SYSTEM_MASTER_NAME;
  }

  @Override
  public Master create(MasterRegistry registry, JournalFactory journalFactory) {
    Preconditions.checkArgument(journalFactory != null, "journal factory may not be null");
    LOG.info("Creating {} ", FileSystemMaster.class.getName());
    return new FileSystemMaster(registry, journalFactory);
  }
}
