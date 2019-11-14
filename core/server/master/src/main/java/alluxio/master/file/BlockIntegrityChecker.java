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

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.heartbeat.HeartbeatExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Heartbeat executor for validating inode and block integrity.
 */
public final class BlockIntegrityChecker implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(BlockIntegrityChecker.class);

  private final FileSystemMaster mFileSystemMaster;
  private final boolean mRepair;

  /**
   * Constructs a block integrity checker based on the given {@link FileSystemMaster}.
   *
   * @param fsm the master to check
   */
  public BlockIntegrityChecker(FileSystemMaster fsm) {
    mFileSystemMaster = fsm;
    mRepair = ServerConfiguration
        .getBoolean(PropertyKey.MASTER_PERIODIC_BLOCK_INTEGRITY_CHECK_REPAIR);
  }

  @Override
  public void heartbeat() {
    try {
      mFileSystemMaster.validateInodeBlocks(mRepair);
    } catch (Exception e) {
      LOG.error("Failed to run periodic block integrity check.", e);
    }
  }

  @Override
  public void close() {
    // Nothing to clean up.
  }
}
