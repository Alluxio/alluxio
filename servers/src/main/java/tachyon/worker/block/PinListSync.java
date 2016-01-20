/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.heartbeat.HeartbeatExecutor;
import tachyon.worker.file.FileSystemMasterClient;

/**
 * PinListSync periodically syncs the set of pinned inodes from master,  and saves the new pinned
 * inodes to the {@link BlockDataManager}.
 *
 */
public final class PinListSync implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Block data manager responsible for interacting with Tachyon and UFS storage */
  private final BlockWorker mBlockDataManager;

  /** Client for all master communication */
  private FileSystemMasterClient mMasterClient;

  /**
   * Creates a new instance of {@link PinListSync}.
   *
   * @param blockDataManager a block data manager handle
   * @param masterClient the Tachyon master client
   */
  public PinListSync(BlockWorker blockDataManager, FileSystemMasterClient masterClient) {
    mBlockDataManager = blockDataManager;

    mMasterClient = masterClient;
  }

  @Override
  public void heartbeat() {
    // Send the sync
    try {
      Set<Long> pinList = mMasterClient.getPinList();
      mBlockDataManager.updatePinList(pinList);
    } catch (Exception e) {
      // An error occurred, retry after 1 second or error if sync timeout is reached
      LOG.error("Failed to receive pinlist.", e);
      // TODO(gene): Add this method to MasterClientBase.
      // mMasterClient.resetConnection();
    }
  }

  @Override
  public void close() {
    // Nothing to clean up
  }
}
