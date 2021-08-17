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

package alluxio.worker.block;

import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.worker.file.FileSystemMasterClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * PinListSync periodically syncs the set of pinned inodes from master,  and saves the new pinned
 * inodes to the {@link BlockWorker}.
 *
 */
@NotThreadSafe
public final class PinListSync implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(PinListSync.class);

  /** Block worker handle responsible for interacting with Alluxio and UFS storage. */
  private final BlockWorker mBlockWorker;

  /** Client for all master communication. */
  private FileSystemMasterClient mMasterClient;

  /**
   * Creates a new instance of {@link PinListSync}.
   *
   * @param blockWorker the block worker handle
   * @param masterClient the Alluxio master client
   */
  public PinListSync(BlockWorker blockWorker, FileSystemMasterClient masterClient) {
    mBlockWorker = blockWorker;
    mMasterClient = masterClient;
  }

  @Override
  public void heartbeat() {
    // Send the sync
    try {
      Set<Long> pinList = mMasterClient.getPinList();
      mBlockWorker.updatePinList(pinList);
    } catch (Exception e) {
      // An error occurred, retry after 1 second or error if sync timeout is reached
      LOG.warn("Failed to receive pinlist: {}", e.toString());
      LOG.debug("Exception: ", e);
      // TODO(gene): Add this method to AbstractMasterClient.
      // mMasterClient.resetConnection();
    }
  }

  @Override
  public void close() {
    // Nothing to clean up
  }
}
