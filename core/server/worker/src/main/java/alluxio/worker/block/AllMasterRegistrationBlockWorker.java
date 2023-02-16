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

import alluxio.Sessions;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.file.FileSystemMasterClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The class is responsible for managing all top level components of BlockWorker.
 *
 * This block worker implementation register workers to all masters.
 */
@NotThreadSafe
public class AllMasterRegistrationBlockWorker extends DefaultBlockWorker {
  private static final Logger LOG = LoggerFactory.getLogger(AllMasterRegistrationBlockWorker.class);
  private BlockSyncMasterGroup mBlockSyncMasterGroup;

  /**
   * Constructs a block worker when workers register to all masters.
   *
   * @param blockMasterClientPool a client pool for talking to the block master
   * @param fileSystemMasterClient a client for talking to the file system master
   * @param sessions an object for tracking and cleaning up client sessions
   * @param blockStore an Alluxio block store
   * @param workerId worker id
   */
  public AllMasterRegistrationBlockWorker(
      BlockMasterClientPool blockMasterClientPool,
      FileSystemMasterClient fileSystemMasterClient, Sessions sessions,
      BlockStore blockStore, AtomicReference<Long> workerId) {
    super(blockMasterClientPool, fileSystemMasterClient, sessions, blockStore, workerId);
  }

  @Override
  protected void setupBlockMasterSync() {
    mBlockSyncMasterGroup =
        BlockSyncMasterGroup.Factory.createAllMasterSync(this);
    mResourceCloser.register(mBlockSyncMasterGroup);
    mBlockSyncMasterGroup.start(getExecutorService());
  }

  @Override
  public void start(WorkerNetAddress address) throws IOException {
    super.start(address);

    InetSocketAddress primaryMasterAddress =
        (InetSocketAddress) mFileSystemMasterClient.getRemoteSockAddress();
    // Registrations on standby masters are not required to complete for starting a worker
    // because standby masters do not serve read requests.
    // Standby masters will catch up following block location changes via worker heartbeats.
    mBlockSyncMasterGroup.waitForPrimaryMasterRegistrationComplete(primaryMasterAddress);
  }

  /**
   * @return the block sync master group
   */
  public BlockSyncMasterGroup getBlockSyncMasterGroup() {
    return mBlockSyncMasterGroup;
  }
}
