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

import alluxio.ProcessUtils;
import alluxio.Sessions;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.security.user.ServerUserState;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.WaitForOptions;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The class is responsible for managing all top level components of the Block Worker.
 *
 * This block worker implementation register workers to all masters.
 */
@NotThreadSafe
public class AllMasterRegistrationBlockWorker extends DefaultBlockWorker {
  private static final Logger LOG = LoggerFactory.getLogger(AllMasterRegistrationBlockWorker.class);
  private static final long WORKER_ALL_MASTER_REGISTRATION_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.WORKER_ALL_MASTER_REGISTRATION_TIMEOUT_MS);
  private final Map<InetSocketAddress, SpecificMasterBlockSync>
      mMasterSyncOperators = new HashMap<>();

  private BlockMasterClientPool.Factory mBlockMasterClientPoolFactory;

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
    mBlockMasterClientPoolFactory = new BlockMasterClientPool.Factory();
  }

  @VisibleForTesting
  void setBlockMasterClientPoolFactory(BlockMasterClientPool.Factory factory) {
    mBlockMasterClientPoolFactory = factory;
  }

  @Override
  protected void setupBlockMasterSync() throws IOException {
    // Prepare one pool for each master because there are concurrent requests
    List<InetSocketAddress> masterAddresses =
        ConfigurationUtils.getMasterRpcAddresses(Configuration.global());

    // Create a client pool to each master
    // TODO(jiacheng/elega): handle master quorum changes
    Map<InetSocketAddress, BlockMasterClientPool> masterClientPools = new HashMap<>();
    for (InetSocketAddress masterAddr : masterAddresses) {
      BlockMasterClientPool masterClientPool = mBlockMasterClientPoolFactory.create(masterAddr);
      masterClientPools.put(masterAddr, masterClientPool);
      mResourceCloser.register(masterClientPool);
      BlockHeartbeatReporter heartbeatReporter = new BlockHeartbeatReporter();
      mBlockStore.registerBlockStoreEventListener(heartbeatReporter);
      // Setup BlockMasterSync
      SpecificMasterBlockSync blockMasterSync = mResourceCloser
          .register(new SpecificMasterBlockSync(
              this, mWorkerId, mAddress, masterClientPool, heartbeatReporter));
      // Register each BlockMasterSync to the block events on this worker
      getExecutorService()
          .submit(new HeartbeatThread(HeartbeatContext.WORKER_BLOCK_SYNC, blockMasterSync,
              () -> Configuration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS),
              Configuration.global(), ServerUserState.global()));
      mMasterSyncOperators.put(masterAddr, blockMasterSync);
      LOG.info("Kick off BlockMasterSync with master {}", masterAddr);
    }
  }

  private void waitForPrimaryMasterRegistrationComplete()
      throws UnavailableException {
    InetSocketAddress primaryMasterAddress =
        (InetSocketAddress) mFileSystemMasterClient.getRemoteSockAddress();

    SpecificMasterBlockSync primaryMasterSync =
        mMasterSyncOperators.get(primaryMasterAddress);
    Preconditions.checkNotNull(
        primaryMasterSync, "Primary master block sync should not be null");
    try {
      CommonUtils.waitFor(this + " to start",
          primaryMasterSync::isRegistered,
          WaitForOptions.defaults().setTimeoutMs((int) WORKER_ALL_MASTER_REGISTRATION_TIMEOUT_MS));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Exit the worker on interruption", e);
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      ProcessUtils.fatalError(LOG, e, "Failed to register with primary master");
    }
    LOG.info("The worker has registered with primary master, address {}",
        mFileSystemMasterClient.getRemoteSockAddress());
  }

  @Override
  public void start(WorkerNetAddress address) throws IOException {
    super.start(address);
    // Because standby masters do not take read requests,
    // workers can start as long as it has registered to the primary master.
    // The block changes will be report to standby masters later once the registration is done.
    waitForPrimaryMasterRegistrationComplete();
  }

  /**
   * @return the master sync operators
   */
  public Map<InetSocketAddress, SpecificMasterBlockSync> getMasterSyncOperators() {
    return mMasterSyncOperators;
  }
}
