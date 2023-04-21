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

import alluxio.ClientContext;
import alluxio.ProcessUtils;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.heartbeat.FixedIntervalSupplier;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.MasterClientContext;
import alluxio.security.user.ServerUserState;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.WaitForOptions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

/**
 * An abstraction layer that manages the worker heartbeats with multiple block masters.
 * This is only active when worker.register.to.all.masters=true.
 */
public class BlockSyncMasterGroup implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(SpecificMasterBlockSync.class);
  private volatile boolean mStarted = false;

  private final boolean mTestMode = Configuration.getBoolean(PropertyKey.TEST_MODE);

  private static BlockMasterClientFactory sBlockMasterClientFactory
      = new BlockMasterClientFactory();

  private static final long WORKER_MASTER_CONNECT_RETRY_TIMEOUT =
      Configuration.getMs(PropertyKey.WORKER_MASTER_CONNECT_RETRY_TIMEOUT);

  /**
   * Creates a block sync master group.
   * @param masterAddresses the master addresses to sync
   * @param blockWorker the block worker instance
   */
  public BlockSyncMasterGroup(
      List<InetSocketAddress> masterAddresses,
      BlockWorker blockWorker
  ) throws IOException {
    // TODO(elega): handle master membership changes
    // https://github.com/Alluxio/alluxio/issues/16898
    for (InetSocketAddress masterAddr : masterAddresses) {
      BlockMasterClient masterClient = sBlockMasterClientFactory.create(masterAddr);
      BlockHeartbeatReporter heartbeatReporter = new BlockHeartbeatReporter();

      blockWorker.getBlockStore().registerBlockStoreEventListener(heartbeatReporter);
      // Setup BlockMasterSync
      SpecificMasterBlockSync blockMasterSync = mTestMode
          ? new TestSpecificMasterBlockSync(
          blockWorker, masterClient, heartbeatReporter)
          : new SpecificMasterBlockSync(
              blockWorker, masterClient, heartbeatReporter);
      // Register each BlockMasterSync to the block events on this worker
      mMasterSyncOperators.put(masterAddr, blockMasterSync);
      LOG.info("Kick off BlockMasterSync with master {}", masterAddr);
    }
  }

  /**
   * Starts the heartbeats.
   * @param executorService the executor service to run the heartbeats
   */
  public synchronized void start(ExecutorService executorService) {
    if (!mStarted) {
      mStarted = true;
    }
    mMasterSyncOperators.values().forEach(blockMasterSync -> executorService
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_BLOCK_SYNC, blockMasterSync,
            () -> new FixedIntervalSupplier(
                Configuration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)),
            Configuration.global(), ServerUserState.global())));
  }

  private final Map<InetSocketAddress, SpecificMasterBlockSync> mMasterSyncOperators =
      new HashMap<>();

  @Override
  public void close() throws IOException {
    mMasterSyncOperators.values().forEach(
        SpecificMasterBlockSync::close
    );
  }

  static void setBlockMasterClientFactory(BlockMasterClientFactory factory) {
    sBlockMasterClientFactory = factory;
  }

  /**
   * Waits until the primary master registration completes.
   * @param primaryMasterAddress the primary master address
   */
  public void waitForPrimaryMasterRegistrationComplete(InetSocketAddress primaryMasterAddress) {
    SpecificMasterBlockSync primaryMasterSync =
        mMasterSyncOperators.get(primaryMasterAddress);
    Preconditions.checkNotNull(
        primaryMasterSync, "Primary master block sync should not be null");
    try {
      CommonUtils.waitFor(this + " to start",
          primaryMasterSync::isRegistered,
          WaitForOptions.defaults().setTimeoutMs(WORKER_MASTER_CONNECT_RETRY_TIMEOUT));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Exit the worker on interruption", e);
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      ProcessUtils.fatalError(LOG, e, "Failed to register with primary master");
    }
    LOG.info("The worker has registered with primary master, address {}", primaryMasterAddress);
  }

  /**
   * @return if the worker is registered to all masters
   */
  public boolean isRegisteredToAllMasters() {
    return mMasterSyncOperators.values().stream().allMatch(SpecificMasterBlockSync::isRegistered);
  }

  /**
   * @return the master sync operators
   */
  public Map<InetSocketAddress, SpecificMasterBlockSync> getMasterSyncOperators() {
    return mMasterSyncOperators;
  }

  /**
   * The factory class.
   */
  public static class Factory {
    /**
     * Creates a block sync master group that heartbeats to all masters.
     * @param blockWorker the block worker instance
     * @return the block sync master group instance
     */
    public static BlockSyncMasterGroup createAllMasterSync(BlockWorker blockWorker) {
      List<InetSocketAddress> masterAddresses =
          ConfigurationUtils.getMasterRpcAddresses(Configuration.global());
      try {
        return new BlockSyncMasterGroup(masterAddresses, blockWorker);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * A factory class for testing purpose.
   */
  @VisibleForTesting
  static class BlockMasterClientFactory {
    BlockMasterClient create(InetSocketAddress address) {
      MasterClientContext context = MasterClientContext
          .newBuilder(ClientContext.create(Configuration.global())).build();

      return new BlockMasterClient(context, address);
    }
  }
}
