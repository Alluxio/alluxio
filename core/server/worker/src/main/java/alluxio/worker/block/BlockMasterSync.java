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

import alluxio.conf.ServerConfiguration;
import alluxio.ProcessUtils;
import alluxio.conf.PropertyKey;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.ConnectionFailedException;
import alluxio.grpc.Command;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.Scope;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.metrics.MetricsSystem;
import alluxio.util.ConfigurationUtils;
import alluxio.wire.WorkerNetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Task that carries out the necessary block worker to master communications, including register and
 * heartbeat. This class manages its own {@link BlockMasterClient}.
 *
 * When running, this task first requests a block report from the
 * {@link alluxio.worker.block.BlockWorker}, then sends it to the master. The master may
 * respond to the heartbeat with a command which will be executed. After which, the task will wait
 * for the elapsed time since its last heartbeat has reached the heartbeat interval. Then the cycle
 * will continue.
 *
 * If the task fails to heartbeat to the master, it will destroy its old master client and recreate
 * it before retrying.
 */
@NotThreadSafe
public final class BlockMasterSync implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(BlockMasterSync.class);

  /** The block worker responsible for interacting with Alluxio and UFS storage. */
  private final BlockWorker mBlockWorker;

  /** The worker ID for the worker. This may change if the master asks the worker to re-register. */
  private final AtomicReference<Long> mWorkerId;

  /** The net address of the worker. */
  private final WorkerNetAddress mWorkerAddress;

  /** Milliseconds between heartbeats before a timeout. */
  private final int mHeartbeatTimeoutMs;

  /** Client-pool for all master communication. */
  private final BlockMasterClientPool mMasterClientPool;
  /** Client for all master communication. */
  private BlockMasterClient mMasterClient;

  /** An async service to remove block. */
  private final AsyncBlockRemover mAsyncBlockRemover;

  /** Last System.currentTimeMillis() timestamp when a heartbeat successfully completed. */
  private long mLastSuccessfulHeartbeatMs;

  /**
   * Creates a new instance of {@link BlockMasterSync}.
   *
   * @param blockWorker the {@link BlockWorker} this syncer is updating to
   * @param workerId the worker id of the worker, assigned by the block master
   * @param workerAddress the net address of the worker
   * @param masterClientPool the Alluxio master client pool
   */
  BlockMasterSync(BlockWorker blockWorker, AtomicReference<Long> workerId,
      WorkerNetAddress workerAddress, BlockMasterClientPool masterClientPool) throws IOException {
    mBlockWorker = blockWorker;
    mWorkerId = workerId;
    mWorkerAddress = workerAddress;
    mMasterClientPool = masterClientPool;
    mMasterClient = mMasterClientPool.acquire();
    mHeartbeatTimeoutMs = (int) ServerConfiguration
        .getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS);
    mAsyncBlockRemover = new AsyncBlockRemover(mBlockWorker);

    registerWithMaster();
    mLastSuccessfulHeartbeatMs = System.currentTimeMillis();
  }

  /**
   * Registers with the Alluxio master. This should be called before the continuous heartbeat thread
   * begins.
   */
  private void registerWithMaster() throws IOException {
    BlockStoreMeta storeMeta = mBlockWorker.getStoreMetaFull();
    StorageTierAssoc storageTierAssoc = new WorkerStorageTierAssoc();
    List<ConfigProperty> configList =
        ConfigurationUtils.getConfiguration(ServerConfiguration.global(), Scope.WORKER);
    mMasterClient.register(mWorkerId.get(),
        storageTierAssoc.getOrderedStorageAliases(), storeMeta.getCapacityBytesOnTiers(),
        storeMeta.getUsedBytesOnTiers(), storeMeta.getBlockListByStorageLocation(),
        storeMeta.getLostStorage(), configList);
  }

  /**
   * Heartbeats to the master node about the change in the worker's managed space.
   */
  @Override
  public void heartbeat() {
    // Prepare metadata for the next heartbeat
    BlockHeartbeatReport blockReport = mBlockWorker.getReport();
    BlockStoreMeta storeMeta = mBlockWorker.getStoreMeta();

    // Send the heartbeat and execute the response
    Command cmdFromMaster = null;
    List<alluxio.grpc.Metric> metrics = MetricsSystem.reportWorkerMetrics();

    try {
      cmdFromMaster = mMasterClient.heartbeat(mWorkerId.get(), storeMeta.getCapacityBytesOnTiers(),
          storeMeta.getUsedBytesOnTiers(), blockReport.getRemovedBlocks(),
          blockReport.getAddedBlocks(), blockReport.getLostStorage(), metrics);
      handleMasterCommand(cmdFromMaster);
      mLastSuccessfulHeartbeatMs = System.currentTimeMillis();
    } catch (IOException | ConnectionFailedException e) {
      // An error occurred, log and ignore it or error if heartbeat timeout is reached
      if (cmdFromMaster == null) {
        LOG.error("Failed to receive master heartbeat command.", e);
      } else {
        LOG.error("Failed to receive or execute master heartbeat command: {}",
            cmdFromMaster.toString(), e);
      }
      mMasterClient.disconnect();
      if (mHeartbeatTimeoutMs > 0) {
        if (System.currentTimeMillis() - mLastSuccessfulHeartbeatMs >= mHeartbeatTimeoutMs) {
          if (ServerConfiguration.getBoolean(PropertyKey.TEST_MODE)) {
            throw new RuntimeException("Master heartbeat timeout exceeded: " + mHeartbeatTimeoutMs);
          }
          // TODO(andrew): Propagate the exception to the main thread and exit there.
          ProcessUtils.fatalError(LOG, "Master heartbeat timeout exceeded: %d",
              mHeartbeatTimeoutMs);
        }
      }
    }
  }

  @Override
  public void close() {
    mAsyncBlockRemover.shutDown();
    mMasterClientPool.release(mMasterClient);
  }

  /**
   * Handles a master command. The command is one of Unknown, Nothing, Register, Free, or Delete.
   * This call will block until the command is complete.
   *
   * @param cmd the command to execute
   * @throws IOException if I/O errors occur
   * @throws ConnectionFailedException if connection fails
   */
  // TODO(calvin): Evaluate the necessity of each command.
  private void handleMasterCommand(Command cmd) throws IOException, ConnectionFailedException {
    if (cmd == null) {
      return;
    }
    switch (cmd.getCommandType()) {
      // Currently unused
      case Delete:
        break;
      // Master requests blocks to be removed from Alluxio managed space.
      case Free:
        mAsyncBlockRemover.addBlocksToDelete(cmd.getDataList());
        break;
      // No action required
      case Nothing:
        break;
      // Master requests re-registration
      case Register:
        mWorkerId.set(mMasterClient.getId(mWorkerAddress));
        registerWithMaster();
        break;
      // Unknown request
      case Unknown:
        LOG.error("Master heartbeat sends unknown command {}", cmd);
        break;
      default:
        throw new RuntimeException("Un-recognized command from master " + cmd);
    }
  }
}
