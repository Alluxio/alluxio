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
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.FailedToAcquireRegisterLeaseException;
import alluxio.grpc.Command;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.wire.WorkerNetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  private static final long ACQUIRE_LEASE_WAIT_MAX_DURATION =
      Configuration.getMs(PropertyKey.WORKER_REGISTER_LEASE_RETRY_MAX_DURATION);
  private static final int HEARTBEAT_TIMEOUT_MS =
      (int) Configuration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS);

  /** The block worker responsible for interacting with Alluxio and UFS storage. */
  private final BlockWorker mBlockWorker;

  /** The worker ID for the worker. This may change if the master asks the worker to re-register. */
  private final AtomicReference<Long> mWorkerId;

  /** The net address of the worker. */
  private final WorkerNetAddress mWorkerAddress;

  /** Client-pool for all master communication. */
  private final BlockMasterClientPool mMasterClientPool;
  /** Client for all master communication. */
  private BlockMasterClient mMasterClient;

  /** An async service to remove block. */
  private final AsyncBlockRemover mAsyncBlockRemover;

  /** Last System.currentTimeMillis() timestamp when a heartbeat successfully completed. */
  private long mLastSuccessfulHeartbeatMs;

  /** The helper instance for sync related methods. */
  private final BlockMasterSyncHelper mBlockMasterSyncHelper;

  /**
   * Creates a new instance of {@link BlockMasterSync}.
   *
   * @param blockWorker the {@link BlockWorker} this syncer is updating to
   * @param workerId the worker id of the worker, assigned by the block master
   * @param workerAddress the net address of the worker
   * @param masterClientPool the Alluxio master client pool
   */
  public BlockMasterSync(BlockWorker blockWorker, AtomicReference<Long> workerId,
      WorkerNetAddress workerAddress, BlockMasterClientPool masterClientPool) throws IOException {
    mBlockWorker = blockWorker;
    mWorkerId = workerId;
    mWorkerAddress = workerAddress;
    mMasterClientPool = masterClientPool;
    mMasterClient = mMasterClientPool.acquire();
    mAsyncBlockRemover = new AsyncBlockRemover(mBlockWorker);
    mBlockMasterSyncHelper = new BlockMasterSyncHelper(mMasterClient);

    registerWithMaster();
    mLastSuccessfulHeartbeatMs = System.currentTimeMillis();
  }

  /**
   * Registers with the Alluxio master. This should be called before the
   * continuous heartbeat thread begins.
   */
  private void registerWithMaster() throws IOException {
    BlockStoreMeta storeMeta = mBlockWorker.getStoreMetaFull();
    try {
      mBlockMasterSyncHelper.tryAcquireLease(mWorkerId.get(), storeMeta);
    } catch (FailedToAcquireRegisterLeaseException e) {
      mMasterClient.disconnect();
      if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
        throw new RuntimeException(String.format("Master register lease timeout exceeded: %dms",
            ACQUIRE_LEASE_WAIT_MAX_DURATION));
      }
      ProcessUtils.fatalError(LOG, "Master register lease timeout exceeded: %dms",
          ACQUIRE_LEASE_WAIT_MAX_DURATION);
    }
    mBlockMasterSyncHelper.registerToMaster(mWorkerId.get(), storeMeta);
  }

  /**
   * Heartbeats to the master node about the change in the worker's managed space.
   */
  @Override
  public void heartbeat() {
    boolean success = mBlockMasterSyncHelper.heartbeat(
        mWorkerId.get(), mBlockWorker.getReport(),
        mBlockWorker.getStoreMeta(), this::handleMasterCommand);
    if (success) {
      mLastSuccessfulHeartbeatMs = System.currentTimeMillis();
    } else {
      if (HEARTBEAT_TIMEOUT_MS > 0) {
        if (System.currentTimeMillis() - mLastSuccessfulHeartbeatMs >= HEARTBEAT_TIMEOUT_MS) {
          if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
            throw new RuntimeException(
                String.format("Master heartbeat timeout exceeded: %s", HEARTBEAT_TIMEOUT_MS));
          }
          // TODO(andrew): Propagate the exception to the main thread and exit there.
          ProcessUtils.fatalError(LOG, "Master heartbeat timeout exceeded: %d",
              HEARTBEAT_TIMEOUT_MS);
        }
      }
    }
  }

  @Override
  public void close() {
    BlockMasterClient tmpBlockMasterClient = mMasterClient;
    mMasterClient = null;
    mAsyncBlockRemover.shutDown();
    mMasterClientPool.release(tmpBlockMasterClient);
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
      case Decommission:
        // TODO(Tony Sun): Doing something.
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
