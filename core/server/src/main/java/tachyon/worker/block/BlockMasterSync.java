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

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Sessions;
import tachyon.StorageTierAssoc;
import tachyon.WorkerStorageTierAssoc;
import tachyon.conf.TachyonConf;
import tachyon.exception.BlockDoesNotExistException;
import tachyon.exception.ConnectionFailedException;
import tachyon.exception.InvalidWorkerStateException;
import tachyon.exception.TachyonException;
import tachyon.heartbeat.HeartbeatExecutor;
import tachyon.thrift.Command;
import tachyon.util.ThreadFactoryUtils;
import tachyon.WorkerNetAddress;
import tachyon.worker.WorkerContext;
import tachyon.worker.WorkerIdRegistry;

/**
 * Task that carries out the necessary block worker to master communications, including register and
 * heartbeat. This class manages its own {@link BlockMasterClient}.
 *
 * When running, this task first requests a block report from the
 * {@link tachyon.worker.block.BlockWorker}, then sends it to the master. The master may
 * respond to the heartbeat with a command which will be executed. After which, the task will wait
 * for the elapsed time since its last heartbeat has reached the heartbeat interval. Then the cycle
 * will continue.
 *
 * If the task fails to heartbeat to the master, it will destroy its old master client and recreate
 * it before retrying.
 */
@NotThreadSafe
public final class BlockMasterSync implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int DEFAULT_BLOCK_REMOVER_POOL_SIZE = 10;

  /** The block worker responsible for interacting with Tachyon and UFS storage. */
  private final BlockWorker mBlockWorker;

  /** The net address of the worker. */
  private final WorkerNetAddress mWorkerAddress;

  /** Milliseconds between heartbeats before a timeout. */
  private final int mHeartbeatTimeoutMs;

  /** Client for all master communication. */
  private final BlockMasterClient mMasterClient;

  /** The thread pool to remove block. */
  private final ExecutorService mBlockRemovalService = Executors.newFixedThreadPool(
      DEFAULT_BLOCK_REMOVER_POOL_SIZE, ThreadFactoryUtils.build("block-removal-service-%d", true));

  /** Last System.currentTimeMillis() timestamp when a heartbeat successfully completed. */
  private long mLastSuccessfulHeartbeatMs;

  /**
   * Creates a new instance of {@link BlockMasterSync}.
   *
   * @param blockWorker the {@link BlockWorker} this syncer is updating to
   * @param workerAddress the net address of the worker
   * @param masterClient the Tachyon master client
   */
  BlockMasterSync(BlockWorker blockWorker, WorkerNetAddress workerAddress,
      BlockMasterClient masterClient) {
    mBlockWorker = blockWorker;
    mWorkerAddress = workerAddress;
    TachyonConf conf = WorkerContext.getConf();
    mMasterClient = masterClient;
    mHeartbeatTimeoutMs = conf.getInt(Constants.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS);

    try {
      registerWithMaster();
      mLastSuccessfulHeartbeatMs = System.currentTimeMillis();
    } catch (IOException e) {
      // If failed to register when the thread starts, no retry will happen.
      throw new RuntimeException("Failed to register with master.", e);
    } catch (ConnectionFailedException e) {
      throw new RuntimeException("Failed to register with master.", e);
    }
  }

  /**
   * Registers with the Tachyon master. This should be called before the continuous heartbeat thread
   * begins. The workerId will be set after this method is successful.
   *
   * @throws IOException when workerId cannot be found
   * @throws ConnectionFailedException if network connection failed
   */
  private void registerWithMaster() throws IOException, ConnectionFailedException {
    BlockStoreMeta storeMeta = mBlockWorker.getStoreMeta();
    try {
      StorageTierAssoc storageTierAssoc = new WorkerStorageTierAssoc(WorkerContext.getConf());
      mMasterClient.register(WorkerIdRegistry.getWorkerId(),
          storageTierAssoc.getOrderedStorageAliases(), storeMeta.getCapacityBytesOnTiers(),
          storeMeta.getUsedBytesOnTiers(), storeMeta.getBlockList());
    } catch (IOException e) {
      LOG.error("Failed to register with master.", e);
      throw e;
    } catch (TachyonException e) {
      LOG.error("Failed to register with master.", e);
      throw new IOException(e);
    }
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
    try {
      cmdFromMaster = mMasterClient
          .heartbeat(WorkerIdRegistry.getWorkerId(), storeMeta.getUsedBytesOnTiers(),
              blockReport.getRemovedBlocks(), blockReport.getAddedBlocks());
      handleMasterCommand(cmdFromMaster);
      mLastSuccessfulHeartbeatMs = System.currentTimeMillis();
    } catch (Exception e) {
      // An error occurred, log and ignore it or error if heartbeat timeout is reached
      if (cmdFromMaster == null) {
        LOG.error("Failed to receive master heartbeat command.", e);
      } else {
        LOG.error("Failed to receive or execute master heartbeat command: {}",
            cmdFromMaster.toString(), e);
      }
      mMasterClient.resetConnection();
      if (System.currentTimeMillis() - mLastSuccessfulHeartbeatMs >= mHeartbeatTimeoutMs) {
        throw new RuntimeException("Master heartbeat timeout exceeded: " + mHeartbeatTimeoutMs);
      }
    }
  }

  @Override
  public void close() {
    mBlockRemovalService.shutdown();
  }

  /**
   * Handles a master command. The command is one of Unknown, Nothing, Register, Free, or Delete.
   * This call will block until the command is complete.
   *
   * @param cmd the command to execute
   * @throws Exception if an error occurs when executing the command
   */
  // TODO(calvin): Evaluate the necessity of each command.
  private void handleMasterCommand(Command cmd) throws Exception {
    if (cmd == null) {
      return;
    }
    switch (cmd.getCommandType()) {
      // Currently unused
      case Delete:
        break;
      // Master requests blocks to be removed from Tachyon managed space.
      case Free:
        for (long block : cmd.getData()) {
          mBlockRemovalService.execute(new BlockRemover(mBlockWorker,
              Sessions.MASTER_COMMAND_SESSION_ID, block));
        }
        break;
      // No action required
      case Nothing:
        break;
      // Master requests re-registration
      case Register:
        WorkerIdRegistry.registerWithBlockMaster(mMasterClient, mWorkerAddress);
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

  /**
   * Thread to remove block from master.
   */
  @NotThreadSafe
  private class BlockRemover implements Runnable {
    private BlockWorker mBlockWorker;
    private long mSessionId;
    private long mBlockId;

    /**
     * Creates a new instance of {@link BlockRemover}.
     *
     * @param blockWorker block worker for data manager
     * @param sessionId the session id
     * @param blockId the block id
     */
    public BlockRemover(BlockWorker blockWorker, long sessionId, long blockId) {
      mBlockWorker = blockWorker;
      mSessionId = sessionId;
      mBlockId = blockId;
    }

    @Override
    public void run() {
      try {
        mBlockWorker.removeBlock(mSessionId, mBlockId);
        LOG.info("Block {} removed at session {}", mBlockId, mSessionId);
      } catch (IOException e) {
        LOG.warn("Failed master free block cmd for: {} due to concurrent read.", mBlockId, e);
      } catch (InvalidWorkerStateException e) {
        LOG.warn("Failed master free block cmd for: {} due to block uncommitted.", mBlockId, e);
      } catch (BlockDoesNotExistException e) {
        LOG.warn("Failed master free block cmd for: {} due to block not found.", mBlockId, e);
      }
    }
  }
}
