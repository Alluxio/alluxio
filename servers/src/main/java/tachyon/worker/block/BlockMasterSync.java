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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.Users;
import tachyon.client.BlockMasterClient;
import tachyon.conf.TachyonConf;
import tachyon.exception.InvalidStateException;
import tachyon.exception.NotFoundException;
import tachyon.thrift.Command;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;

/**
 * Task that carries out the necessary block worker to master communications, including register and
 * heartbeat. This class manages its own {@link tachyon.master.MasterClient}.
 *
 * When running, this task first requests a block report from the
 * {@link tachyon.worker.block.BlockDataManager}, then sends it to the master. The master may
 * respond to the heartbeat with a command which will be executed. After which, the task will wait
 * for the elapsed time since its last heartbeat has reached the heartbeat interval. Then the cycle
 * will continue.
 *
 * If the task fails to heartbeat to the master, it will destroy its old master client and recreate
 * it before retrying.
 */
public final class BlockMasterSync implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int DEFAULT_BLOCK_REMOVER_POOL_SIZE = 10;
  /** Block data manager responsible for interacting with Tachyon and UFS storage */
  private final BlockDataManager mBlockDataManager;
  /** The net address of the worker */
  private final NetAddress mWorkerAddress;
  /** The configuration values */
  private final TachyonConf mTachyonConf;
  /** Milliseconds between each heartbeat */
  private final int mHeartbeatIntervalMs;
  /** Milliseconds between heartbeats before a timeout */
  private final int mHeartbeatTimeoutMs;

  /** Client for all master communication */
  private BlockMasterClient mMasterClient;
  /** Flag to indicate if the sync should continue */
  private volatile boolean mRunning;
  /** The id of the worker */
  private long mWorkerId;
  /** The thread pool to remove block */
  private final ExecutorService mFixedExecutionService =
          Executors.newFixedThreadPool(DEFAULT_BLOCK_REMOVER_POOL_SIZE);

  BlockMasterSync(BlockDataManager blockDataManager, TachyonConf tachyonConf,
      NetAddress workerAddress, BlockMasterClient masterClient) {
    mBlockDataManager = blockDataManager;
    mWorkerAddress = workerAddress;
    mTachyonConf = tachyonConf;
    mMasterClient = masterClient;
    mHeartbeatIntervalMs =
        mTachyonConf.getInt(Constants.WORKER_TO_MASTER_HEARTBEAT_INTERVAL_MS);
    mHeartbeatTimeoutMs =
        mTachyonConf.getInt(Constants.WORKER_HEARTBEAT_TIMEOUT_MS);

    mRunning = true;
    mWorkerId = 0;
  }

  /**
   * Gets the worker id, 0 if registerWithMaster has not been called successfully.
   *
   * @return the worker id
   */
  public long getWorkerId() {
    return mWorkerId;
  }

  public void setWorkerId() throws IOException {
    try {
      mWorkerId = mMasterClient.workerGetId(mWorkerAddress);
    } catch (IOException ioe) {
      LOG.error("Failed to register with master.", ioe);
      throw ioe;
    }
  }

  /**
   * Registers with the Tachyon master. This should be called before the continuous heartbeat thread
   * begins. The workerId will be set after this method is successful.
   */
  public void registerWithMaster() {
    BlockStoreMeta storeMeta = mBlockDataManager.getStoreMeta();
    try {
      mWorkerId =
          mMasterClient.workerRegister(mWorkerId, storeMeta.getCapacityBytesOnTiers(),
              storeMeta.getUsedBytesOnTiers(), storeMeta.getBlockList());
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to register with master.", ioe);
    }
  }

  /**
   * Main loop for the sync, continuously heartbeats to the master node about the change in the
   * worker's managed space.
   */
  @Override
  public void run() {
    long lastHeartbeatMs = System.currentTimeMillis();
    registerWithMaster();
    while (mRunning) {
      // Check the time since last heartbeat, and wait until it is within heartbeat interval
      long lastIntervalMs = System.currentTimeMillis() - lastHeartbeatMs;
      long toSleepMs = mHeartbeatIntervalMs - lastIntervalMs;
      if (toSleepMs > 0) {
        CommonUtils.sleepMs(LOG, toSleepMs);
      } else {
        LOG.warn("Heartbeat took: " + lastIntervalMs + ", expected: " + mHeartbeatIntervalMs);
      }

      // Prepare metadata for the next heartbeat
      BlockHeartbeatReport blockReport = mBlockDataManager.getReport();
      BlockStoreMeta storeMeta = mBlockDataManager.getStoreMeta();

      // Send the heartbeat and execute the response
      try {
        Command cmdFromMaster =
            mMasterClient.workerHeartbeat(mWorkerId, storeMeta.getUsedBytesOnTiers(),
                blockReport.getRemovedBlocks(), blockReport.getAddedBlocks());
        lastHeartbeatMs = System.currentTimeMillis();
        handleMasterCommand(cmdFromMaster);
      } catch (Exception ioe) {
        // An error occurred, retry after 1 second or error if heartbeat timeout is reached
        LOG.error("Failed to receive or execute master heartbeat command.", ioe);
        // TODO: Add this method in MasterClientBase
        //mMasterClient.resetConnection();
        CommonUtils.sleepMs(LOG, Constants.SECOND_MS);
        if (System.currentTimeMillis() - lastHeartbeatMs >= mHeartbeatTimeoutMs) {
          throw new RuntimeException("Master heartbeat timeout exceeded: " + mHeartbeatTimeoutMs);
        }
      }
    }
  }

  /**
   * Stops the sync, once this method is called, the object should be discarded
   */
  public void stop() {
    mRunning = false;
  }

  /**
   * Handles a master command. The command is one of Unknown, Nothing, Register, Free, or Delete.
   * This call will block until the command is complete.
   *
   * @param cmd the command to execute.
   * @throws Exception if an error occurs when executing the command
   */
  // TODO: Evaluate the necessity of each command
  // TODO: Do this in a non blocking way
  private void handleMasterCommand(Command cmd) throws Exception {
    if (cmd == null) {
      return;
    }
    switch (cmd.mCommandType) {
    // Currently unused
      case Delete:
        break;
      // Master requests blocks to be removed from Tachyon managed space.
      case Free:
        for (long block : cmd.mData) {
          mFixedExecutionService.execute(new BlockRemover(mBlockDataManager,
                  Users.MASTER_COMMAND_USER_ID, block));
        }
        break;
      // No action required
      case Nothing:
        break;
      // Master requests re-registration
      case Register:
        registerWithMaster();
        break;
      // Unknown request
      case Unknown:
        LOG.error("Master heartbeat sends unknown command " + cmd);
        break;
      default:
        throw new RuntimeException("Un-recognized command from master " + cmd);
    }
  }

  /**
   * Thread to remove block from master
   */
  private class BlockRemover implements Runnable {
    private BlockDataManager mBlockDataManager;
    private long mUserId;
    private long mBlockId;

    public BlockRemover(BlockDataManager blockDataManager, long userId, long blockId) {
      mBlockDataManager = blockDataManager;
      mUserId = userId;
      mBlockId = blockId;
    }

    @Override
    public void run() {
      try {
        mBlockDataManager.removeBlock(mUserId, mBlockId);
      } catch (IOException ioe) {
        LOG.warn("Failed master free block cmd for: " + mBlockId + " due to concurrent read.");
      } catch (InvalidStateException e) {
        LOG.warn("Failed master free block cmd for: " + mBlockId + " due to block uncommitted.");
      } catch (NotFoundException e) {
        LOG.warn("Failed master free block cmd for: " + mBlockId + " due to block not found.");
      }
    }

  }
}
