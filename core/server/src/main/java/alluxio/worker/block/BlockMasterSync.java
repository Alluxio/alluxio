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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.Sessions;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.thrift.Command;
import alluxio.util.ThreadFactoryUtils;
import alluxio.wire.WorkerNetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.GuardedBy;
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
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final int DEFAULT_BLOCK_REMOVER_POOL_SIZE = 10;

  /** The block worker responsible for interacting with Alluxio and UFS storage. */
  private final BlockWorker mBlockWorker;

  /** The worker ID for the worker. This may change if the master asks the worker to re-register. */
  private AtomicReference<Long> mWorkerId;

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

  /** Map from a block Id to whether it has been removed successfully. */
  @GuardedBy("itself")
  private final Map<Long, Boolean> mRemovingBlockIdToFinished;

  /**
   * Creates a new instance of {@link BlockMasterSync}.
   *
   * @param blockWorker the {@link BlockWorker} this syncer is updating to
   * @param workerId the worker id of the worker, assigned by the block master
   * @param workerAddress the net address of the worker
   * @param masterClient the Alluxio master client
   */
  BlockMasterSync(BlockWorker blockWorker, AtomicReference<Long> workerId,
      WorkerNetAddress workerAddress, BlockMasterClient masterClient) {
    mBlockWorker = blockWorker;
    mWorkerId = workerId;
    mWorkerAddress = workerAddress;
    mMasterClient = masterClient;
    mHeartbeatTimeoutMs = Configuration.getInt(PropertyKey.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS);
    mRemovingBlockIdToFinished = new HashMap<>();

    try {
      registerWithMaster();
      mLastSuccessfulHeartbeatMs = System.currentTimeMillis();
    } catch (IOException | ConnectionFailedException e) {
      // If failed to register when the thread starts, no retry will happen.
      throw new RuntimeException("Failed to register with master.", e);
    }
  }

  /**
   * Registers with the Alluxio master. This should be called before the continuous heartbeat thread
   * begins.
   *
   * @throws IOException when workerId cannot be found
   * @throws ConnectionFailedException if network connection failed
   */
  private void registerWithMaster() throws IOException, ConnectionFailedException {
    BlockStoreMeta storeMeta = mBlockWorker.getStoreMetaFull();
    try {
      StorageTierAssoc storageTierAssoc = new WorkerStorageTierAssoc();
      mMasterClient.register(mWorkerId.get(),
          storageTierAssoc.getOrderedStorageAliases(), storeMeta.getCapacityBytesOnTiers(),
          storeMeta.getUsedBytesOnTiers(), storeMeta.getBlockList());
    } catch (IOException e) {
      LOG.error("Failed to register with master.", e);
      throw e;
    } catch (AlluxioException e) {
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
          .heartbeat(mWorkerId.get(), storeMeta.getUsedBytesOnTiers(),
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
      // Master requests blocks to be removed from Alluxio managed space.
      case Free:
        synchronized (mRemovingBlockIdToFinished) {
          for (long block : cmd.getData()) {
            if (!mRemovingBlockIdToFinished.containsKey(block)) {
              mRemovingBlockIdToFinished.put(block, false);
              mBlockRemovalService.execute(new BlockRemover(mBlockWorker,
                  mRemovingBlockIdToFinished, Sessions.MASTER_COMMAND_SESSION_ID, block));
            }
          }
          Iterator<Map.Entry<Long, Boolean>> it = mRemovingBlockIdToFinished.entrySet().iterator();
          while (it.hasNext()) {
            if (it.next().getValue()) {
              // The block has been successfully removed.
              it.remove();
            }
          }
        }
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

  /**
   * Thread to remove block from master.
   */
  @NotThreadSafe
  private class BlockRemover implements Runnable {
    private final BlockWorker mBlockWorker;
    private final long mSessionId;
    private final long mBlockId;
    private final Map<Long, Boolean> mRemovingBlockIdToFinished;

    /**
     * Creates a new instance of {@link BlockRemover}.
     *
     * @param blockWorker block worker for data manager
     * @param removingBlockIdToFinished map from block ID to whether it has been removed
     * @param sessionId the session id
     * @param blockId the block id
     */
    public BlockRemover(BlockWorker blockWorker, Map<Long, Boolean> removingBlockIdToFinished,
        long sessionId, long blockId) {
      mBlockWorker = blockWorker;
      mRemovingBlockIdToFinished = removingBlockIdToFinished;
      mSessionId = sessionId;
      mBlockId = blockId;
    }

    @Override
    public void run() {
      boolean success = false;
      try {
        mBlockWorker.removeBlock(mSessionId, mBlockId);
        success = true;
        synchronized (mRemovingBlockIdToFinished) {
          mRemovingBlockIdToFinished.put(mBlockId, true);
        }
        LOG.info("Block {} removed at session {}", mBlockId, mSessionId);
      } catch (IOException e) {
        LOG.warn("Failed master free block cmd for: {}.", mBlockId, e);
      } catch (InvalidWorkerStateException e) {
        LOG.warn("Failed master free block cmd for: {} due to block uncommitted.", mBlockId, e);
      } catch (BlockDoesNotExistException e) {
        LOG.warn("Failed master free block cmd for: {} due to block not found.", mBlockId, e);
      } finally {
        if (!success) {
          synchronized (mRemovingBlockIdToFinished) {
            // The remove operation fails, so remove the block from the map in order to make it
            // possible for another BlockRemover to remove it later.
            mRemovingBlockIdToFinished.remove(mBlockId);
          }
        }
      }
    }
  }
}
