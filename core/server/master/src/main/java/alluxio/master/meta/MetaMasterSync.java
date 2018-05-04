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

package alluxio.master.meta;

import alluxio.heartbeat.HeartbeatExecutor;

import alluxio.master.MasterProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Task that carries out the necessary standby master to leader master communications, including register and
 * heartbeat. This class manages its own {@link MetaMasterMasterClient}.
 *
 * When running, this task first requests master configuration from the standby master,
 * then sends it to the leader master. After which, the task will wait for the elapsed time
 * since its last heartbeat has reached the heartbeat interval. Then the cycle will continue.
 *
 * If the task fails to heartbeat to the leader master, it will destroy its old master client and recreate
 * it before retrying.
 */
@NotThreadSafe
public final class MetaMasterSync implements HeartbeatExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(MetaMasterSync.class);

  private static final int DEFAULT_BLOCK_REMOVER_POOL_SIZE = 10;

  /** The master process responsible for interacting with leader master. */
  private final MasterProcess mMasterProcess;

  /** The worker ID for the worker. This may change if the master asks the worker to re-register. */
  private final AtomicReference<Long> mWorkerId;

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

  /** Map from a block ID to whether it has been removed successfully. */
  @GuardedBy("itself")
  private final Map<Long, Boolean> mRemovingBlockIdToFinished;

  /**
   * Creates a new instance of {@link MetaMasterSync}.
   *
   * @param blockWorker the {@link BlockWorker} this syncer is updating to
   * @param workerId the worker id of the worker, assigned by the block master
   * @param workerAddress the net address of the worker
   * @param masterClient the Alluxio master client
   */
  MetaMasterSync(BlockWorker blockWorker, AtomicReference<Long> workerId,
                  WorkerNetAddress workerAddress, BlockMasterClient masterClient) throws IOException {
    mBlockWorker = blockWorker;
    mWorkerId = workerId;
    mWorkerAddress = workerAddress;
    mMasterClient = masterClient;
    mHeartbeatTimeoutMs = (int) Configuration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_TIMEOUT_MS);
    mRemovingBlockIdToFinished = new HashMap<>();

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
    mMasterClient.register(mWorkerId.get(),
        storageTierAssoc.getOrderedStorageAliases(), storeMeta.getCapacityBytesOnTiers(),
        storeMeta.getUsedBytesOnTiers(), storeMeta.getBlockList());
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
          if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
            throw new RuntimeException("Master heartbeat timeout exceeded: " + mHeartbeatTimeoutMs);
          }
          LOG.error("Master heartbeat timeout exceeded: " + mHeartbeatTimeoutMs);
          // TODO(andrew): Propagate the exception to the main thread and exit there.
          System.exit(-1);
        }
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
        synchronized (mRemovingBlockIdToFinished) {
          List<Long> blocks = new ArrayList<>();
          for (long block : cmd.getData()) {
            if (!mRemovingBlockIdToFinished.containsKey(block)) {
              blocks.add(block);
              mRemovingBlockIdToFinished.put(block, false);
            }
          }
          if (!blocks.isEmpty()) {
            mBlockRemovalService.execute(new BlockRemover(blocks));
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
   * Thread to remove blocks that have been freed by the master.
   */
  @NotThreadSafe
  private class BlockRemover implements Runnable {
    private final List<Long> mBlocks;

    /**
     * @param blocks the blocks to remove
     */
    public BlockRemover(List<Long> blocks) {
      mBlocks = blocks;
    }

    @Override
    public void run() {
      for (long block : mBlocks) {
        try {
          mBlockWorker.removeBlock(Sessions.MASTER_COMMAND_SESSION_ID, block);
          synchronized (mRemovingBlockIdToFinished) {
            mRemovingBlockIdToFinished.put(block, true);
          }
          LOG.debug("Removed block {} due to request from master", block);
          continue;
        } catch (IOException e) {
          LOG.warn("Failed master free block cmd for: {}.", block, e);
        } catch (InvalidWorkerStateException e) {
          LOG.warn("Failed master free block cmd for: {} due to block uncommitted.", block, e);
        } catch (BlockDoesNotExistException e) {
          LOG.warn("Failed master free block cmd for: {} due to block not found.", block, e);
        }
        // The remove operation fails, so remove the block from the map in order to make it
        // possible for another BlockRemover to remove it later.
        synchronized (mRemovingBlockIdToFinished) {
          mRemovingBlockIdToFinished.remove(block);
        }
      }
    }
  }
}

}
