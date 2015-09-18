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

package tachyon.master.block;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.HeartbeatExecutor;
import tachyon.HeartbeatThread;
import tachyon.StorageDirId;
import tachyon.StorageLevelAlias;
import tachyon.conf.TachyonConf;
import tachyon.master.IndexedSet;
import tachyon.master.MasterBase;
import tachyon.master.MasterContext;
import tachyon.master.block.journal.BlockContainerIdGeneratorEntry;
import tachyon.master.block.journal.BlockInfoEntry;
import tachyon.master.block.meta.MasterBlockInfo;
import tachyon.master.block.meta.MasterBlockLocation;
import tachyon.master.block.meta.MasterWorkerInfo;
import tachyon.master.journal.Journal;
import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalInputStream;
import tachyon.master.journal.JournalOutputStream;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.BlockLocation;
import tachyon.thrift.BlockMasterService;
import tachyon.thrift.Command;
import tachyon.thrift.CommandType;
import tachyon.thrift.NetAddress;
import tachyon.thrift.TachyonException;
import tachyon.thrift.WorkerInfo;
import tachyon.util.CommonUtils;
import tachyon.util.FormatUtils;
import tachyon.util.ThreadFactoryUtils;
import tachyon.util.io.PathUtils;

/**
 * This master manages the metadata for all the blocks and block workers in Tachyon.
 */
public final class BlockMaster extends MasterBase implements ContainerIdGenerable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Block metadata management. */
  /**
   * Blocks on all workers, including active and lost blocks. This state must be journaled. Access
   * must be synchronized on mBlocks. If both block and worker metadata must be locked, mBlocks must
   * be locked first.
   */
  private final Map<Long, MasterBlockInfo> mBlocks = new HashMap<Long, MasterBlockInfo>();
  /**
   * Keeps track of block which are no longer in tachyon storage. Access must be synchronized on
   * mBlocks.
   */
  private final Set<Long> mLostBlocks = new HashSet<Long>();
  /** This state must be journaled. */
  private final BlockContainerIdGenerator mBlockContainerIdGenerator =
      new BlockContainerIdGenerator();

  /** Worker metadata management. */
  private final IndexedSet.FieldIndex<MasterWorkerInfo> mIdIndex =
      new IndexedSet.FieldIndex<MasterWorkerInfo>() {
        @Override
        public Object getFieldValue(MasterWorkerInfo o) {
          return o.getId();
        }
      };
  private final IndexedSet.FieldIndex<MasterWorkerInfo> mAddressIndex =
      new IndexedSet.FieldIndex<MasterWorkerInfo>() {
        @Override
        public Object getFieldValue(MasterWorkerInfo o) {
          return o.getAddress();
        }
      };
  @SuppressWarnings("unchecked")
  /**
   * All worker information. Access must be synchronized on mWorkers. If both block and worker
   * metadata must be locked, mBlocks must be locked first.
   */
  private final IndexedSet<MasterWorkerInfo> mWorkers =
      new IndexedSet<MasterWorkerInfo>(mIdIndex, mAddressIndex);
  /**
   * Keeps track of workers which are no longer in communication with the master. Access must be
   * synchronized on mWorkers.
   */
  private final BlockingQueue<MasterWorkerInfo> mLostWorkers =
      new LinkedBlockingQueue<MasterWorkerInfo>();
  /** The service that detects lost worker nodes, and tries to restart the failed workers. */
  private Future<?> mLostWorkerDetectionService;
  /** The next worker id to use. This state must be journaled. */
  private final AtomicLong mNextWorkerId = new AtomicLong(1);

  /**
   * @param baseDirectory the base journal directory
   * @return the journal directory for this master
   */
  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, Constants.BLOCK_MASTER_SERVICE_NAME);
  }

  public BlockMaster(Journal journal) {
    super(journal,
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("block-master-%d", true)));
  }

  @Override
  public TProcessor getProcessor() {
    return new BlockMasterService.Processor<BlockMasterServiceHandler>(
        new BlockMasterServiceHandler(this));
  }

  @Override
  public String getServiceName() {
    return Constants.BLOCK_MASTER_SERVICE_NAME;
  }

  @Override
  public void processJournalCheckpoint(JournalInputStream inputStream) throws IOException {
    // clear state before processing checkpoint.
    mBlocks.clear();
    super.processJournalCheckpoint(inputStream);
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    // TODO(gene): A better way to process entries besides a huge switch?
    if (entry instanceof BlockContainerIdGeneratorEntry) {
      mBlockContainerIdGenerator
          .setNextContainerId(((BlockContainerIdGeneratorEntry) entry).getNextContainerId());
    } else if (entry instanceof BlockInfoEntry) {
      BlockInfoEntry blockInfoEntry = (BlockInfoEntry) entry;
      mBlocks.put(blockInfoEntry.getBlockId(),
          new MasterBlockInfo(blockInfoEntry.getBlockId(), blockInfoEntry.getLength()));
    } else {
      throw new IOException("unexpected entry in journal: " + entry);
    }
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    outputStream.writeEntry(mBlockContainerIdGenerator.toJournalEntry());
    for (MasterBlockInfo blockInfo : mBlocks.values()) {
      outputStream.writeEntry(new BlockInfoEntry(blockInfo.getBlockId(), blockInfo.getLength()));
    }
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    super.start(isLeader);
    if (isLeader) {
      mLostWorkerDetectionService =
          getExecutorService().submit(new HeartbeatThread("Lost worker detection service",
              new LostWorkerDetectionHeartbeatExecutor(),
              MasterContext.getConf().getInt(Constants.MASTER_HEARTBEAT_INTERVAL_MS)));
    }
  }

  @Override
  public void stop() throws IOException {
    super.stop();
    if (mLostWorkerDetectionService != null) {
      mLostWorkerDetectionService.cancel(true);
    }
  }

  /**
   * @return the number of workers
   */
  public int getWorkerCount() {
    synchronized (mWorkers) {
      return mWorkers.size();
    }
  }

  /**
   * @return a list of {@link WorkerInfo} objects representing the workers in Tachyon. Called via
   *         RPC, and the internal web ui.
   */
  public List<WorkerInfo> getWorkerInfoList() {
    synchronized (mWorkers) {
      List<WorkerInfo> workerInfoList = new ArrayList<WorkerInfo>(mWorkers.size());
      for (MasterWorkerInfo masterWorkerInfo : mWorkers) {
        workerInfoList.add(masterWorkerInfo.generateClientWorkerInfo());
      }
      return workerInfoList;
    }
  }

  /**
   * @return the total capacity (in bytes) on all tiers, on all workers of Tachyon. Called via RPC
   *         and internal web ui.
   */
  public long getCapacityBytes() {
    long ret = 0;
    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers) {
        ret += worker.getCapacityBytes();
      }
    }
    return ret;
  }

  /**
   * @return the total used bytes on all tiers, on all workers of Tachyon. Called via RPC and
   *         internal web ui.
   */
  public long getUsedBytes() {
    long ret = 0;
    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers) {
        ret += worker.getUsedBytes();
      }
    }
    return ret;
  }

  /**
   * Gets info about the lost workers. Called by the internal web ui.
   *
   * @return a list of worker info
   */
  public List<WorkerInfo> getLostWorkersInfo() {
    synchronized (mWorkers) {
      List<WorkerInfo> ret = new ArrayList<WorkerInfo>(mLostWorkers.size());
      for (MasterWorkerInfo worker : mLostWorkers) {
        ret.add(worker.generateClientWorkerInfo());
      }
      return ret;
    }
  }

  /**
   * Removes blocks from workers. Called by internal masters.
   *
   * @param blockIds a list of block ids to remove from Tachyon space.
   */
  public void removeBlocks(List<Long> blockIds) {
    synchronized (mBlocks) {
      synchronized (mWorkers) {
        for (long blockId : blockIds) {
          MasterBlockInfo masterBlockInfo = mBlocks.get(blockId);
          if (masterBlockInfo == null) {
            continue;
          }
          for (long workerId : masterBlockInfo.getWorkers()) {
            masterBlockInfo.removeWorker(workerId);
            MasterWorkerInfo worker = mWorkers.getFirstByField(mIdIndex, workerId);
            if (worker != null) {
              worker.updateToRemovedBlock(true, blockId);
            }
          }
          mLostBlocks.remove(blockId);
        }
      }
    }
  }

  /**
   * @return a new block container id. Called by internal masters.
   */
  @Override
  public long getNewContainerId() {
    synchronized (mBlockContainerIdGenerator) {
      long containerId = mBlockContainerIdGenerator.getNewContainerId();
      writeJournalEntry(mBlockContainerIdGenerator.toJournalEntry());
      flushJournal();
      return containerId;
    }
  }

  /**
   * Marks a block as committed on a specific worker. Called by workers via RPC.
   *
   * @param workerId the worker id committing the block
   * @param usedBytesOnTier the updated used bytes on the tier of the worker
   * @param tierAlias the tier alias where the worker is committing the block to
   * @param blockId the committing block id
   * @param length the length of the block
   */
  public void commitBlock(long workerId, long usedBytesOnTier, int tierAlias, long blockId,
      long length) {
    LOG.debug("Commit block from worker: {}",
        FormatUtils.parametersToString(workerId, usedBytesOnTier, blockId, length));
    synchronized (mBlocks) {
      synchronized (mWorkers) {
        MasterWorkerInfo workerInfo = mWorkers.getFirstByField(mIdIndex, workerId);
        workerInfo.addBlock(blockId);
        workerInfo.updateUsedBytes(tierAlias, usedBytesOnTier);
        workerInfo.updateLastUpdatedTimeMs();

        MasterBlockInfo masterBlockInfo = mBlocks.get(blockId);
        if (masterBlockInfo == null) {
          masterBlockInfo = new MasterBlockInfo(blockId, length);
          mBlocks.put(blockId, masterBlockInfo);
          writeJournalEntry(
              new BlockInfoEntry(masterBlockInfo.getBlockId(), masterBlockInfo.getLength()));
          flushJournal();
        }
        masterBlockInfo.addWorker(workerId, tierAlias);
        mLostBlocks.remove(blockId);
      }
    }
  }

  /**
   * Marks a block as committed, but without a worker location. This means the block is only in ufs.
   * Called by internal masters.
   *
   * @param blockId the id of the block to commit
   * @param length the length of the block
   */
  public void commitBlockInUFS(long blockId, long length) {
    LOG.debug("Commit block to ufs: {}", FormatUtils.parametersToString(blockId, length));
    synchronized (mBlocks) {
      MasterBlockInfo masterBlockInfo = mBlocks.get(blockId);
      if (masterBlockInfo == null) {
        // The block has not been committed previously, so add the metadata to commit the block.
        masterBlockInfo = new MasterBlockInfo(blockId, length);
        mBlocks.put(blockId, masterBlockInfo);
        writeJournalEntry(
            new BlockInfoEntry(masterBlockInfo.getBlockId(), masterBlockInfo.getLength()));
        flushJournal();
      }
    }
  }

  /**
   * @param blockId the block id to get information for
   * @return the {@link BlockInfo} for the given block id. Called via RPC.
   * @throws BlockInfoException
   */
  public BlockInfo getBlockInfo(long blockId) throws BlockInfoException {
    synchronized (mBlocks) {
      MasterBlockInfo masterBlockInfo = mBlocks.get(blockId);
      if (masterBlockInfo == null) {
        throw new BlockInfoException("Block info not found for " + blockId);
      }
      // Construct the block info object to return.
      synchronized (mWorkers) {
        return generateBlockInfo(masterBlockInfo);
      }
    }
  }

  /**
   * Retrieves information for the given list of block ids. Called by internal masters.
   *
   * @param blockIds A list of block ids to retrieve the information for
   * @return A list of {@link BlockInfo} objects corresponding to the input list of block ids. The
   *         list is in the same order as the input list.
   */
  public List<BlockInfo> getBlockInfoList(List<Long> blockIds) {
    List<BlockInfo> ret = new ArrayList<BlockInfo>(blockIds.size());
    synchronized (mBlocks) {
      synchronized (mWorkers) {
        for (long blockId : blockIds) {
          MasterBlockInfo masterBlockInfo = mBlocks.get(blockId);
          if (masterBlockInfo != null) {
            // Construct the block info object to return.
            ret.add(generateBlockInfo(masterBlockInfo));
          }
        }
        return ret;
      }
    }
  }

  /**
   * @return the total bytes on each storage tier. Called by internal web ui.
   */
  public List<Long> getTotalBytesOnTiers() {
    List<Long> ret = new ArrayList<Long>(Collections.nCopies(StorageLevelAlias.SIZE, 0L));
    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers) {
        for (int i = 0; i < worker.getTotalBytesOnTiers().size(); i ++) {
          ret.set(i, ret.get(i) + worker.getTotalBytesOnTiers().get(i));
        }
      }
    }
    return ret;
  }

  /**
   * @return the used bytes on each storage tier. Called by internal web ui.
   */
  public List<Long> getUsedBytesOnTiers() {
    List<Long> ret = new ArrayList<Long>(Collections.nCopies(StorageLevelAlias.SIZE, 0L));
    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers) {
        for (int i = 0; i < worker.getUsedBytesOnTiers().size(); i ++) {
          ret.set(i, ret.get(i) + worker.getUsedBytesOnTiers().get(i));
        }
      }
    }
    return ret;
  }

  /**
   * Returns a worker id for the given worker. Returns the existing worker id if it exists. Called
   * by workers, via RPC.
   *
   * @param workerNetAddress the worker {@link NetAddress}
   * @return the worker id for this worker
   */
  public long getWorkerId(NetAddress workerNetAddress) {
    // TODO(gene): This NetAddress cloned in case thrift re-uses the object. Does thrift re-use it?
    NetAddress workerAddress = new NetAddress(workerNetAddress);

    synchronized (mWorkers) {
      if (mWorkers.contains(mAddressIndex, workerAddress)) {
        // This worker address is already mapped to a worker id.
        long oldWorkerId = mWorkers.getFirstByField(mAddressIndex, workerAddress).getId();
        LOG.warn("The worker " + workerAddress + " already exists as id " + oldWorkerId + ".");
        return oldWorkerId;
      }

      // Generate a new worker id.
      long workerId = mNextWorkerId.getAndIncrement();
      mWorkers.add(new MasterWorkerInfo(workerId, workerNetAddress));

      LOG.info("getWorkerId(): WorkerNetAddress: " + workerAddress + " id: " + workerId);
      return workerId;
    }
  }

  /**
   * Updates metadata when a worker registers with the master. Called by workers via RPC.
   *
   * @param workerId the worker id of the worker registering
   * @param totalBytesOnTiers list of total bytes on each tier
   * @param usedBytesOnTiers list of the used byes on each tier
   * @param currentBlocksOnTiers a mapping of each storage dir, to all the blocks on that storage
   * @throws TachyonException if workerId cannot be found
   */
  public void workerRegister(long workerId, List<Long> totalBytesOnTiers,
      List<Long> usedBytesOnTiers, Map<Long, List<Long>> currentBlocksOnTiers)
        throws TachyonException {
    synchronized (mBlocks) {
      synchronized (mWorkers) {
        if (!mWorkers.contains(mIdIndex, workerId)) {
          throw new TachyonException("Could not find worker id: " + workerId + " to register.");
        }
        MasterWorkerInfo workerInfo = mWorkers.getFirstByField(mIdIndex, workerId);
        workerInfo.updateLastUpdatedTimeMs();

        // Gather all blocks on this worker.
        HashSet<Long> blocks = new HashSet<Long>();
        for (List<Long> blockIds : currentBlocksOnTiers.values()) {
          blocks.addAll(blockIds);
        }

        // Detect any lost blocks on this worker.
        Set<Long> removedBlocks = workerInfo.register(totalBytesOnTiers, usedBytesOnTiers, blocks);

        processWorkerRemovedBlocks(workerInfo, removedBlocks);
        processWorkerAddedBlocks(workerInfo, currentBlocksOnTiers);
        LOG.info("registerWorker(): " + workerInfo);
      }
    }
  }

  /**
   * Updates metadata when a worker periodically heartbeats with the master. Called by the worker
   * periodically, via RPC.
   *
   * @param workerId the worker id
   * @param usedBytesOnTiers a list of used bytes on each tier
   * @param removedBlockIds a list of block ids removed from this worker
   * @param addedBlocksOnTiers the added blocks for each storage dir. It maps storage dir id, to a
   *        list of added block for that storage dir.
   * @return an optional command for the worker to execute
   */
  public Command workerHeartbeat(long workerId, List<Long> usedBytesOnTiers,
      List<Long> removedBlockIds, Map<Long, List<Long>> addedBlocksOnTiers) {
    synchronized (mBlocks) {
      synchronized (mWorkers) {
        if (!mWorkers.contains(mIdIndex, workerId)) {
          LOG.warn("Could not find worker id: " + workerId + " for heartbeat.");
          return new Command(CommandType.Register, new ArrayList<Long>());
        }
        MasterWorkerInfo workerInfo = mWorkers.getFirstByField(mIdIndex, workerId);
        processWorkerRemovedBlocks(workerInfo, removedBlockIds);
        processWorkerAddedBlocks(workerInfo, addedBlocksOnTiers);

        workerInfo.updateUsedBytes(usedBytesOnTiers);
        workerInfo.updateLastUpdatedTimeMs();

        List<Long> toRemoveBlocks = workerInfo.getToRemoveBlocks();
        if (toRemoveBlocks.isEmpty()) {
          return new Command(CommandType.Nothing, new ArrayList<Long>());
        }
        return new Command(CommandType.Free, toRemoveBlocks);
      }
    }
  }

  /**
   * Updates the worker and block metadata for blocks removed from a worker.
   *
   * mBlocks should already be locked before calling this method.
   *
   * @param workerInfo The worker metadata object
   * @param removedBlockIds A list of block ids removed from the worker
   */
  private void processWorkerRemovedBlocks(MasterWorkerInfo workerInfo,
      Collection<Long> removedBlockIds) {
    for (long removedBlockId : removedBlockIds) {
      MasterBlockInfo masterBlockInfo = mBlocks.get(removedBlockId);
      if (masterBlockInfo == null) {
        LOG.warn("Worker " + workerInfo.getId() + " removed block " + removedBlockId
            + " but block does not exist.");
        // Continue to remove the remaining blocks.
        continue;
      }
      workerInfo.removeBlock(masterBlockInfo.getBlockId());
      masterBlockInfo.removeWorker(workerInfo.getId());
      if (masterBlockInfo.getNumLocations() == 0) {
        mLostBlocks.add(removedBlockId);
      }
    }
  }

  /**
   * Updates the worker and block metadata for blocks added to a worker.
   *
   * mBlocks should already be locked before calling this method.
   *
   * @param workerInfo The worker metadata object
   * @param addedBlockIds Mapping from StorageDirId to a list of block ids added to the directory.
   */
  private void processWorkerAddedBlocks(MasterWorkerInfo workerInfo,
      Map<Long, List<Long>> addedBlockIds) {
    for (Entry<Long, List<Long>> blockIds : addedBlockIds.entrySet()) {
      long storageDirId = blockIds.getKey();
      for (long blockId : blockIds.getValue()) {
        MasterBlockInfo masterBlockInfo = mBlocks.get(blockId);
        if (masterBlockInfo != null) {
          workerInfo.addBlock(blockId);
          // TODO(gene): Change upper API so that this is tier level or type, not storage dir id.
          int tierAlias = StorageDirId.getStorageLevelAliasValue(storageDirId);
          masterBlockInfo.addWorker(workerInfo.getId(), tierAlias);
          mLostBlocks.remove(blockId);
        } else {
          LOG.warn(
              "failed to register workerId: " + workerInfo.getId() + " to blockId: " + blockId);
        }
      }
    }
  }

  /**
   * Creates a {@link BlockInfo} form a given {@link MasterBlockInfo}, by populating worker
   * locations.
   *
   * mWorkers should already be locked before calling this method.
   *
   * @param masterBlockInfo the {@link MasterBlockInfo}
   * @return a {@link BlockInfo} from a {@link MasterBlockInfo}. Populates worker locations.
   */
  private BlockInfo generateBlockInfo(MasterBlockInfo masterBlockInfo) {
    // "Join" to get all the addresses of the workers.
    List<BlockLocation> locations = new ArrayList<BlockLocation>();
    for (MasterBlockLocation masterBlockLocation : masterBlockInfo.getBlockLocations()) {
      MasterWorkerInfo workerInfo =
          mWorkers.getFirstByField(mIdIndex, masterBlockLocation.getWorkerId());
      if (workerInfo != null) {
        locations.add(new BlockLocation(masterBlockLocation.getWorkerId(),
            workerInfo.getAddress(), masterBlockLocation.getTier()));
      }
    }
    return new BlockInfo(masterBlockInfo.getBlockId(), masterBlockInfo.getLength(), locations);
  }

  /**
   * Lost worker periodic check.
   */
  public final class LostWorkerDetectionHeartbeatExecutor implements HeartbeatExecutor {
    @Override
    public void heartbeat() {
      LOG.debug("System status checking.");
      TachyonConf conf = MasterContext.getConf();

      int masterWorkerTimeoutMs = conf.getInt(Constants.MASTER_WORKER_TIMEOUT_MS);
      synchronized (mWorkers) {
        Iterator<MasterWorkerInfo> iter = mWorkers.iterator();
        while (iter.hasNext()) {
          MasterWorkerInfo worker = iter.next();
          if (CommonUtils.getCurrentMs() - worker.getLastUpdatedTimeMs() > masterWorkerTimeoutMs) {
            LOG.error("The worker " + worker + " got timed out!");
            mLostWorkers.add(worker);
            iter.remove();
          } else if (mLostWorkers.contains(worker)) {
            LOG.info("The lost worker " + worker + " is found.");
            mLostWorkers.remove(worker);
          }
        }
      }

      // restart the failed workers
      if (mLostWorkers.size() != 0) {
        LOG.warn("Restarting failed workers.");
        try {
          String tachyonHome = conf.get(Constants.TACHYON_HOME);
          java.lang.Runtime.getRuntime()
              .exec(tachyonHome + "/bin/tachyon-start.sh restart_workers");
        } catch (IOException e) {
          LOG.error(e.getMessage());
        }
      }
    }
  }
}
