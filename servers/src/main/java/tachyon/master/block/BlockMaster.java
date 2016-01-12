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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import tachyon.Constants;
import tachyon.MasterStorageTierAssoc;
import tachyon.StorageTierAssoc;
import tachyon.collections.IndexedSet;
import tachyon.conf.TachyonConf;
import tachyon.exception.BlockInfoException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.NoWorkerException;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.heartbeat.HeartbeatExecutor;
import tachyon.heartbeat.HeartbeatThread;
import tachyon.master.MasterBase;
import tachyon.master.MasterContext;
import tachyon.master.block.meta.MasterBlockInfo;
import tachyon.master.block.meta.MasterBlockLocation;
import tachyon.master.block.meta.MasterWorkerInfo;
import tachyon.master.journal.Journal;
import tachyon.master.journal.JournalInputStream;
import tachyon.master.journal.JournalOutputStream;
import tachyon.master.journal.JournalProtoUtils;
import tachyon.proto.journal.Block.BlockContainerIdGeneratorEntry;
import tachyon.proto.journal.Block.BlockInfoEntry;
import tachyon.proto.journal.Journal.JournalEntry;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.BlockLocation;
import tachyon.thrift.BlockMasterClientService;
import tachyon.thrift.BlockMasterWorkerService;
import tachyon.thrift.Command;
import tachyon.thrift.CommandType;
import tachyon.thrift.WorkerInfo;
import tachyon.thrift.WorkerNetAddress;
import tachyon.util.CommonUtils;
import tachyon.util.FormatUtils;
import tachyon.util.io.PathUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This master manages the metadata for all the blocks and block workers in Tachyon.
 */
public final class BlockMaster extends MasterBase implements ContainerIdGenerable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  // Block metadata management.
  /**
   * Blocks on all workers, including active and lost blocks. This state must be journaled. Access
   * must be synchronized on mBlocks. If both block and worker metadata must be locked, mBlocks must
   * be locked first.
   */
  private final Map<Long, MasterBlockInfo> mBlocks = new HashMap<Long, MasterBlockInfo>();
  /**
   * Keeps track of block which are no longer in Tachyon storage. Access must be synchronized on
   * mBlocks.
   */
  private final Set<Long> mLostBlocks = new HashSet<Long>();
  /** This state must be journaled. */
  private final BlockContainerIdGenerator mBlockContainerIdGenerator =
      new BlockContainerIdGenerator();

  // Worker metadata management.
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
  /**
   * Mapping between all possible storage level aliases and their ordinal position. This mapping
   * forms a total ordering on all storage level aliases in the system, and must be consistent
   * across masters.
   */
  private StorageTierAssoc mGlobalStorageTierAssoc;

  /**
   * All worker information. Access must be synchronized on mWorkers. If both block and worker
   * metadata must be locked, mBlocks must be locked first.
   */
  // This warning cannot be avoided when passing generics into varargs
  @SuppressWarnings("unchecked")
  private final IndexedSet<MasterWorkerInfo> mWorkers =
      new IndexedSet<MasterWorkerInfo>(mIdIndex, mAddressIndex);
  /**
   * Keeps track of workers which are no longer in communication with the master. Access must be
   * synchronized on {@link #mWorkers}.
   */
  // This warning cannot be avoided when passing generics into varargs
  @SuppressWarnings("unchecked")
  private final IndexedSet<MasterWorkerInfo> mLostWorkers =
      new IndexedSet<MasterWorkerInfo>(mIdIndex, mAddressIndex);
  /**
   * The service that detects lost worker nodes, and tries to restart the failed workers.
   * We store it here so that it can be accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mLostWorkerDetectionService;
  /** The next worker id to use. This state must be journaled. */
  private final AtomicLong mNextWorkerId = new AtomicLong(1);

  /**
   * @param baseDirectory the base journal directory
   * @return the journal directory for this master
   */
  public static String getJournalDirectory(String baseDirectory) {
    return PathUtils.concatPath(baseDirectory, Constants.BLOCK_MASTER_NAME);
  }

  /**
   * Creates a new instance of {@link BlockMaster}.
   *
   * @param journal the journal to use for tracking master operations
   */
  public BlockMaster(Journal journal) {
    super(journal, 2);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<String, TProcessor>();
    services.put(
        Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME,
        new BlockMasterClientService.Processor<BlockMasterClientServiceHandler>(
        new BlockMasterClientServiceHandler(this)));
    services.put(
        Constants.BLOCK_MASTER_WORKER_SERVICE_NAME,
        new BlockMasterWorkerService.Processor<BlockMasterWorkerServiceHandler>(
            new BlockMasterWorkerServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.BLOCK_MASTER_NAME;
  }

  @Override
  public void processJournalCheckpoint(JournalInputStream inputStream) throws IOException {
    // clear state before processing checkpoint.
    mBlocks.clear();
    super.processJournalCheckpoint(inputStream);
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    Message innerEntry = JournalProtoUtils.unwrap(entry);
    // TODO(gene): A better way to process entries besides a huge switch?
    if (innerEntry instanceof BlockContainerIdGeneratorEntry) {
      mBlockContainerIdGenerator
          .setNextContainerId(((BlockContainerIdGeneratorEntry) innerEntry).getNextContainerId());
    } else if (innerEntry instanceof BlockInfoEntry) {
      BlockInfoEntry blockInfoEntry = (BlockInfoEntry) innerEntry;
      mBlocks.put(blockInfoEntry.getBlockId(),
          new MasterBlockInfo(blockInfoEntry.getBlockId(), blockInfoEntry.getLength()));
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
    }
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    outputStream.writeEntry(mBlockContainerIdGenerator.toJournalEntry());
    for (MasterBlockInfo blockInfo : mBlocks.values()) {
      BlockInfoEntry blockInfoEntry = BlockInfoEntry.newBuilder()
          .setBlockId(blockInfo.getBlockId())
          .setLength(blockInfo.getLength())
          .build();
      outputStream.writeEntry(JournalEntry.newBuilder().setBlockInfo(blockInfoEntry).build());
    }
  }

  @Override
  public void start(boolean isLeader) throws IOException {
    super.start(isLeader);
    mGlobalStorageTierAssoc = new MasterStorageTierAssoc(MasterContext.getConf());
    if (isLeader) {
      mLostWorkerDetectionService = getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_LOST_WORKER_DETECTION, new LostWorkerDetectionHeartbeatExecutor(),
          MasterContext.getConf().getInt(Constants.MASTER_HEARTBEAT_INTERVAL_MS)));
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
   * @return the global storage tier mapping
   */
  public StorageTierAssoc getGlobalStorageTierAssoc() {
    return mGlobalStorageTierAssoc;
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
   * @return a set of worker info
   */
  public Set<WorkerInfo> getLostWorkersInfo() {
    synchronized (mWorkers) {
      Set<WorkerInfo> ret = new HashSet<WorkerInfo>(mLostWorkers.size());
      for (MasterWorkerInfo worker : mLostWorkers) {
        ret.add(worker.generateClientWorkerInfo());
      }
      return ret;
    }
  }

  /**
   * Removes blocks from workers. Called by internal masters.
   *
   * @param blockIds a list of block ids to remove from Tachyon space
   */
  public void removeBlocks(List<Long> blockIds) {
    synchronized (mBlocks) {
      synchronized (mWorkers) {
        for (long blockId : blockIds) {
          MasterBlockInfo masterBlockInfo = mBlocks.get(blockId);
          if (masterBlockInfo == null) {
            continue;
          }
          for (long workerId : new ArrayList<Long>(masterBlockInfo.getWorkers())) {
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
   * @return a new block container id. Called by internal masters
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
   * @param tierAlias the alias of the storage tier where the worker is committing the block to
   * @param blockId the committing block id
   * @param length the length of the block
   */
  public void commitBlock(long workerId, long usedBytesOnTier, String tierAlias, long blockId,
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
          BlockInfoEntry blockInfo = BlockInfoEntry.newBuilder()
              .setBlockId(masterBlockInfo.getBlockId())
              .setLength(masterBlockInfo.getLength())
              .build();
          writeJournalEntry(JournalEntry.newBuilder().setBlockInfo(blockInfo).build());
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

        BlockInfoEntry blockInfo = BlockInfoEntry.newBuilder()
            .setBlockId(masterBlockInfo.getBlockId())
            .setLength(masterBlockInfo.getLength())
            .build();
        writeJournalEntry(JournalEntry.newBuilder().setBlockInfo(blockInfo).build());
        flushJournal();
      }
    }
  }

  /**
   * @param blockId the block id to get information for
   * @return the {@link BlockInfo} for the given block id. Called via RPC
   * @throws BlockInfoException if the block info is not found
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
   *         list is in the same order as the input list
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
   * @return the total bytes on each storage tier. Called by internal web ui
   */
  public Map<String, Long> getTotalBytesOnTiers() {
    Map<String, Long> ret = new HashMap<String, Long>();
    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers) {
        for (Map.Entry<String, Long> entry : worker.getTotalBytesOnTiers().entrySet()) {
          Long total = ret.get(entry.getKey());
          ret.put(entry.getKey(), (total == null ? 0L : total) + entry.getValue());
        }
      }
    }
    return ret;
  }

  /**
   * @return the used bytes on each storage tier. Called by internal web ui
   */
  public Map<String, Long> getUsedBytesOnTiers() {
    Map<String, Long> ret = new HashMap<String, Long>();
    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers) {
        for (Map.Entry<String, Long> entry : worker.getUsedBytesOnTiers().entrySet()) {
          Long used = ret.get(entry.getKey());
          ret.put(entry.getKey(), (used == null ? 0L : used) + entry.getValue());
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
  public long getWorkerId(WorkerNetAddress workerNetAddress) {
    // TODO(gene): This NetAddress cloned in case thrift re-uses the object. Does thrift re-use it?
    WorkerNetAddress workerAddress = new WorkerNetAddress(workerNetAddress);

    synchronized (mWorkers) {
      if (mWorkers.contains(mAddressIndex, workerAddress)) {
        // This worker address is already mapped to a worker id.
        long oldWorkerId = mWorkers.getFirstByField(mAddressIndex, workerAddress).getId();
        LOG.warn("The worker {} already exists as id {}.", workerAddress, oldWorkerId);
        return oldWorkerId;
      }

      if (mLostWorkers.contains(mAddressIndex, workerAddress)) { // this is one of the lost workers
        final MasterWorkerInfo lostWorkerInfo =
            mLostWorkers.getFirstByField(mAddressIndex, workerAddress);
        final long lostWorkerId = lostWorkerInfo.getId();
        LOG.warn("A lost worker {} has requested its old id {}.", workerAddress, lostWorkerId);

        // Update the timestamp of the worker before it is considered an active worker.
        lostWorkerInfo.updateLastUpdatedTimeMs();
        mWorkers.add(lostWorkerInfo);
        mLostWorkers.remove(lostWorkerInfo);
        return lostWorkerId;
      }

      // Generate a new worker id.
      long workerId = mNextWorkerId.getAndIncrement();
      mWorkers.add(new MasterWorkerInfo(workerId, workerNetAddress));

      LOG.info("getWorkerId(): WorkerNetAddress: {} id: {}", workerAddress, workerId);
      return workerId;
    }
  }

  /**
   * Updates metadata when a worker registers with the master. Called by workers via RPC.
   *
   * @param workerId the worker id of the worker registering
   * @param storageTiers a list of storage tier aliases in order of their position in the worker's
   *        hierarchy
   * @param totalBytesOnTiers a mapping from storage tier alias to total bytes
   * @param usedBytesOnTiers a mapping from storage tier alias to the used byes
   * @param currentBlocksOnTiers a mapping from storage tier alias to a list of blocks
   * @throws NoWorkerException if workerId cannot be found
   */
  public void workerRegister(long workerId, List<String> storageTiers,
      Map<String, Long> totalBytesOnTiers, Map<String, Long> usedBytesOnTiers,
      Map<String, List<Long>> currentBlocksOnTiers) throws NoWorkerException {
    synchronized (mBlocks) {
      synchronized (mWorkers) {
        if (!mWorkers.contains(mIdIndex, workerId)) {
          throw new NoWorkerException("Could not find worker id: " + workerId + " to register.");
        }
        MasterWorkerInfo workerInfo = mWorkers.getFirstByField(mIdIndex, workerId);
        workerInfo.updateLastUpdatedTimeMs();

        // Gather all blocks on this worker.
        HashSet<Long> blocks = new HashSet<Long>();
        for (List<Long> blockIds : currentBlocksOnTiers.values()) {
          blocks.addAll(blockIds);
        }

        // Detect any lost blocks on this worker.
        Set<Long> removedBlocks = workerInfo.register(mGlobalStorageTierAssoc, storageTiers,
            totalBytesOnTiers, usedBytesOnTiers, blocks);

        processWorkerRemovedBlocks(workerInfo, removedBlocks);
        processWorkerAddedBlocks(workerInfo, currentBlocksOnTiers);
        LOG.info("registerWorker(): {}", workerInfo);
      }
    }
  }

  /**
   * Updates metadata when a worker periodically heartbeats with the master. Called by the worker
   * periodically, via RPC.
   *
   * @param workerId the worker id
   * @param usedBytesOnTiers a mapping from tier alias to the used bytes
   * @param removedBlockIds a list of block ids removed from this worker
   * @param addedBlocksOnTiers a mapping from tier alias to the added blocks
   * @return an optional command for the worker to execute
   */
  public Command workerHeartbeat(long workerId, Map<String, Long> usedBytesOnTiers,
      List<Long> removedBlockIds, Map<String, List<Long>> addedBlocksOnTiers) {
    synchronized (mBlocks) {
      synchronized (mWorkers) {
        if (!mWorkers.contains(mIdIndex, workerId)) {
          LOG.warn("Could not find worker id: {} for heartbeat.", workerId);
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
        LOG.warn("Worker {} removed block {} but block does not exist.", workerInfo.getId(),
            removedBlockId);
        // Continue to remove the remaining blocks.
        continue;
      }
      LOG.info("Block {} is removed on worker {}.", removedBlockId, workerInfo.getId());
      workerInfo.removeBlock(masterBlockInfo.getBlockId());
      masterBlockInfo.removeWorker(workerInfo.getId());
      if (masterBlockInfo.getNumLocations() == 0) {
        mLostBlocks.add(removedBlockId);
      }
    }
  }

  /**
   * Called by the heartbeat thread whenever a worker is lost.
   *
   * @param latest the latest {@link MasterWorkerInfo} available at the time of worker loss
   */
  // Synchronized on mBlocks by the caller
  private void processLostWorker(MasterWorkerInfo latest) {
    final Set<Long> lostBlocks = latest.getBlocks();
    processWorkerRemovedBlocks(latest, lostBlocks);
  }

  /**
   * Updates the worker and block metadata for blocks added to a worker.
   *
   * {@link #mBlocks} should already be locked before calling this method.
   *
   * @param workerInfo The worker metadata object
   * @param addedBlockIds A mapping from storage tier alias to a list of block ids added
   */
  private void processWorkerAddedBlocks(MasterWorkerInfo workerInfo,
      Map<String, List<Long>> addedBlockIds) {
    for (Map.Entry<String, List<Long>> entry : addedBlockIds.entrySet()) {
      for (long blockId : entry.getValue()) {
        MasterBlockInfo masterBlockInfo = mBlocks.get(blockId);
        if (masterBlockInfo != null) {
          workerInfo.addBlock(blockId);
          masterBlockInfo.addWorker(workerInfo.getId(), entry.getKey());
          mLostBlocks.remove(blockId);
        } else {
          LOG.warn("Failed to register workerId: {} to blockId: {}", workerInfo.getId(), blockId);
        }
      }
    }
  }

  /**
   * @return the lost blocks in Tachyon Storage
   */
  public Set<Long> getLostBlocks() {
    synchronized (mBlocks) {
      return ImmutableSet.copyOf(mLostBlocks);
    }
  }

  /**
   * Creates a {@link BlockInfo} form a given {@link MasterBlockInfo}, by populating worker
   * locations.
   *
   * {@link #mWorkers} should already be locked before calling this method.
   *
   * @param masterBlockInfo the {@link MasterBlockInfo}
   * @return a {@link BlockInfo} from a {@link MasterBlockInfo}. Populates worker locations
   */
  private BlockInfo generateBlockInfo(MasterBlockInfo masterBlockInfo) {
    // "Join" to get all the addresses of the workers.
    List<BlockLocation> ret = new ArrayList<BlockLocation>();
    List<MasterBlockLocation> blockLocations = masterBlockInfo.getBlockLocations();
    // Sort the block locations by their alias ordinal in the master storage tier mapping
    Collections.sort(blockLocations, new Comparator<MasterBlockLocation>() {
      @Override
      public int compare(MasterBlockLocation o1, MasterBlockLocation o2) {
        return mGlobalStorageTierAssoc.getOrdinal(o1.getTierAlias())
            - mGlobalStorageTierAssoc.getOrdinal(o2.getTierAlias());
      }
    });
    for (MasterBlockLocation masterBlockLocation : blockLocations) {
      MasterWorkerInfo workerInfo =
          mWorkers.getFirstByField(mIdIndex, masterBlockLocation.getWorkerId());
      if (workerInfo != null) {
        ret.add(new BlockLocation(masterBlockLocation.getWorkerId(), workerInfo.getAddress(),
            masterBlockLocation.getTierAlias()));
      }
    }
    return new BlockInfo(masterBlockInfo.getBlockId(), masterBlockInfo.getLength(), ret);
  }

  /**
   * Reports the ids of the blocks lost on workers.
   *
   * @param blockIds the ids of the lost blocks
   */
  public void reportLostBlocks(List<Long> blockIds) {
    synchronized (mLostBlocks) {
      mLostBlocks.addAll(blockIds);
    }
  }

  /**
   * Lost worker periodic check.
   */
  private final class LostWorkerDetectionHeartbeatExecutor implements HeartbeatExecutor {
    @Override
    public void heartbeat() {
      LOG.debug("System status checking.");
      TachyonConf conf = MasterContext.getConf();

      int masterWorkerTimeoutMs = conf.getInt(Constants.MASTER_WORKER_TIMEOUT_MS);
      synchronized (mBlocks) {
        synchronized (mWorkers) {
          Iterator<MasterWorkerInfo> iter = mWorkers.iterator();
          while (iter.hasNext()) {
            MasterWorkerInfo worker = iter.next();
            final long lastUpdate = CommonUtils.getCurrentMs() - worker.getLastUpdatedTimeMs();
            if (lastUpdate > masterWorkerTimeoutMs) {
              LOG.error("The worker {} got timed out!", worker);
              mLostWorkers.add(worker);
              iter.remove();
              processLostWorker(worker);
            }
          }
        }
      }
    }
  }
}
