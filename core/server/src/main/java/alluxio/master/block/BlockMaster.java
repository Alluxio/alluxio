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

package alluxio.master.block;

import alluxio.Constants;
import alluxio.MasterStorageTierAssoc;
import alluxio.StorageTierAssoc;
import alluxio.collections.ConcurrentHashSet;
import alluxio.collections.IndexedSet;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.NoWorkerException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.AbstractMaster;
import alluxio.master.MasterContext;
import alluxio.master.block.meta.MasterBlockInfo;
import alluxio.master.block.meta.MasterBlockLocation;
import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.master.journal.AsyncJournalWriter;
import alluxio.master.journal.Journal;
import alluxio.master.journal.JournalInputStream;
import alluxio.master.journal.JournalOutputStream;
import alluxio.master.journal.JournalProtoUtils;
import alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry;
import alluxio.proto.journal.Block.BlockInfoEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.thrift.BlockMasterClientService;
import alluxio.thrift.BlockMasterWorkerService;
import alluxio.thrift.Command;
import alluxio.thrift.CommandType;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This master manages the metadata for all the blocks and block workers in Alluxio.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class BlockMaster extends AbstractMaster implements ContainerIdGenerable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  /**
   * The number of container ids to 'reserve' before having to journal container id state. This
   * allows the master to return container ids within the reservation, without having to write to
   * the journal.
   */
  private static final long CONTAINER_ID_RESERVATION_SIZE = 1000;

  /**
   * Concurrency and locking in the BlockMaster
   *
   * The block master uses concurrent data structures, to allow non-conflicting concurrent access.
   * This means each piece of metadata should be locked individually. There are two types of
   * metadata in the {@link BlockMaster}; {@link MasterBlockInfo} and {@link MasterWorkerInfo}.
   * Individual objects must be locked before modifying the object, or reading a modifiable field
   * of an object. This will protect the internal integrity of the metadata object.
   *
   * Lock ordering must be preserved in order to prevent deadlock. If both a worker and block
   * metadata must be locked at the same time, the worker metadata ({@link MasterWorkerInfo})
   * must be locked before the block metadata ({@link MasterBlockInfo}).
   *
   * It should not be the case that multiple worker metadata must be locked at the same time, or
   * multiple block metadata must be locked at the same time. Operations involving multiple
   * workers or multiple blocks should be able to be performed independently.
   */

  // Block metadata management.
  /** Blocks on all workers, including active and lost blocks. This state must be journaled. */
  private final ConcurrentHashMap<Long, MasterBlockInfo>
      mBlocks = new ConcurrentHashMap<>(8192, 0.90f, 64);
  /** Keeps track of block which are no longer in Alluxio storage. */
  private final ConcurrentHashSet<Long> mLostBlocks = new ConcurrentHashSet<>(64, 0.90f, 64);

  /** This state must be journaled. */
  @GuardedBy("itself")
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
          return o.getWorkerAddress();
        }
      };

  /**
   * Mapping between all possible storage level aliases and their ordinal position. This mapping
   * forms a total ordering on all storage level aliases in the system, and must be consistent
   * across masters.
   */
  private StorageTierAssoc mGlobalStorageTierAssoc;

  /** All worker information. */
  private final IndexedSet<MasterWorkerInfo> mWorkers =
      new IndexedSet<>(mIdIndex, mAddressIndex);
  /** Keeps track of workers which are no longer in communication with the master. */
  private final IndexedSet<MasterWorkerInfo> mLostWorkers =
      new IndexedSet<>(mIdIndex, mAddressIndex);

  /**
   * The service that detects lost worker nodes, and tries to restart the failed workers.
   * We store it here so that it can be accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mLostWorkerDetectionService;

  /** The next worker id to use. This state must be journaled. */
  private final AtomicLong mNextWorkerId = new AtomicLong(1);

  /** The value of the 'next container id' last journaled. */
  @GuardedBy("mBlockContainerIdGenerator")
  private long mJournaledNextContainerId = 0;

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
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.BLOCK_MASTER_CLIENT_SERVICE_NAME,
        new BlockMasterClientService.Processor<>(new BlockMasterClientServiceHandler(this)));
    services.put(Constants.BLOCK_MASTER_WORKER_SERVICE_NAME,
        new BlockMasterWorkerService.Processor<>(new BlockMasterWorkerServiceHandler(this)));
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
      mJournaledNextContainerId =
          ((BlockContainerIdGeneratorEntry) innerEntry).getNextContainerId();
      mBlockContainerIdGenerator.setNextContainerId((mJournaledNextContainerId));
    } else if (innerEntry instanceof BlockInfoEntry) {
      BlockInfoEntry blockInfoEntry = (BlockInfoEntry) innerEntry;
      if (mBlocks.containsKey(blockInfoEntry.getBlockId())) {
        // Update the existing block info.
        MasterBlockInfo blockInfo = mBlocks.get(blockInfoEntry.getBlockId());
        blockInfo.updateLength(blockInfoEntry.getLength());
      } else {
        mBlocks.put(blockInfoEntry.getBlockId(), new MasterBlockInfo(blockInfoEntry.getBlockId(),
            blockInfoEntry.getLength()));
      }
    } else {
      throw new IOException(ExceptionMessage.UNEXPECTED_JOURNAL_ENTRY.getMessage(entry));
    }
  }

  @Override
  public void streamToJournalCheckpoint(JournalOutputStream outputStream) throws IOException {
    outputStream.writeEntry(getContainerIdJournalEntry());
    for (MasterBlockInfo blockInfo : mBlocks.values()) {
      BlockInfoEntry blockInfoEntry =
          BlockInfoEntry.newBuilder().setBlockId(blockInfo.getBlockId())
              .setLength(blockInfo.getLength()).build();
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
    return mWorkers.size();
  }

  /**
   * @return a list of {@link WorkerInfo} objects representing the workers in Alluxio
   */
  public List<WorkerInfo> getWorkerInfoList() {
    List<WorkerInfo> workerInfoList = new ArrayList<>(mWorkers.size());
    for (MasterWorkerInfo worker : mWorkers) {
      synchronized (worker) {
        workerInfoList.add(worker.generateClientWorkerInfo());
      }
    }
    return workerInfoList;
  }

  /**
   * @return the total capacity (in bytes) on all tiers, on all workers of Alluxio
   */
  public long getCapacityBytes() {
    long ret = 0;
    for (MasterWorkerInfo worker : mWorkers) {
      synchronized (worker) {
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
   * @return the total used bytes on all tiers, on all workers of Alluxio
   */
  public long getUsedBytes() {
    long ret = 0;
    for (MasterWorkerInfo worker : mWorkers) {
      synchronized (worker) {
        ret += worker.getUsedBytes();
      }
    }
    return ret;
  }

  /**
   * Gets info about the lost workers.
   *
   * @return a set of worker info
   */
  public Set<WorkerInfo> getLostWorkersInfo() {
    Set<WorkerInfo> ret = new HashSet<>(mLostWorkers.size());
    for (MasterWorkerInfo worker : mLostWorkers) {
      synchronized (worker) {
        ret.add(worker.generateClientWorkerInfo());
      }
    }
    return ret;
  }

  /**
   * Removes blocks from workers.
   *
   * @param blockIds a list of block ids to remove from Alluxio space
   * @param delete whether to delete blocks metadata in Master
   */
  public void removeBlocks(List<Long> blockIds, boolean delete) {
    for (long blockId : blockIds) {
      MasterBlockInfo block = mBlocks.get(blockId);
      if (block == null) {
        continue;
      }
      HashSet<Long> workerIds = new HashSet<>();
      synchronized (block) {
        // Technically, 'block' should be confirmed to still be in the data structure. A
        // concurrent removeBlock call can remove it. However, we are intentionally ignoring this
        // race, since deleting the same block again is a noop.
        workerIds.addAll(block.getWorkers());
        // Two cases here:
        // 1) For delete: delete the block metadata.
        // 2) For free: keep the block metadata. mLostBlocks will be changed in
        // processWorkerRemovedBlocks
        if (delete) {
          // Make sure blockId is removed from mLostBlocks when the block metadata is deleted.
          // Otherwise blockId in mLostBlock can be dangling index if the metadata is gone.
          mLostBlocks.remove(blockId);
          mBlocks.remove(blockId);
        }
      }

      // Outside of locking the block. This does not have to be synchronized with the block
      // metadata, since it is essentially an asynchronous signal to the worker to remove the block.
      for (long workerId : workerIds) {
        MasterWorkerInfo worker = mWorkers.getFirstByField(mIdIndex, workerId);
        if (worker != null) {
          synchronized (worker) {
            worker.updateToRemovedBlock(true, blockId);
          }
        }
      }
    }
  }

  /**
   * @return a new block container id
   */
  @Override
  public long getNewContainerId() {
    synchronized (mBlockContainerIdGenerator) {
      long containerId = mBlockContainerIdGenerator.getNewContainerId();
      if (containerId < mJournaledNextContainerId) {
        // This container id is within the reserved container ids, so it is safe to return the id
        // without having to write anything to the journal.
        return containerId;
      }
      // This container id is not safe with respect to the last journaled container id.
      // Therefore, journal the new state of the container id. This implies that when a master
      // crashes, the container ids within the reservation which have not been used yet will
      // never be used. This is a tradeoff between fully utilizing the container id space, vs.
      // improving master scalability.
      // TODO(gpang): investigate if dynamic reservation sizes could be effective

      // Set the next id to journal with a reservation of container ids, to avoid having to write
      // to the journal for ids within the reservation.
      mJournaledNextContainerId = containerId + CONTAINER_ID_RESERVATION_SIZE;
      long counter = appendJournalEntry(getContainerIdJournalEntry());
      // This must be flushed while holding the lock on mBlockContainerIdGenerator, in order to
      // prevent subsequent calls to return container ids that have not been journaled and flushed.
      waitForJournalFlush(counter);
      return containerId;
    }
  }

  /**
   * @return a {@link JournalEntry} representing the state of the container id generator
   */
  private JournalEntry getContainerIdJournalEntry() {
    BlockContainerIdGeneratorEntry blockContainerIdGenerator =
        BlockContainerIdGeneratorEntry.newBuilder().setNextContainerId(mJournaledNextContainerId)
            .build();
    return JournalEntry.newBuilder().setBlockContainerIdGenerator(blockContainerIdGenerator)
        .build();
  }

  /**
   * Marks a block as committed on a specific worker.
   *
   * @param workerId the worker id committing the block
   * @param usedBytesOnTier the updated used bytes on the tier of the worker
   * @param tierAlias the alias of the storage tier where the worker is committing the block to
   * @param blockId the committing block id
   * @param length the length of the block
   * @throws NoWorkerException if the workerId is not active
   */
  // TODO(binfan): check the logic is correct or not when commitBlock is a retry
  public void commitBlock(long workerId, long usedBytesOnTier, String tierAlias, long blockId,
      long length) throws NoWorkerException {
    LOG.debug("Commit block from workerId: {}, usedBytesOnTier: {}, blockId: {}, length: {}",
        workerId, usedBytesOnTier, blockId, length);

    long counter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;

    MasterWorkerInfo worker = mWorkers.getFirstByField(mIdIndex, workerId);
    // TODO(peis): Check lost workers as well.
    if (worker == null) {
      throw new NoWorkerException(ExceptionMessage.NO_WORKER_FOUND.getMessage(workerId));
    }

    // Lock the worker metadata first.
    synchronized (worker) {
      // Loop until block metadata is successfully locked.
      for (;;) {
        boolean newBlock = false;
        MasterBlockInfo block = mBlocks.get(blockId);
        if (block == null) {
          // The block metadata doesn't exist yet.
          block = new MasterBlockInfo(blockId, length);
          newBlock = true;
        }

        // Lock the block metadata.
        synchronized (block) {
          boolean writeJournal = false;
          if (newBlock) {
            if (mBlocks.putIfAbsent(blockId, block) != null) {
              // Another thread already inserted the metadata for this block, so start loop over.
              continue;
            }
            // Successfully added the new block metadata. Append a journal entry for the new
            // metadata.
            writeJournal = true;
          } else if (block.getLength() != length
              && block.getLength() == Constants.UNKNOWN_SIZE) {
            // The block size was previously unknown. Update the block size with the committed
            // size, and append a journal entry.
            block.updateLength(length);
            writeJournal = true;
          }
          if (writeJournal) {
            BlockInfoEntry blockInfo =
                BlockInfoEntry.newBuilder().setBlockId(blockId).setLength(length).build();
            counter = appendJournalEntry(JournalEntry.newBuilder().setBlockInfo(blockInfo).build());
          }
          // At this point, both the worker and the block metadata are locked.

          // Update the block metadata with the new worker location.
          block.addWorker(workerId, tierAlias);
          // This worker has this block, so it is no longer lost.
          mLostBlocks.remove(blockId);

          // Update the worker information for this new block.
          // TODO(binfan): when retry commitBlock on master is expected, make sure metrics are not
          // double counted.
          worker.addBlock(blockId);
          worker.updateUsedBytes(tierAlias, usedBytesOnTier);
          worker.updateLastUpdatedTimeMs();
        }
        break;
      }
    }

    waitForJournalFlush(counter);
  }

  /**
   * Marks a block as committed, but without a worker location. This means the block is only in ufs.
   *
   * @param blockId the id of the block to commit
   * @param length the length of the block
   */
  public void commitBlockInUFS(long blockId, long length) {
    LOG.debug("Commit block in ufs. blockId: {}, length: {}", blockId, length);
    if (mBlocks.get(blockId) != null) {
      // Block metadata already exists, so do not need to create a new one.
      return;
    }

    // The block has not been committed previously, so add the metadata to commit the block.
    MasterBlockInfo block = new MasterBlockInfo(blockId, length);
    long counter = AsyncJournalWriter.INVALID_FLUSH_COUNTER;
    synchronized (block) {
      if (mBlocks.putIfAbsent(blockId, block) == null) {
        // Successfully added the new block metadata. Append a journal entry for the new metadata.
        BlockInfoEntry blockInfo =
            BlockInfoEntry.newBuilder().setBlockId(blockId).setLength(length).build();
        counter = appendJournalEntry(JournalEntry.newBuilder().setBlockInfo(blockInfo).build());
      }
    }
    waitForJournalFlush(counter);
  }

  /**
   * @param blockId the block id to get information for
   * @return the {@link BlockInfo} for the given block id
   * @throws BlockInfoException if the block info is not found
   */
  public BlockInfo getBlockInfo(long blockId) throws BlockInfoException {
    MasterBlockInfo block = mBlocks.get(blockId);
    if (block == null) {
      throw new BlockInfoException(ExceptionMessage.BLOCK_META_NOT_FOUND, blockId);
    }
    synchronized (block) {
      return generateBlockInfo(block);
    }
  }

  /**
   * Retrieves information for the given list of block ids.
   *
   * @param blockIds A list of block ids to retrieve the information for
   * @return A list of {@link BlockInfo} objects corresponding to the input list of block ids. The
   *         list is in the same order as the input list
   */
  public List<BlockInfo> getBlockInfoList(List<Long> blockIds) {
    List<BlockInfo> ret = new ArrayList<>(blockIds.size());
    for (long blockId : blockIds) {
      MasterBlockInfo block = mBlocks.get(blockId);
      if (block == null) {
        continue;
      }
      synchronized (block) {
        ret.add(generateBlockInfo(block));
      }
    }
    return ret;
  }

  /**
   * @return the total bytes on each storage tier
   */
  public Map<String, Long> getTotalBytesOnTiers() {
    Map<String, Long> ret = new HashMap<>();
    for (MasterWorkerInfo worker : mWorkers) {
      synchronized (worker) {
        for (Map.Entry<String, Long> entry : worker.getTotalBytesOnTiers().entrySet()) {
          Long total = ret.get(entry.getKey());
          ret.put(entry.getKey(), (total == null ? 0L : total) + entry.getValue());
        }
      }
    }
    return ret;
  }

  /**
   * @return the used bytes on each storage tier
   */
  public Map<String, Long> getUsedBytesOnTiers() {
    Map<String, Long> ret = new HashMap<>();
    for (MasterWorkerInfo worker : mWorkers) {
      synchronized (worker) {
        for (Map.Entry<String, Long> entry : worker.getUsedBytesOnTiers().entrySet()) {
          Long used = ret.get(entry.getKey());
          ret.put(entry.getKey(), (used == null ? 0L : used) + entry.getValue());
        }
      }
    }
    return ret;
  }

  /**
   * Returns a worker id for the given worker.
   *
   * @param workerNetAddress the worker {@link WorkerNetAddress}
   * @return the worker id for this worker
   */
  public long getWorkerId(WorkerNetAddress workerNetAddress) {
    // TODO(gpang): This NetAddress cloned in case thrift re-uses the object. Does thrift re-use it?
    MasterWorkerInfo existingWorker = mWorkers.getFirstByField(mAddressIndex, workerNetAddress);
    if (existingWorker != null) {
      // This worker address is already mapped to a worker id.
      long oldWorkerId = existingWorker.getId();
      LOG.warn("The worker {} already exists as id {}.", workerNetAddress, oldWorkerId);
      return oldWorkerId;
    }

    MasterWorkerInfo lostWorker = mLostWorkers.getFirstByField(mAddressIndex, workerNetAddress);
    if (lostWorker != null) {
      // this is one of the lost workers
      synchronized (lostWorker) {
        final long lostWorkerId = lostWorker.getId();
        LOG.warn("A lost worker {} has requested its old id {}.", workerNetAddress, lostWorkerId);

        // Update the timestamp of the worker before it is considered an active worker.
        lostWorker.updateLastUpdatedTimeMs();
        mWorkers.add(lostWorker);
        mLostWorkers.remove(lostWorker);
        return lostWorkerId;
      }
    }

    // Generate a new worker id.
    long workerId = mNextWorkerId.getAndIncrement();
    mWorkers.add(new MasterWorkerInfo(workerId, workerNetAddress));

    LOG.info("getWorkerId(): WorkerNetAddress: {} id: {}", workerNetAddress, workerId);
    return workerId;
  }

  /**
   * Updates metadata when a worker registers with the master.
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
    MasterWorkerInfo worker = mWorkers.getFirstByField(mIdIndex, workerId);
    if (worker == null) {
      throw new NoWorkerException(ExceptionMessage.NO_WORKER_FOUND.getMessage(workerId));
    }

    // Gather all blocks on this worker.
    HashSet<Long> blocks = new HashSet<>();
    for (List<Long> blockIds : currentBlocksOnTiers.values()) {
      blocks.addAll(blockIds);
    }

    synchronized (worker) {
      worker.updateLastUpdatedTimeMs();
      // Detect any lost blocks on this worker.
      Set<Long> removedBlocks = worker.register(mGlobalStorageTierAssoc, storageTiers,
          totalBytesOnTiers, usedBytesOnTiers, blocks);
      processWorkerRemovedBlocks(worker, removedBlocks);
      processWorkerAddedBlocks(worker, currentBlocksOnTiers);
    }

    LOG.info("registerWorker(): {}", worker);
  }

  /**
   * Updates metadata when a worker periodically heartbeats with the master.
   *
   * @param workerId the worker id
   * @param usedBytesOnTiers a mapping from tier alias to the used bytes
   * @param removedBlockIds a list of block ids removed from this worker
   * @param addedBlocksOnTiers a mapping from tier alias to the added blocks
   * @return an optional command for the worker to execute
   */
  public Command workerHeartbeat(long workerId, Map<String, Long> usedBytesOnTiers,
      List<Long> removedBlockIds, Map<String, List<Long>> addedBlocksOnTiers) {
    MasterWorkerInfo worker = mWorkers.getFirstByField(mIdIndex, workerId);
    if (worker == null) {
      LOG.warn("Could not find worker id: {} for heartbeat.", workerId);
      return new Command(CommandType.Register, new ArrayList<Long>());
    }

    synchronized (worker) {
      // Technically, 'worker' should be confirmed to still be in the data structure. Lost worker
      // detection can remove it. However, we are intentionally ignoring this race, since the worker
      // will just re-register regardless.
      processWorkerRemovedBlocks(worker, removedBlockIds);
      processWorkerAddedBlocks(worker, addedBlocksOnTiers);

      worker.updateUsedBytes(usedBytesOnTiers);
      worker.updateLastUpdatedTimeMs();

      List<Long> toRemoveBlocks = worker.getToRemoveBlocks();
      if (toRemoveBlocks.isEmpty()) {
        return new Command(CommandType.Nothing, new ArrayList<Long>());
      }
      return new Command(CommandType.Free, toRemoveBlocks);
    }
  }

  /**
   * Updates the worker and block metadata for blocks removed from a worker.
   *
   * @param workerInfo The worker metadata object
   * @param removedBlockIds A list of block ids removed from the worker
   */
  @GuardedBy("workerInfo")
  private void processWorkerRemovedBlocks(MasterWorkerInfo workerInfo,
      Collection<Long> removedBlockIds) {
    for (long removedBlockId : removedBlockIds) {
      MasterBlockInfo block = mBlocks.get(removedBlockId);
      // TODO(calvin): Investigate if this branching logic can be simplified.
      if (block == null) {
        // LOG.warn("Worker {} informs the removed block {}, but block metadata does not exist"
        //    + " on Master!", workerInfo.getId(), removedBlockId);
        // TODO(pfxuan): [ALLUXIO-1804] should find a better way to handle the removed blocks.
        // Ideally, the delete/free I/O flow should never reach this point. Because Master may
        // update the block metadata only after receiving the acknowledgement from Workers.
        workerInfo.removeBlock(removedBlockId);
        // Continue to remove the remaining blocks.
        continue;
      }
      synchronized (block) {
        LOG.info("Block {} is removed on worker {}.", removedBlockId, workerInfo.getId());
        workerInfo.removeBlock(block.getBlockId());
        block.removeWorker(workerInfo.getId());
        if (block.getNumLocations() == 0) {
          mLostBlocks.add(removedBlockId);
        }
      }
    }
  }

  /**
   * Updates the worker and block metadata for blocks added to a worker.
   *
   * @param workerInfo The worker metadata object
   * @param addedBlockIds A mapping from storage tier alias to a list of block ids added
   */
  @GuardedBy("workerInfo")
  private void processWorkerAddedBlocks(MasterWorkerInfo workerInfo,
      Map<String, List<Long>> addedBlockIds) {
    for (Map.Entry<String, List<Long>> entry : addedBlockIds.entrySet()) {
      for (long blockId : entry.getValue()) {
        MasterBlockInfo block = mBlocks.get(blockId);
        if (block != null) {
          synchronized (block) {
            workerInfo.addBlock(blockId);
            block.addWorker(workerInfo.getId(), entry.getKey());
            mLostBlocks.remove(blockId);
          }
        } else {
          LOG.warn("Failed to register workerId: {} to blockId: {}", workerInfo.getId(), blockId);
        }
      }
    }
  }

  /**
   * @return the lost blocks in Alluxio Storage
   */
  public Set<Long> getLostBlocks() {
    return ImmutableSet.copyOf(mLostBlocks);
  }

  /**
   * Creates a {@link BlockInfo} form a given {@link MasterBlockInfo}, by populating worker
   * locations.
   *
   * @param masterBlockInfo the {@link MasterBlockInfo}
   * @return a {@link BlockInfo} from a {@link MasterBlockInfo}. Populates worker locations
   */
  @GuardedBy("masterBlockInfo")
  private BlockInfo generateBlockInfo(MasterBlockInfo masterBlockInfo) {
    // "Join" to get all the addresses of the workers.
    List<BlockLocation> locations = new ArrayList<>();
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
        // worker metadata is intentionally not locked here because:
        // - it would be an incorrect order (correct order is lock worker first, then block)
        // - only uses getters of final variables
        locations.add(new BlockLocation().setWorkerId(masterBlockLocation.getWorkerId())
            .setWorkerAddress(workerInfo.getWorkerAddress())
            .setTierAlias(masterBlockLocation.getTierAlias()));
      }
    }
    return new BlockInfo().setBlockId(masterBlockInfo.getBlockId())
        .setLength(masterBlockInfo.getLength()).setLocations(locations);
  }

  /**
   * Reports the ids of the blocks lost on workers.
   *
   * @param blockIds the ids of the lost blocks
   */
  public void reportLostBlocks(List<Long> blockIds) {
    mLostBlocks.addAll(blockIds);
  }

  /**
   * Lost worker periodic check.
   */
  private final class LostWorkerDetectionHeartbeatExecutor implements HeartbeatExecutor {

    /**
     * Constructs a new {@link LostWorkerDetectionHeartbeatExecutor}.
     */
    public LostWorkerDetectionHeartbeatExecutor() {}

    @Override
    public void heartbeat() {
      int masterWorkerTimeoutMs =
          MasterContext.getConf().getInt(Constants.MASTER_WORKER_TIMEOUT_MS);
      for (MasterWorkerInfo worker : mWorkers) {
        synchronized (worker) {
          final long lastUpdate = CommonUtils.getCurrentMs() - worker.getLastUpdatedTimeMs();
          if (lastUpdate > masterWorkerTimeoutMs) {
            LOG.error("The worker {} timed out after {}ms without a heartbeat!", worker,
                lastUpdate);
            mLostWorkers.add(worker);
            mWorkers.remove(worker);
            processWorkerRemovedBlocks(worker, worker.getBlocks());
          }
        }
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }
}
