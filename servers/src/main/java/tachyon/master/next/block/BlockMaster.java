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

package tachyon.master.next.block;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.StorageDirId;
import tachyon.master.next.IndexedSet;
import tachyon.master.next.MasterBase;
import tachyon.master.next.block.journal.BlockIdGeneratorEntry;
import tachyon.master.next.block.meta.MasterBlockInfo;
import tachyon.master.next.block.meta.MasterBlockLocation;
import tachyon.master.next.block.meta.MasterWorkerInfo;
import tachyon.master.next.journal.Journal;
import tachyon.master.next.journal.JournalEntry;
import tachyon.master.next.journal.JournalInputStream;
import tachyon.master.next.journal.JournalOutputStream;
import tachyon.thrift.BlockInfo;
import tachyon.thrift.BlockLocation;
import tachyon.thrift.BlockMasterService;
import tachyon.thrift.Command;
import tachyon.thrift.CommandType;
import tachyon.thrift.NetAddress;
import tachyon.thrift.WorkerInfo;
import tachyon.util.FormatUtils;

public class BlockMaster extends MasterBase implements ContainerIdGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  // Block metadata management.
  private final Map<Long, MasterBlockInfo> mBlocks;
  private final BlockIdGenerator mBlockIdGenerator;
  private final Set<Long> mLostBlocks;

  // Worker metadata management.
  private final IndexedSet.FieldIndex mIdIndex = new IndexedSet.FieldIndex<MasterWorkerInfo>() {
    @Override
    public Object getFieldValue(MasterWorkerInfo o) {
      return o.getId();
    }
  };
  private final IndexedSet.FieldIndex mAddressIndex =
      new IndexedSet.FieldIndex<MasterWorkerInfo>() {
        @Override
        public Object getFieldValue(MasterWorkerInfo o) {
          return o.getAddress();
        }
      };
  private final IndexedSet<MasterWorkerInfo> mWorkers =
      new IndexedSet<MasterWorkerInfo>(mIdIndex, mAddressIndex);
  private final AtomicInteger mWorkerCounter;

  public BlockMaster(Journal journal) {
    super(journal);
    mBlocks = new HashMap<Long, MasterBlockInfo>();
    mBlockIdGenerator = new BlockIdGenerator();
    mWorkerCounter = new AtomicInteger(0);
    mLostBlocks = new HashSet<Long>();
  }

  @Override
  public TProcessor getProcessor() {
    return new BlockMasterService.Processor<BlockMasterServiceHandler>(
        new BlockMasterServiceHandler(this));
  }

  @Override
  public String getProcessorName() {
    return Constants.BLOCK_MASTER_SERVICE_NAME;
  }

  @Override
  public void processJournalCheckpoint(JournalInputStream inputStream) throws IOException {
    JournalEntry entry;
    while ((entry = inputStream.getNextEntry()) != null) {
      if (entry instanceof BlockIdGeneratorEntry) {
        mBlockIdGenerator.setNextContainerId(((BlockIdGeneratorEntry) entry).getNextContainerId());
      } else {
        throw new IOException("unexpected entry in checkpoint: " + entry);
      }
    }
    inputStream.close();
  }

  @Override
  public void processJournalEntry(JournalInputStream inputStream) throws IOException {
    if (inputStream instanceof BlockIdGeneratorEntry) {
      mBlockIdGenerator.setNextContainerId(((BlockIdGeneratorEntry) inputStream).getNextContainerId());
    } else {
      throw new IOException("unexpected entry in checkpoint: " + inputStream);
    }
  }

  @Override
  public void writeToJournal(JournalOutputStream outputStream) throws IOException {
    mBlockIdGenerator.writeToJournal(outputStream);
  }

  @Override
  public void start(boolean asMaster) throws IOException {
    startMaster(asMaster);
    if (isMasterMode()) {
      // TODO: start periodic heartbeat threads.
    }
  }

  @Override
  public void stop() throws IOException {
    stopMaster();
    if (isMasterMode()) {
      // TODO: stop heartbeat threads.
    }
  }

  public List<WorkerInfo> getWorkerInfoList() {
    List<WorkerInfo> workerInfoList = new ArrayList<WorkerInfo>(mWorkers.size());
    synchronized (mWorkers) {
      for (MasterWorkerInfo masterWorkerInfo : mWorkers) {
        workerInfoList.add(masterWorkerInfo.generateClientWorkerInfo());
      }
    }
    return workerInfoList;
  }

  public long getCapacityBytes() {
    long ret = 0;
    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers) {
        ret += worker.getCapacityBytes();
      }
    }
    return ret;
  }

  public long getUsedBytes() {
    long ret = 0;
    synchronized (mWorkers) {
      for (MasterWorkerInfo worker : mWorkers) {
        ret += worker.getUsedBytes();
      }
    }
    return ret;
  }

  public Set<Long> getLostBlocks() {
    return mLostBlocks;
  }

  public void removeBlocks(List<Long> blockIds) {
    for (long blockId : blockIds) {
      MasterBlockInfo masterBlockInfo = mBlocks.get(blockId);
      if (masterBlockInfo == null) {
        return;
      }
      for (long workerId : masterBlockInfo.getWorkers()) {
        masterBlockInfo.removeWorker(workerId);
        MasterWorkerInfo worker = mWorkers.getFirstByField(mIdIndex, workerId);
        if (worker != null) {
          worker.updateToRemovedBlock(true, blockId);
        }
      }
    }
  }

  @Override
  public long getNewContainerId() {
    synchronized (mBlockIdGenerator) {
      long containerId = mBlockIdGenerator.getNewBlockContainerId();
      writeJournalEntry(new BlockIdGeneratorEntry(containerId));
      flushJournal();
      return containerId;
    }
  }

  public void commitBlock(long workerId, long usedBytesOnTier, int tierAlias, long blockId,
      long length) {
    LOG.debug("Commit block: {}",
        FormatUtils.parametersToString(workerId, usedBytesOnTier, blockId, length));

    MasterWorkerInfo workerInfo = mWorkers.getFirstByField(mIdIndex, workerId);
    workerInfo.addBlock(blockId);
    workerInfo.updateUsedBytes(tierAlias, usedBytesOnTier);
    workerInfo.updateLastUpdatedTimeMs();

    MasterBlockInfo masterBlockInfo = mBlocks.get(blockId);
    if (masterBlockInfo == null) {
      masterBlockInfo = new MasterBlockInfo(blockId, length);
      mBlocks.put(blockId, masterBlockInfo);
    }
    masterBlockInfo.addWorker(workerId, tierAlias);
  }

  /**
   * Retrieves information for the given list of block ids.
   *
   * @param blockIds A list of block ids to retrieve the information for
   * @return A list of {@link BlockInfo} objects corresponding to the input list of block ids. The
   *         list is in the same order as the input list.
   */
  public List<BlockInfo> getBlockInfoList(List<Long> blockIds) {
    List<BlockInfo> ret = new ArrayList<BlockInfo>(blockIds.size());
    for (long blockId : blockIds) {
      MasterBlockInfo masterBlockInfo = mBlocks.get(blockId);
      if (masterBlockInfo != null) {
        // Construct the block info object to return.

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
        BlockInfo retInfo =
            new BlockInfo(masterBlockInfo.getBlockId(), masterBlockInfo.getLength(), locations);
        ret.add(retInfo);
      }
    }
    return ret;
  }

  public long getWorkerId(NetAddress workerNetAddress) {
    // TODO: this NetAddress cloned in case thrift re-uses the object. Does thrift re-use it?
    NetAddress workerAddress = new NetAddress(workerNetAddress);
    LOG.info("registerWorker(): WorkerNetAddress: " + workerAddress);

    synchronized (mWorkers) {
      if (mWorkers.contains(mAddressIndex, workerAddress)) {
        // This worker address is already mapped to a worker id.
        long oldWorkerId = mWorkers.getFirstByField(mAddressIndex, workerAddress).getId();
        LOG.warn("The worker " + workerAddress + " already exists as id " + oldWorkerId + ".");
        return oldWorkerId;
      }

      // Generate a new worker id.
      long workerId = mWorkerCounter.incrementAndGet();
      mWorkers.add(new MasterWorkerInfo(workerId, workerNetAddress));

      return workerId;
    }
  }

  public long workerRegister(long workerId, List<Long> totalBytesOnTiers,
      List<Long> usedBytesOnTiers, Map<Long, List<Long>> currentBlocksOnTiers) {
    synchronized (mWorkers) {
      if (mWorkers.contains(mIdIndex, workerId)) {
        LOG.warn("Could not find worker id: " + workerId + " to register.");
        return -1;
      }
      MasterWorkerInfo workerInfo = mWorkers.getFirstByField(mIdIndex, workerId);
      workerInfo.updateLastUpdatedTimeMs();

      // Gather all blocks on this worker.
      HashSet<Long> newBlocks = new HashSet<Long>();
      for (List<Long> blockIds : currentBlocksOnTiers.values()) {
        newBlocks.addAll(blockIds);
      }

      // Detect any lost blocks on this worker.
      Set<Long> removedBlocks = workerInfo.register(totalBytesOnTiers, usedBytesOnTiers, newBlocks);

      processWorkerRemovedBlocks(workerInfo, removedBlocks);
      processWorkerAddedBlocks(workerInfo, currentBlocksOnTiers);
      LOG.info("registerWorker(): " + workerInfo);
    }
    return workerId;
  }

  public Command workerHeartbeat(long workerId, List<Long> usedBytesOnTiers,
      List<Long> removedBlockIds, Map<Long, List<Long>> addedBlocksOnTiers) {
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

  /**
   * Updates the worker and block metadata for blocks removed from a worker.
   *
   * @param workerInfo The worker metadata object
   * @param removedBlockIds A list of block ids removed from the worker
   */
  private void processWorkerRemovedBlocks(MasterWorkerInfo workerInfo,
      Collection<Long> removedBlockIds) {
    // TODO: lock mBlocks?
    for (long removedBlockId : removedBlockIds) {
      MasterBlockInfo masterBlockInfo = mBlocks.get(removedBlockId);
      if (masterBlockInfo == null) {
        // TODO: throw exception?
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
   * @param workerInfo The worker metadata object
   * @param addedBlockIds Mapping from StorageDirId to a list of block ids added to the directory.
   */
  private void processWorkerAddedBlocks(MasterWorkerInfo workerInfo,
      Map<Long, List<Long>> addedBlockIds) {
    // TODO: lock mBlocks?
    for (Entry<Long, List<Long>> blockIds : addedBlockIds.entrySet()) {
      long storageDirId = blockIds.getKey();
      for (long blockId : blockIds.getValue()) {
        MasterBlockInfo masterBlockInfo = mBlocks.get(blockId);
        if (masterBlockInfo != null) {
          workerInfo.addBlock(blockId);
          // TODO: change upper API so that this is tier level or type, not storage dir id.
          int tierAlias = StorageDirId.getStorageLevelAliasValue(storageDirId);
          masterBlockInfo.addWorker(workerInfo.getId(), tierAlias);
          // TODO: update lost workers?
        } else {
          // TODO: throw exception?
          LOG.warn(
              "failed to register workerId: " + workerInfo.getId() + " to blockId: " + blockId);
        }
      }
    }
  }
}
