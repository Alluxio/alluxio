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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.MasterStorageTierAssoc;
import alluxio.PropertyKey;
import alluxio.Server;
import alluxio.StorageTierAssoc;
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.client.block.options.GetWorkerReportOptions.WorkerRange;
import alluxio.clock.SystemClock;
import alluxio.collections.ConcurrentHashSet;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GrpcService;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.ServiceType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.AbstractMaster;
import alluxio.master.MasterContext;
import alluxio.master.block.meta.MasterBlockInfo;
import alluxio.master.block.meta.MasterBlockLocation;
import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.master.journal.JournalContext;
import alluxio.master.metrics.MetricsMaster;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry;
import alluxio.proto.journal.Block.BlockInfoEntry;
import alluxio.proto.journal.Block.DeleteBlockEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.Address;
import alluxio.wire.BlockInfo;
import alluxio.wire.BlockLocation;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jersey.repackaged.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This block master manages the metadata for all the blocks and block workers in Alluxio.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class DefaultBlockMaster extends AbstractMaster implements BlockMaster {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultBlockMaster.class);
  private static final Set<Class<? extends Server>> DEPS =
      ImmutableSet.<Class<? extends Server>>of(MetricsMaster.class);

  /**
   * The number of container ids to 'reserve' before having to journal container id state. This
   * allows the master to return container ids within the reservation, without having to write to
   * the journal.
   */
  private static final long CONTAINER_ID_RESERVATION_SIZE = 1000;

  // Worker metadata management.
  private static final IndexDefinition<MasterWorkerInfo, Long> ID_INDEX =
      new IndexDefinition<MasterWorkerInfo, Long>(true) {
        @Override
        public Long getFieldValue(MasterWorkerInfo o) {
          return o.getId();
        }
      };

  private static final IndexDefinition<MasterWorkerInfo, WorkerNetAddress> ADDRESS_INDEX =
      new IndexDefinition<MasterWorkerInfo, WorkerNetAddress>(true) {
        @Override
        public WorkerNetAddress getFieldValue(MasterWorkerInfo o) {
          return o.getWorkerAddress();
        }
      };

  /**
   * Concurrency and locking in the BlockMaster
   *
   * The block master uses concurrent data structures to allow non-conflicting concurrent access.
   * This means each piece of metadata should be locked individually. There are two types of
   * metadata in the {@link DefaultBlockMaster}; {@link MasterBlockInfo} and
   * {@link MasterWorkerInfo}.
   * Individual objects must be locked before modifying the object, or reading a modifiable field
   * of an object. This will protect the internal integrity of the metadata object.
   *
   * Lock ordering must be preserved in order to prevent deadlock. If both a worker and block
   * metadata must be locked at the same time, the worker metadata ({@link MasterWorkerInfo})
   * must be locked before the block metadata ({@link MasterBlockInfo}).
   *
   * It should not be the case that multiple worker metadata must be locked at the same time, or
   * multiple block metadata must be locked at the same time. Operations involving different
   * workers or different blocks should be able to be performed independently.
   */

  // Block metadata management.
  /** Blocks on all workers, including active and lost blocks. This state must be journaled. */
  private final ConcurrentHashMap<Long, MasterBlockInfo> mBlocks =
      new ConcurrentHashMap<>(8192, 0.90f, 64);
  /** Keeps track of blocks which are no longer in Alluxio storage. */
  private final ConcurrentHashSet<Long> mLostBlocks = new ConcurrentHashSet<>(64, 0.90f, 64);

  /** This state must be journaled. */
  @GuardedBy("itself")
  private final BlockContainerIdGenerator mBlockContainerIdGenerator =
      new BlockContainerIdGenerator();

  /**
   * Mapping between all possible storage level aliases and their ordinal position. This mapping
   * forms a total ordering on all storage level aliases in the system, and must be consistent
   * across masters.
   */
  private final StorageTierAssoc mGlobalStorageTierAssoc;

  /** Keeps track of workers which are in communication with the master. */
  private final IndexedSet<MasterWorkerInfo> mWorkers =
      new IndexedSet<>(ID_INDEX, ADDRESS_INDEX);
  /** Keeps track of workers which are no longer in communication with the master. */
  private final IndexedSet<MasterWorkerInfo> mLostWorkers =
      new IndexedSet<>(ID_INDEX, ADDRESS_INDEX);
  /** Worker is not visualable until registration completes. */
  private final IndexedSet<MasterWorkerInfo> mTempWorkers =
      new IndexedSet<>(ID_INDEX, ADDRESS_INDEX);

  /** Listeners to call when lost workers are found. */
  private final List<Consumer<Address>> mLostWorkerFoundListeners
      = new ArrayList<>();

  /** Listeners to call when workers are lost. */
  private final List<Consumer<Address>> mWorkerLostListeners = new ArrayList<>();

  /** Listeners to call when a new worker registers. */
  private final List<BiConsumer<Address, List<ConfigProperty>>> mWorkerRegisteredListeners
      = new ArrayList<>();

  /** Handle to the metrics master. */
  private final MetricsMaster mMetricsMaster;

  /**
   * The service that detects lost worker nodes, and tries to restart the failed workers.
   * We store it here so that it can be accessed from tests.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mLostWorkerDetectionService;

  /** The value of the 'next container id' last journaled. */
  @GuardedBy("mBlockContainerIdGenerator")
  private long mJournaledNextContainerId = 0;

  /**
   * Creates a new instance of {@link DefaultBlockMaster}.
   *
   * @param metricsMaster the metrics master
   * @param masterContext the context for Alluxio master
   */
  DefaultBlockMaster(MetricsMaster metricsMaster, MasterContext masterContext) {
    this(metricsMaster, masterContext, new SystemClock(),
        ExecutorServiceFactories.cachedThreadPool(Constants.BLOCK_MASTER_NAME));
  }

  /**
   * Creates a new instance of {@link DefaultBlockMaster}.
   *
   * @param metricsMaster the metrics master
   * @param masterContext the context for Alluxio master
   * @param clock the clock to use for determining the time
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  DefaultBlockMaster(MetricsMaster metricsMaster, MasterContext masterContext, Clock clock,
      ExecutorServiceFactory executorServiceFactory) {
    super(masterContext, clock, executorServiceFactory);
    Preconditions.checkNotNull(metricsMaster, "metricsMaster");
    mGlobalStorageTierAssoc = new MasterStorageTierAssoc();
    mMetricsMaster = metricsMaster;
    Metrics.registerGauges(this);
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    Map<ServiceType, GrpcService> services = new HashMap<>();
    services.put(ServiceType.BLOCK_MASTER_CLIENT_SERVICE,
        new GrpcService(new BlockMasterClientServiceHandler(this)));
    services.put(ServiceType.BLOCK_MASTER_WORKER_SERVICE,
        new GrpcService(new BlockMasterWorkerServiceHandler(this)));
    return services;
  }

  @Override
  public String getName() {
    return Constants.BLOCK_MASTER_NAME;
  }

  @Override
  public void processJournalEntry(JournalEntry entry) throws IOException {
    // TODO(gene): A better way to process entries besides a huge switch?
    if (entry.hasBlockContainerIdGenerator()) {
      mJournaledNextContainerId = (entry.getBlockContainerIdGenerator()).getNextContainerId();
      mBlockContainerIdGenerator.setNextContainerId((mJournaledNextContainerId));
    } else if (entry.hasDeleteBlock()) {
      mBlocks.remove(entry.getDeleteBlock().getBlockId());
    } else if (entry.hasBlockInfo()) {
      BlockInfoEntry blockInfoEntry = entry.getBlockInfo();
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
  public void resetState() {
    mBlocks.clear();
    mJournaledNextContainerId = 0;
    mBlockContainerIdGenerator.setNextContainerId(0);
  }

  @Override
  public Iterator<JournalEntry> getJournalEntryIterator() {
    final Iterator<MasterBlockInfo> it = mBlocks.values().iterator();
    Iterator<JournalEntry> blockIterator = new Iterator<JournalEntry>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public JournalEntry next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        MasterBlockInfo info = it.next();
        BlockInfoEntry blockInfoEntry =
            BlockInfoEntry.newBuilder().setBlockId(info.getBlockId())
                .setLength(info.getLength()).build();
        return JournalEntry.newBuilder().setBlockInfo(blockInfoEntry).build();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("BlockMaster#Iterator#remove is not supported.");
      }
    };

    return Iterators
        .concat(CommonUtils.singleElementIterator(getContainerIdJournalEntry()), blockIterator);
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
    if (isLeader) {
      mLostWorkerDetectionService = getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_LOST_WORKER_DETECTION, new LostWorkerDetectionHeartbeatExecutor(),
          (int) Configuration.getMs(PropertyKey.MASTER_WORKER_HEARTBEAT_INTERVAL)));
    }
  }

  @Override
  public int getWorkerCount() {
    return mWorkers.size();
  }

  @Override
  public int getLostWorkerCount() {
    return mLostWorkers.size();
  }

  @Override
  public long getCapacityBytes() {
    long ret = 0;
    for (MasterWorkerInfo worker : mWorkers) {
      synchronized (worker) {
        ret += worker.getCapacityBytes();
      }
    }
    return ret;
  }

  @Override
  public StorageTierAssoc getGlobalStorageTierAssoc() {
    return mGlobalStorageTierAssoc;
  }

  @Override
  public long getUsedBytes() {
    long ret = 0;
    for (MasterWorkerInfo worker : mWorkers) {
      synchronized (worker) {
        ret += worker.getUsedBytes();
      }
    }
    return ret;
  }

  @Override
  public List<WorkerInfo> getWorkerInfoList() throws UnavailableException {
    if (mMasterContext.getSafeModeManager().isInSafeMode()) {
      throw new UnavailableException(ExceptionMessage.MASTER_IN_SAFEMODE.getMessage());
    }
    List<WorkerInfo> workerInfoList = new ArrayList<>(mWorkers.size());
    for (MasterWorkerInfo worker : mWorkers) {
      synchronized (worker) {
        workerInfoList.add(worker.generateWorkerInfo(null, true));
      }
    }
    return workerInfoList;
  }

  @Override
  public List<WorkerInfo> getLostWorkersInfoList() throws UnavailableException {
    if (mMasterContext.getSafeModeManager().isInSafeMode()) {
      throw new UnavailableException(ExceptionMessage.MASTER_IN_SAFEMODE.getMessage());
    }
    List<WorkerInfo> workerInfoList = new ArrayList<>(mLostWorkers.size());
    for (MasterWorkerInfo worker : mLostWorkers) {
      synchronized (worker) {
        workerInfoList.add(worker.generateWorkerInfo(null, false));
      }
    }
    Collections.sort(workerInfoList, new WorkerInfo.LastContactSecComparator());
    return workerInfoList;
  }

  @Override
  public List<WorkerInfo> getWorkerReport(GetWorkerReportOptions options)
      throws UnavailableException, InvalidArgumentException {
    if (mMasterContext.getSafeModeManager().isInSafeMode()) {
      throw new UnavailableException(ExceptionMessage.MASTER_IN_SAFEMODE.getMessage());
    }

    Set<MasterWorkerInfo> selectedLiveWorkers = new HashSet<>();
    Set<MasterWorkerInfo> selectedLostWorkers = new HashSet<>();
    WorkerRange workerRange = options.getWorkerRange();
    switch (workerRange) {
      case ALL:
        selectedLiveWorkers.addAll(mWorkers);
        selectedLostWorkers.addAll(mLostWorkers);
        break;
      case LIVE:
        selectedLiveWorkers.addAll(mWorkers);
        break;
      case LOST:
        selectedLostWorkers.addAll(mLostWorkers);
        break;
      case SPECIFIED:
        Set<String> addresses = options.getAddresses();
        Set<String> workerNames = new HashSet<>();

        selectedLiveWorkers = selectInfoByAddress(addresses, mWorkers, workerNames);
        selectedLostWorkers = selectInfoByAddress(addresses, mLostWorkers, workerNames);

        if (!addresses.isEmpty()) {
          String info = String.format("Unrecognized worker names: %s%n"
                  + "Supported worker names: %s%n",
              addresses.toString(), workerNames.toString());
          throw new InvalidArgumentException(info);
        }
        break;
      default:
        throw new InvalidArgumentException("Unrecognized worker range: " + workerRange);
    }

    List<WorkerInfo> workerInfoList = new ArrayList<>();
    for (MasterWorkerInfo worker : selectedLiveWorkers) {
      synchronized (worker) {
        workerInfoList.add(worker.generateWorkerInfo(options.getFieldRange(), true));
      }
    }
    for (MasterWorkerInfo worker : selectedLostWorkers) {
      synchronized (worker) {
        workerInfoList.add(worker.generateWorkerInfo(options.getFieldRange(), false));
      }
    }
    return workerInfoList;
  }

  @Override
  public void removeBlocks(List<Long> blockIds, boolean delete) throws UnavailableException {
    try (JournalContext journalContext = createJournalContext()) {
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
            if (mBlocks.remove(blockId) != null) {
              JournalEntry entry = JournalEntry.newBuilder()
                  .setDeleteBlock(DeleteBlockEntry.newBuilder().setBlockId(blockId)).build();
              journalContext.append(entry);
            }
          }
        }

        // Outside of locking the block. This does not have to be synchronized with the block
        // metadata, since it is essentially an asynchronous signal to the worker to remove the
        // block.
        for (long workerId : workerIds) {
          MasterWorkerInfo worker = mWorkers.getFirstByField(ID_INDEX, workerId);
          if (worker != null) {
            synchronized (worker) {
              worker.updateToRemovedBlock(true, blockId);
            }
          }
        }
      }
    }
  }

  @Override
  public void validateBlocks(Function<Long, Boolean> validator, boolean repair)
      throws UnavailableException {
    List<Long> invalidBlocks =
        mBlocks.keySet().stream().filter((blockId) -> !validator.apply(blockId))
            .collect(Collectors.toList());
    if (!invalidBlocks.isEmpty()) {
      long limit = 100;
      List<Long> loggedBlocks = invalidBlocks.stream().limit(limit).collect(Collectors.toList());
      LOG.warn("Found {} orphan blocks without corresponding file metadata.", invalidBlocks.size());
      if (invalidBlocks.size() > limit) {
        LOG.warn("The first {} orphan blocks include {}.", limit, loggedBlocks);
      } else {
        LOG.warn("The orphan blocks include {}.", loggedBlocks);
      }
      if (repair) {
        LOG.warn("Deleting {} orphan blocks.", invalidBlocks.size());
        removeBlocks(invalidBlocks, true);
      } else {
        LOG.warn("Restart Alluxio master with {}=true to delete the blocks and repair the system.",
            PropertyKey.Name.MASTER_STARTUP_BLOCK_INTEGRITY_CHECK_ENABLED);
      }
    }
  }

  /**
   * @return a new block container id
   */
  @Override
  public long getNewContainerId() throws UnavailableException {
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
      try (JournalContext journalContext = createJournalContext()) {
        // This must be flushed while holding the lock on mBlockContainerIdGenerator, in order to
        // prevent subsequent calls to return ids that have not been journaled and flushed.
        journalContext.append(getContainerIdJournalEntry());
      }
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

  // TODO(binfan): check the logic is correct or not when commitBlock is a retry
  @Override
  public void commitBlock(long workerId, long usedBytesOnTier, String tierAlias, long blockId,
      long length) throws NotFoundException, UnavailableException {
    LOG.debug("Commit block from workerId: {}, usedBytesOnTier: {}, blockId: {}, length: {}",
        workerId, usedBytesOnTier, blockId, length);

    MasterWorkerInfo worker = mWorkers.getFirstByField(ID_INDEX, workerId);
    // TODO(peis): Check lost workers as well.
    if (worker == null) {
      throw new NotFoundException(ExceptionMessage.NO_WORKER_FOUND.getMessage(workerId));
    }

    // Lock the worker metadata first.
    try (JournalContext journalContext = createJournalContext()) {
      synchronized (worker) {
        // Loop until block metadata is successfully locked.
        while (true) {
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
            } else if (block.getLength() != length && block.getLength() == Constants.UNKNOWN_SIZE) {
              // The block size was previously unknown. Update the block size with the committed
              // size, and append a journal entry.
              block.updateLength(length);
              writeJournal = true;
            }
            if (writeJournal) {
              BlockInfoEntry blockInfo =
                  BlockInfoEntry.newBuilder().setBlockId(blockId).setLength(length).build();
              journalContext.append(JournalEntry.newBuilder().setBlockInfo(blockInfo).build());
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
    }
  }

  @Override
  public void commitBlockInUFS(long blockId, long length) throws UnavailableException {
    LOG.debug("Commit block in ufs. blockId: {}, length: {}", blockId, length);
    if (mBlocks.get(blockId) != null) {
      // Block metadata already exists, so do not need to create a new one.
      return;
    }

    // The block has not been committed previously, so add the metadata to commit the block.
    MasterBlockInfo block = new MasterBlockInfo(blockId, length);
    try (JournalContext journalContext = createJournalContext()) {
      synchronized (block) {
        if (mBlocks.putIfAbsent(blockId, block) == null) {
          // Successfully added the new block metadata. Append a journal entry for the new metadata.
          BlockInfoEntry blockInfo =
              BlockInfoEntry.newBuilder().setBlockId(blockId).setLength(length).build();
          journalContext.append(JournalEntry.newBuilder().setBlockInfo(blockInfo).build());
        }
      }
    }
  }

  @Override
  public BlockInfo getBlockInfo(long blockId) throws BlockInfoException, UnavailableException {
    MasterBlockInfo block = mBlocks.get(blockId);
    if (block == null) {
      throw new BlockInfoException(ExceptionMessage.BLOCK_META_NOT_FOUND, blockId);
    }
    synchronized (block) {
      return generateBlockInfo(block);
    }
  }

  @Override
  public List<BlockInfo> getBlockInfoList(List<Long> blockIds) throws UnavailableException {
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

  @Override
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

  @Override
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
   * Find a worker which is considered lost or just gets its id.
   * @param workerNetAddress the address used to find a worker
   * @return a {@link MasterWorkerInfo} which is presented in master but not registered,
   *         or null if not worker is found.
   */
  @Nullable
  private MasterWorkerInfo findUnregisteredWorker(WorkerNetAddress workerNetAddress) {
    for (IndexedSet<MasterWorkerInfo> workers: Arrays.asList(mTempWorkers, mLostWorkers)) {
      MasterWorkerInfo worker = workers.getFirstByField(ADDRESS_INDEX, workerNetAddress);
      if (worker != null) {
        return worker;
      }
    }
    return null;
  }

  /**
   * Find a worker which is considered lost or just gets its id.
   * @param workerId the id used to find a worker
   * @return a {@link MasterWorkerInfo} which is presented in master but not registered,
   *         or null if not worker is found.
   */
  @Nullable
  private MasterWorkerInfo findUnregisteredWorker(long workerId) {
    for (IndexedSet<MasterWorkerInfo> workers: Arrays.asList(mTempWorkers, mLostWorkers)) {
      MasterWorkerInfo worker = workers.getFirstByField(ID_INDEX, workerId);
      if (worker != null) {
        return worker;
      }
    }
    return null;
  }

  /**
   * Re-register a lost worker or complete registration after getting a worker id.
   *
   * @param workerId the worker id to register
   */
  @Nullable
  private MasterWorkerInfo registerWorkerInternal(long workerId) {
    for (IndexedSet<MasterWorkerInfo> workers: Arrays.asList(mTempWorkers, mLostWorkers)) {
      MasterWorkerInfo worker = workers.getFirstByField(ID_INDEX, workerId);
      if (worker == null) {
        continue;
      }

      synchronized (worker) {
        worker.updateLastUpdatedTimeMs();
        mWorkers.add(worker);
        workers.remove(worker);
        if (workers == mLostWorkers) {
          for (Consumer<Address> function : mLostWorkerFoundListeners) {
            function.accept(new Address(worker.getWorkerAddress().getHost(),
                  worker.getWorkerAddress().getRpcPort()));
          }
          LOG.warn("A lost worker {} has requested its old id {}.",
              worker.getWorkerAddress(), worker.getId());
        }
      }

      return worker;
    }
    return null;
  }

  @Override
  public long getWorkerId(WorkerNetAddress workerNetAddress) {
    // TODO(gpang): Clone WorkerNetAddress in case thrift re-uses the object. Does thrift re-use it?
    MasterWorkerInfo existingWorker = mWorkers.getFirstByField(ADDRESS_INDEX, workerNetAddress);
    if (existingWorker != null) {
      // This worker address is already mapped to a worker id.
      long oldWorkerId = existingWorker.getId();
      LOG.warn("The worker {} already exists as id {}.", workerNetAddress, oldWorkerId);
      return oldWorkerId;
    }

    existingWorker = findUnregisteredWorker(workerNetAddress);
    if (existingWorker != null) {
      return existingWorker.getId();
    }

    // Generate a new worker id.
    long workerId = IdUtils.getRandomNonNegativeLong();
    while (!mTempWorkers.add(new MasterWorkerInfo(workerId, workerNetAddress))) {
      workerId = IdUtils.getRandomNonNegativeLong();
    }

    LOG.info("getWorkerId(): WorkerNetAddress: {} id: {}", workerNetAddress, workerId);
    return workerId;
  }

  @Override
  public void workerRegister(long workerId, List<String> storageTiers,
      Map<String, Long> totalBytesOnTiers, Map<String, Long> usedBytesOnTiers,
      Map<String, List<Long>> currentBlocksOnTiers,
      RegisterWorkerPOptions options) throws NotFoundException {

    MasterWorkerInfo worker = mWorkers.getFirstByField(ID_INDEX, workerId);

    if (worker == null) {
      worker = findUnregisteredWorker(workerId);
    }

    if (worker == null) {
      throw new NotFoundException(ExceptionMessage.NO_WORKER_FOUND.getMessage(workerId));
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
      processWorkerOrphanedBlocks(worker);
    }
    if (options.getConfigsCount() > 0) {
      for (BiConsumer<Address, List<ConfigProperty>> function : mWorkerRegisteredListeners) {
        WorkerNetAddress workerAddress = worker.getWorkerAddress();
        function.accept(new Address(workerAddress.getHost(), workerAddress.getRpcPort()),
            options.getConfigsList());
      }
    }

    registerWorkerInternal(workerId);

    LOG.info("registerWorker(): {}", worker);
  }

  @Override
  public Command workerHeartbeat(long workerId, Map<String, Long> usedBytesOnTiers,
      List<Long> removedBlockIds, Map<String, List<Long>> addedBlocksOnTiers,
      List<Metric> metrics) {
    MasterWorkerInfo worker = mWorkers.getFirstByField(ID_INDEX, workerId);
    if (worker == null) {
      LOG.warn("Could not find worker id: {} for heartbeat.", workerId);
      return Command.newBuilder().setCommandType(CommandType.Register).build();
    }

    synchronized (worker) {
      // Technically, 'worker' should be confirmed to still be in the data structure. Lost worker
      // detection can remove it. However, we are intentionally ignoring this race, since the worker
      // will just re-register regardless.
      processWorkerRemovedBlocks(worker, removedBlockIds);
      processWorkerAddedBlocks(worker, addedBlocksOnTiers);
      processWorkerMetrics(worker.getWorkerAddress().getHost(), metrics);

      worker.updateUsedBytes(usedBytesOnTiers);
      worker.updateLastUpdatedTimeMs();

      List<Long> toRemoveBlocks = worker.getToRemoveBlocks();
      if (toRemoveBlocks.isEmpty()) {
        return Command.newBuilder().setCommandType(CommandType.Nothing).build();
      }
      return Command.newBuilder().setCommandType(CommandType.Free).addAllData(toRemoveBlocks)
          .build();
    }
  }

  private void processWorkerMetrics(String hostname, List<Metric> metrics) {
    if (metrics.isEmpty()) {
      return;
    }
    mMetricsMaster.workerHeartbeat(hostname, metrics);
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
          LOG.warn("Invalid block: {} from worker {}.", blockId,
              workerInfo.getWorkerAddress().getHost());
        }
      }
    }
  }

  @GuardedBy("workerInfo")
  private void processWorkerOrphanedBlocks(MasterWorkerInfo workerInfo) {
    for (long block : workerInfo.getBlocks()) {
      if (!mBlocks.containsKey(block)) {
        LOG.info("Requesting delete for orphaned block: {} from worker {}.", block,
            workerInfo.getWorkerAddress().getHost());
        workerInfo.updateToRemovedBlock(true, block);
      }
    }
  }

  @Override
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
  private BlockInfo generateBlockInfo(MasterBlockInfo masterBlockInfo) throws UnavailableException {
    if (mMasterContext.getSafeModeManager().isInSafeMode()) {
      throw new UnavailableException(ExceptionMessage.MASTER_IN_SAFEMODE.getMessage());
    }
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
          mWorkers.getFirstByField(ID_INDEX, masterBlockLocation.getWorkerId());
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

  @Override
  public void reportLostBlocks(List<Long> blockIds) {
    mLostBlocks.addAll(blockIds);
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return DEPS;
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
      long masterWorkerTimeoutMs = Configuration.getMs(PropertyKey.MASTER_WORKER_TIMEOUT_MS);
      for (MasterWorkerInfo worker : mWorkers) {
        synchronized (worker) {
          final long lastUpdate = mClock.millis() - worker.getLastUpdatedTimeMs();
          if (lastUpdate > masterWorkerTimeoutMs) {
            LOG.error("The worker {}({}) timed out after {}ms without a heartbeat!", worker.getId(),
                worker.getWorkerAddress(), lastUpdate);
            mLostWorkers.add(worker);
            mWorkers.remove(worker);
            WorkerNetAddress workerAddress = worker.getWorkerAddress();
            for (Consumer<Address> function : mWorkerLostListeners) {
              function.accept(new Address(workerAddress.getHost(), workerAddress.getRpcPort()));
            }
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

  /**
   * Selects the MasterWorkerInfo from workerInfoSet whose host or related IP address
   * exists in addresses.
   *
   * @param addresses the address set that user passed in
   * @param workerInfoSet the MasterWorkerInfo set to select info from
   * @param workerNames the supported worker names
   */
  private Set<MasterWorkerInfo> selectInfoByAddress(Set<String> addresses,
      Set<MasterWorkerInfo> workerInfoSet, Set<String> workerNames) {
    return workerInfoSet.stream().filter(info -> {
      String host = info.getWorkerAddress().getHost();
      workerNames.add(host);

      String ip = null;
      try {
        ip = NetworkAddressUtils.resolveIpAddress(host);
        workerNames.add(ip);
      } catch (UnknownHostException e) {
        // The host may already be an IP address
      }

      if (addresses.contains(host)) {
        addresses.remove(host);
        return true;
      }

      if (ip != null) {
        if (addresses.contains(ip)) {
          addresses.remove(ip);
          return true;
        }
      }
      return false;
    }).collect(Collectors.toSet());
  }

  @Override
  public void registerLostWorkerFoundListener(Consumer<Address> function) {
    mLostWorkerFoundListeners.add(function);
  }

  @Override
  public void registerWorkerLostListener(Consumer<Address> function) {
    mWorkerLostListeners.add(function);
  }

  @Override
  public void registerNewWorkerConfListener(BiConsumer<Address, List<ConfigProperty>> function) {
    mWorkerRegisteredListeners.add(function);
  }

  /**
   * Class that contains metrics related to BlockMaster.
   */
  public static final class Metrics {
    public static final String CAPACITY_TOTAL = "CapacityTotal";
    public static final String CAPACITY_USED = "CapacityUsed";
    public static final String CAPACITY_FREE = "CapacityFree";
    public static final String WORKERS = "Workers";
    public static final String TIER = "Tier";

    /**
     * Registers metric gauges.
     *
     * @param master the block master handle
     */
    @VisibleForTesting
    public static void registerGauges(final BlockMaster master) {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(CAPACITY_TOTAL),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return master.getCapacityBytes();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(CAPACITY_USED),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return master.getUsedBytes();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(CAPACITY_FREE),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return master.getCapacityBytes() - master.getUsedBytes();
            }
          });

      for (int i = 0; i < master.getGlobalStorageTierAssoc().size(); i++) {
        String alias = master.getGlobalStorageTierAssoc().getAlias(i);
        MetricsSystem.registerGaugeIfAbsent(
            MetricsSystem.getMetricName(CAPACITY_TOTAL + TIER + alias), new Gauge<Long>() {
              @Override
              public Long getValue() {
                return master.getTotalBytesOnTiers().getOrDefault(alias, 0L);
              }
            });

        MetricsSystem.registerGaugeIfAbsent(
            MetricsSystem.getMetricName(CAPACITY_USED + TIER + alias), new Gauge<Long>() {
              @Override
              public Long getValue() {
                return master.getUsedBytesOnTiers().getOrDefault(alias, 0L);
              }
            });
        MetricsSystem.registerGaugeIfAbsent(
            MetricsSystem.getMetricName(CAPACITY_FREE + TIER + alias), new Gauge<Long>() {
              @Override
              public Long getValue() {
                return master.getTotalBytesOnTiers().getOrDefault(alias, 0L)
                    - master.getUsedBytesOnTiers().getOrDefault(alias, 0L);
              }
            });
      }

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(WORKERS),
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return master.getWorkerCount();
            }
          });
    }

    private Metrics() {} // prevent instantiation
  }
}
