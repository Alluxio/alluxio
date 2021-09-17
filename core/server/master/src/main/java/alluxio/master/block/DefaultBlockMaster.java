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

import alluxio.annotation.SuppressFBWarnings;
import alluxio.Constants;
import alluxio.MasterStorageTierAssoc;
import alluxio.Server;
import alluxio.StorageTierAssoc;
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.client.block.options.GetWorkerReportOptions.WorkerRange;
import alluxio.clock.SystemClock;
import alluxio.collections.ConcurrentHashSet;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GrpcService;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.ServiceType;
import alluxio.grpc.StorageList;
import alluxio.grpc.WorkerLostStorageInfo;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.CoreMaster;
import alluxio.master.CoreMasterContext;
import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.master.block.meta.WorkerMetaLockSection;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.metastore.BlockStore;
import alluxio.master.metastore.BlockStore.Block;
import alluxio.master.metrics.MetricsMaster;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.journal.Block.BlockContainerIdGeneratorEntry;
import alluxio.proto.journal.Block.BlockInfoEntry;
import alluxio.proto.journal.Block.DeleteBlockEntry;
import alluxio.proto.journal.Journal.JournalEntry;
import alluxio.proto.meta.Block.BlockLocation;
import alluxio.proto.meta.Block.BlockMeta;
import alluxio.resource.CloseableIterator;
import alluxio.resource.LockResource;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.Address;
import alluxio.wire.BlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Striped;
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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
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
public class DefaultBlockMaster extends CoreMaster implements BlockMaster {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultBlockMaster.class);
  private static final Set<Class<? extends Server>> DEPS =
      ImmutableSet.<Class<? extends Server>>of(MetricsMaster.class);

  /**
   * The number of container ids to 'reserve' before having to journal container id state. This
   * allows the master to return container ids within the reservation, without having to write to
   * the journal.
   */
  private static final long CONTAINER_ID_RESERVATION_SIZE = 1000;

  /** The only valid key for {@link #mWorkerInfoCache}. */
  private static final String WORKER_INFO_CACHE_KEY = "WorkerInfoKey";

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
   * metadata in the {@link DefaultBlockMaster}: block metadata and worker metadata.
   *
   * The worker metadata is represented by the {@link MasterWorkerInfo} object.
   * See javadoc of {@link MasterWorkerInfo} for details.
   *
   * To modify or read a modifiable piece of worker metadata, the {@link MasterWorkerInfo} for the
   * worker must be locked following the instructions in {@link MasterWorkerInfo}.
   * For block metadata, the id of the block must be locked.
   * This will protect the internal integrity of the block and worker metadata.
   *
   * A worker's relevant locks must be held to
   * - Check/Update the worker register status
   * - Read/Update the worker usage
   * - Read/Update the worker present/to-be-removed blocks
   * - Any combinations of the above
   *
   * A block's lock must be held to
   * - Perform any BlockStore operations on the block
   * - Add or remove the block from mLostBlocks
   *
   * Lock ordering must be preserved in order to prevent deadlock. If both worker and block
   * metadata must be locked at the same time, the worker metadata must be locked before the block
   * metadata. When the locks are released, they must be released in the opposite order.
   *
   * Locking on the worker metadata are managed by
   * {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}.
   * This guarantees when multiple parts of the worker metadata are accessed/updated,
   * the locks are acquired and released in order.
   * See javadoc of {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)} for
   * example usages.
   *
   * It should not be the case that multiple worker metadata must be locked at the same time, or
   * multiple block metadata must be locked at the same time. Operations involving different workers
   * or different blocks should be able to be performed independently.
   */

  /**
   * 10k locks balances between keeping a small memory footprint and avoiding unnecessary lock
   * contention. Each stripe is around 100 bytes, so this takes about 1MB. Block locking critical
   * sections are short, so it is acceptable to occasionally have conflicts where two different
   * blocks want to lock the same stripe.
   */
  private final Striped<Lock> mBlockLocks = Striped.lock(10_000);
  /** Manages block metadata and block locations. */
  private final BlockStore mBlockStore;

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
   * A loading cache for worker info list, refresh periodically.
   * This cache only has a single key {@link  #WORKER_INFO_CACHE_KEY}.
   */
  private LoadingCache<String, List<WorkerInfo>> mWorkerInfoCache;

  /**
   * Creates a new instance of {@link DefaultBlockMaster}.
   *
   * @param metricsMaster the metrics master
   * @param masterContext the context for Alluxio master
   */
  DefaultBlockMaster(MetricsMaster metricsMaster, CoreMasterContext masterContext) {
    this(metricsMaster, masterContext, new SystemClock(),
        ExecutorServiceFactories.cachedThreadPool(Constants.BLOCK_MASTER_NAME));
  }

  @VisibleForTesting
  DefaultBlockMaster(MetricsMaster metricsMaster, CoreMasterContext masterContext, Clock clock,
      ExecutorServiceFactory executorServiceFactory, BlockStore blockStore) {
    super(masterContext, clock, executorServiceFactory);
    Preconditions.checkNotNull(metricsMaster, "metricsMaster");

    mBlockStore = blockStore;
    mGlobalStorageTierAssoc = new MasterStorageTierAssoc();
    mMetricsMaster = metricsMaster;
    Metrics.registerGauges(this);

    mWorkerInfoCache = CacheBuilder.newBuilder()
        .refreshAfterWrite(ServerConfiguration
            .getMs(PropertyKey.MASTER_WORKER_INFO_CACHE_REFRESH_TIME), TimeUnit.MILLISECONDS)
        .build(new CacheLoader<String, List<WorkerInfo>>() {
          @Override
          public List<WorkerInfo> load(String key) {
            return constructWorkerInfoList();
          }
        });

    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_LOST_BLOCK_COUNT.getName(),
        this::getLostBlocksCount);
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
  DefaultBlockMaster(MetricsMaster metricsMaster, CoreMasterContext masterContext, Clock clock,
      ExecutorServiceFactory executorServiceFactory) {
    this(metricsMaster, masterContext, clock, executorServiceFactory,
        masterContext.getBlockStoreFactory().get());
  }

  @Override
  public String getName() {
    return Constants.BLOCK_MASTER_NAME;
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
  public boolean processJournalEntry(JournalEntry entry) {
    // TODO(gene): A better way to process entries besides a huge switch?
    if (entry.hasBlockContainerIdGenerator()) {
      mJournaledNextContainerId = (entry.getBlockContainerIdGenerator()).getNextContainerId();
      mBlockContainerIdGenerator.setNextContainerId((mJournaledNextContainerId));
    } else if (entry.hasDeleteBlock()) {
      mBlockStore.removeBlock(entry.getDeleteBlock().getBlockId());
    } else if (entry.hasBlockInfo()) {
      BlockInfoEntry blockInfoEntry = entry.getBlockInfo();
      long length = blockInfoEntry.getLength();
      Optional<BlockMeta> block = mBlockStore.getBlock(blockInfoEntry.getBlockId());
      if (block.isPresent()) {
        long oldLen = block.get().getLength();
        if (oldLen != Constants.UNKNOWN_SIZE) {
          LOG.warn("Attempting to update block length ({}) to a different length ({}).", oldLen,
              length);
          return true;
        }
      }
      mBlockStore.putBlock(blockInfoEntry.getBlockId(),
          BlockMeta.newBuilder().setLength(blockInfoEntry.getLength()).build());
    } else {
      return false;
    }
    return true;
  }

  @Override
  public void resetState() {
    mBlockStore.clear();
    mJournaledNextContainerId = 0;
    mBlockContainerIdGenerator.setNextContainerId(0);
  }

  @Override
  public CheckpointName getCheckpointName() {
    return CheckpointName.BLOCK_MASTER;
  }

  @Override
  public CloseableIterator<JournalEntry> getJournalEntryIterator() {
    Iterator<Block> it = mBlockStore.iterator();
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
        Block block = it.next();
        BlockInfoEntry blockInfoEntry =
            BlockInfoEntry.newBuilder().setBlockId(block.getId())
                .setLength(block.getMeta().getLength()).build();
        return JournalEntry.newBuilder().setBlockInfo(blockInfoEntry).build();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("BlockMaster#Iterator#remove is not supported.");
      }
    };

    return CloseableIterator.noopCloseable(Iterators
        .concat(CommonUtils.singleElementIterator(getContainerIdJournalEntry()), blockIterator));
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
    if (isLeader) {
      mLostWorkerDetectionService = getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_LOST_WORKER_DETECTION, new LostWorkerDetectionHeartbeatExecutor(),
          (int) ServerConfiguration.getMs(PropertyKey.MASTER_LOST_WORKER_DETECTION_INTERVAL),
          ServerConfiguration.global(), mMasterContext.getUserState()));
    }
  }

  @Override
  public void stop() throws IOException {
    super.stop();
  }

  @Override
  public void close() throws IOException {
    super.close();
    mBlockStore.close();
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
      try (LockResource r = worker.lockWorkerMeta(
          EnumSet.of(WorkerMetaLockSection.USAGE), true)) {
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
      try (LockResource r = worker.lockWorkerMeta(
          EnumSet.of(WorkerMetaLockSection.USAGE), true)) {
        ret += worker.getUsedBytes();
      }
    }
    return ret;
  }

  @Override
  public List<WorkerInfo> getWorkerInfoList() throws UnavailableException {
    if (mSafeModeManager.isInSafeMode()) {
      throw new UnavailableException(ExceptionMessage.MASTER_IN_SAFEMODE.getMessage());
    }
    try {
      return mWorkerInfoCache.get(WORKER_INFO_CACHE_KEY);
    } catch (ExecutionException e) {
      throw new UnavailableException("Unable to get worker info list from cache", e);
    }
  }

  private List<WorkerInfo> constructWorkerInfoList() {
    List<WorkerInfo> workerInfoList = new ArrayList<>(mWorkers.size());
    for (MasterWorkerInfo worker : mWorkers) {
      // extractWorkerInfo handles the locking internally
      workerInfoList.add(extractWorkerInfo(worker, null, true));
    }
    return workerInfoList;
  }

  @Override
  public List<WorkerInfo> getLostWorkersInfoList() throws UnavailableException {
    if (mSafeModeManager.isInSafeMode()) {
      throw new UnavailableException(ExceptionMessage.MASTER_IN_SAFEMODE.getMessage());
    }
    List<WorkerInfo> workerInfoList = new ArrayList<>(mLostWorkers.size());
    for (MasterWorkerInfo worker : mLostWorkers) {
      // extractWorkerInfo handles the locking internally
      workerInfoList.add(extractWorkerInfo(worker, null, false));
    }
    Collections.sort(workerInfoList, new WorkerInfo.LastContactSecComparator());
    return workerInfoList;
  }

  @Override
  public Set<WorkerNetAddress> getWorkerAddresses() throws UnavailableException {
    if (mSafeModeManager.isInSafeMode()) {
      throw new UnavailableException(ExceptionMessage.MASTER_IN_SAFEMODE.getMessage());
    }
    Set<WorkerNetAddress> workerAddresses = new HashSet<>(mWorkers.size());
    for (MasterWorkerInfo worker : mWorkers) {
      // worker net address is unmodifiable after initialization, no locking is needed
      workerAddresses.add(worker.getWorkerAddress());
    }
    return workerAddresses;
  }

  @Override
  public List<WorkerInfo> getWorkerReport(GetWorkerReportOptions options)
      throws UnavailableException, InvalidArgumentException {
    if (mSafeModeManager.isInSafeMode()) {
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
      // extractWorkerInfo handles the locking internally
      workerInfoList.add(extractWorkerInfo(worker, options.getFieldRange(), true));
    }
    for (MasterWorkerInfo worker : selectedLostWorkers) {
      // extractWorkerInfo handles the locking internally
      workerInfoList.add(extractWorkerInfo(worker, options.getFieldRange(), false));
    }
    return workerInfoList;
  }

  /**
   * Locks the {@link MasterWorkerInfo} properly and convert it to a {@link WorkerInfo}.
   */
  private WorkerInfo extractWorkerInfo(MasterWorkerInfo worker,
      Set<GetWorkerReportOptions.WorkerInfoField> fieldRange, boolean isLiveWorker) {
    try (LockResource r = worker.lockWorkerMeta(
        EnumSet.of(WorkerMetaLockSection.USAGE), true)) {
      return worker.generateWorkerInfo(fieldRange, isLiveWorker);
    }
  }

  @Override
  public List<WorkerLostStorageInfo> getWorkerLostStorage() {
    List<WorkerLostStorageInfo> workerLostStorageList = new ArrayList<>();
    for (MasterWorkerInfo worker : mWorkers) {
      try (LockResource r = worker.lockWorkerMeta(EnumSet.of(WorkerMetaLockSection.USAGE), true)) {
        if (worker.hasLostStorage()) {
          Map<String, StorageList> lostStorage = worker.getLostStorage().entrySet()
              .stream().collect(Collectors.toMap(Map.Entry::getKey,
                  e -> StorageList.newBuilder().addAllStorage(e.getValue()).build()));
          workerLostStorageList.add(WorkerLostStorageInfo.newBuilder()
              .setAddress(GrpcUtils.toProto(worker.getWorkerAddress()))
              .putAllLostStorage(lostStorage).build());
        }
      }
    }
    return workerLostStorageList;
  }

  @Override
  public void removeBlocks(List<Long> blockIds, boolean delete) throws UnavailableException {
    try (JournalContext journalContext = createJournalContext()) {
      for (long blockId : blockIds) {
        HashSet<Long> workerIds = new HashSet<>();

        try (LockResource r = lockBlock(blockId)) {
          Optional<BlockMeta> block = mBlockStore.getBlock(blockId);
          if (!block.isPresent()) {
            continue;
          }
          for (BlockLocation loc : mBlockStore.getLocations(blockId)) {
            workerIds.add(loc.getWorkerId());
          }
          // Two cases here:
          // 1) For delete: delete the block metadata.
          // 2) For free: keep the block metadata. mLostBlocks will be changed in
          // processWorkerRemovedBlocks
          if (delete) {
            // Make sure blockId is removed from mLostBlocks when the block metadata is deleted.
            // Otherwise blockId in mLostBlock can be dangling index if the metadata is gone.
            mLostBlocks.remove(blockId);
            mBlockStore.removeBlock(blockId);
            JournalEntry entry = JournalEntry.newBuilder()
                .setDeleteBlock(DeleteBlockEntry.newBuilder().setBlockId(blockId)).build();
            journalContext.append(entry);
          }
        }

        // Outside of locking the block. This does not have to be synchronized with the block
        // metadata, since it is essentially an asynchronous signal to the worker to remove the
        // block.
        // TODO(jiacheng): if the block locations are changed (like a new worker is registered
        //  with the block), the block will not be freed ever. The locking logic in
        //  workerRegister should be changed to address this race condition.
        for (long workerId : workerIds) {
          MasterWorkerInfo worker = mWorkers.getFirstByField(ID_INDEX, workerId);
          if (worker != null) {
            try (LockResource r = worker.lockWorkerMeta(
                EnumSet.of(WorkerMetaLockSection.BLOCKS), false)) {
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
    List<Long> invalidBlocks = new ArrayList<>();
    for (Iterator<Block> iter = mBlockStore.iterator(); iter.hasNext(); ) {
      long id = iter.next().getId();
      if (!validator.apply(id)) {
        invalidBlocks.add(id);
      }
    }
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
    synchronized (mBlockContainerIdGenerator) {
      BlockContainerIdGeneratorEntry blockContainerIdGenerator =
          BlockContainerIdGeneratorEntry.newBuilder().setNextContainerId(mJournaledNextContainerId)
              .build();
      return JournalEntry.newBuilder().setBlockContainerIdGenerator(blockContainerIdGenerator)
          .build();
    }
  }

  // TODO(binfan): check the logic is correct or not when commitBlock is a retry
  @Override
  public void commitBlock(long workerId, long usedBytesOnTier, String tierAlias,
      String mediumType, long blockId, long length)
      throws NotFoundException, UnavailableException {
    LOG.debug("Commit block from workerId: {}, usedBytesOnTier: {}, blockId: {}, length: {}",
        workerId, usedBytesOnTier, blockId, length);

    MasterWorkerInfo worker = mWorkers.getFirstByField(ID_INDEX, workerId);
    // TODO(peis): Check lost workers as well.
    if (worker == null) {
      throw new NotFoundException(ExceptionMessage.NO_WORKER_FOUND.getMessage(workerId));
    }

    try (JournalContext journalContext = createJournalContext()) {
      // Lock the worker metadata here to preserve the lock order
      // The worker metadata must be locked before the blocks
      try (LockResource lr = worker.lockWorkerMeta(
          EnumSet.of(WorkerMetaLockSection.USAGE, WorkerMetaLockSection.BLOCKS), false)) {
        try (LockResource r = lockBlock(blockId)) {
          Optional<BlockMeta> block = mBlockStore.getBlock(blockId);
          if (!block.isPresent() || block.get().getLength() != length) {
            if (block.isPresent() && block.get().getLength() != Constants.UNKNOWN_SIZE) {
              LOG.warn("Rejecting attempt to change block length from {} to {}",
                  block.get().getLength(), length);
            } else {
              mBlockStore.putBlock(blockId, BlockMeta.newBuilder().setLength(length).build());
              BlockInfoEntry blockInfo =
                  BlockInfoEntry.newBuilder().setBlockId(blockId).setLength(length).build();
              journalContext.append(JournalEntry.newBuilder().setBlockInfo(blockInfo).build());
            }
          }
          // Update the block metadata with the new worker location.
          mBlockStore.addLocation(blockId, BlockLocation.newBuilder()
              .setWorkerId(workerId)
              .setTier(tierAlias)
              .setMediumType(mediumType)
              .build());
          // This worker has this block, so it is no longer lost.
          mLostBlocks.remove(blockId);

          // Update the worker information for this new block.
          // TODO(binfan): when retry commitBlock on master is expected, make sure metrics are not
          // double counted.
          worker.addBlock(blockId);
          worker.updateUsedBytes(tierAlias, usedBytesOnTier);
        }
      }

      worker.updateLastUpdatedTimeMs();
    }
  }

  @Override
  public void commitBlockInUFS(long blockId, long length) throws UnavailableException {
    LOG.debug("Commit block in ufs. blockId: {}, length: {}", blockId, length);
    try (JournalContext journalContext = createJournalContext();
         LockResource r = lockBlock(blockId)) {
      if (mBlockStore.getBlock(blockId).isPresent()) {
        // Block metadata already exists, so do not need to create a new one.
        return;
      }
      mBlockStore.putBlock(blockId, BlockMeta.newBuilder().setLength(length).build());
      BlockInfoEntry blockInfo =
          BlockInfoEntry.newBuilder().setBlockId(blockId).setLength(length).build();
      journalContext.append(JournalEntry.newBuilder().setBlockInfo(blockInfo).build());
    }
  }

  @Override
  public BlockInfo getBlockInfo(long blockId) throws BlockInfoException, UnavailableException {
    return generateBlockInfo(blockId)
        .orElseThrow(() -> new BlockInfoException(ExceptionMessage.BLOCK_META_NOT_FOUND, blockId));
  }

  @Override
  public List<BlockInfo> getBlockInfoList(List<Long> blockIds) throws UnavailableException {
    List<BlockInfo> ret = new ArrayList<>(blockIds.size());
    for (long blockId : blockIds) {
      generateBlockInfo(blockId).ifPresent(info -> ret.add(info));
    }
    return ret;
  }

  @Override
  public Map<String, Long> getTotalBytesOnTiers() {
    Map<String, Long> ret = new HashMap<>();
    for (MasterWorkerInfo worker : mWorkers) {
      try (LockResource r = worker.lockWorkerMeta(EnumSet.of(WorkerMetaLockSection.USAGE), true)) {
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
      try (LockResource r = worker.lockWorkerMeta(
          EnumSet.of(WorkerMetaLockSection.USAGE), true)) {
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
   * This method requires no locking on {@link MasterWorkerInfo} because it is only
   * reading final fields.
   *
   * @param workerId the worker id to register
   */
  @Nullable
  private MasterWorkerInfo recordWorkerRegistration(long workerId) {
    for (IndexedSet<MasterWorkerInfo> workers: Arrays.asList(mTempWorkers, mLostWorkers)) {
      MasterWorkerInfo worker = workers.getFirstByField(ID_INDEX, workerId);
      if (worker == null) {
        continue;
      }

      mWorkers.add(worker);
      workers.remove(worker);
      if (workers == mLostWorkers) {
        for (Consumer<Address> function : mLostWorkerFoundListeners) {
          // The worker address is final, no need for locking here
          function.accept(new Address(worker.getWorkerAddress().getHost(),
              worker.getWorkerAddress().getRpcPort()));
        }
        LOG.warn("A lost worker {} has requested its old id {}.",
            worker.getWorkerAddress(), worker.getId());
      }

      return worker;
    }
    return null;
  }

  @Override
  public long getWorkerId(WorkerNetAddress workerNetAddress) {
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
      Map<BlockLocation, List<Long>> currentBlocksOnLocation,
      Map<String, StorageList> lostStorage, RegisterWorkerPOptions options)
      throws NotFoundException {

    MasterWorkerInfo worker = mWorkers.getFirstByField(ID_INDEX, workerId);

    if (worker == null) {
      worker = findUnregisteredWorker(workerId);
    }

    if (worker == null) {
      throw new NotFoundException(ExceptionMessage.NO_WORKER_FOUND.getMessage(workerId));
    }

    // Gather all blocks on this worker.
    HashSet<Long> blocks = new HashSet<>();
    for (List<Long> blockIds : currentBlocksOnLocation.values()) {
      blocks.addAll(blockIds);
    }

    // Lock all the locks
    try (LockResource r = worker.lockWorkerMeta(EnumSet.of(
        WorkerMetaLockSection.STATUS,
        WorkerMetaLockSection.USAGE,
        WorkerMetaLockSection.BLOCKS), false)) {
      // Detect any lost blocks on this worker.
      Set<Long> removedBlocks = worker.register(mGlobalStorageTierAssoc, storageTiers,
          totalBytesOnTiers, usedBytesOnTiers, blocks);
      processWorkerRemovedBlocks(worker, removedBlocks);
      processWorkerAddedBlocks(worker, currentBlocksOnLocation);
      processWorkerOrphanedBlocks(worker);
      worker.addLostStorage(lostStorage);
    }

    if (options.getConfigsCount() > 0) {
      for (BiConsumer<Address, List<ConfigProperty>> function : mWorkerRegisteredListeners) {
        WorkerNetAddress workerAddress = worker.getWorkerAddress();
        function.accept(new Address(workerAddress.getHost(), workerAddress.getRpcPort()),
            options.getConfigsList());
      }
    }

    recordWorkerRegistration(workerId);

    // Update the TS at the end of the process
    worker.updateLastUpdatedTimeMs();

    // Invalidate cache to trigger new build of worker info list
    mWorkerInfoCache.invalidate(WORKER_INFO_CACHE_KEY);
    LOG.info("registerWorker(): {}", worker);
  }

  @Override
  public Command workerHeartbeat(long workerId, Map<String, Long> capacityBytesOnTiers,
      Map<String, Long> usedBytesOnTiers, List<Long> removedBlockIds,
      Map<BlockLocation, List<Long>> addedBlocks,
      Map<String, StorageList> lostStorage,
      List<Metric> metrics) {
    MasterWorkerInfo worker = mWorkers.getFirstByField(ID_INDEX, workerId);
    if (worker == null) {
      LOG.warn("Could not find worker id: {} for heartbeat.", workerId);
      return Command.newBuilder().setCommandType(CommandType.Register).build();
    }

    // Update the TS before the heartbeat so even if the worker heartbeat processing
    // is time-consuming or triggers GC, the worker does not get marked as lost
    // by the LostWorkerDetectionHeartbeatExecutor
    worker.updateLastUpdatedTimeMs();

    // The address is final, no need for locking
    processWorkerMetrics(worker.getWorkerAddress().getHost(), metrics);

    Command workerCommand = null;
    try (LockResource r = worker.lockWorkerMeta(
        EnumSet.of(WorkerMetaLockSection.USAGE, WorkerMetaLockSection.BLOCKS), false)) {
      worker.addLostStorage(lostStorage);

      if (capacityBytesOnTiers != null) {
        worker.updateCapacityBytes(capacityBytesOnTiers);
      }
      worker.updateUsedBytes(usedBytesOnTiers);

      // Technically, 'worker' should be confirmed to still be in the data structure. Lost worker
      // detection can remove it. However, we are intentionally ignoring this race, since the worker
      // will just re-register regardless.

      processWorkerRemovedBlocks(worker, removedBlockIds);
      processWorkerAddedBlocks(worker, addedBlocks);
      List<Long> toRemoveBlocks = worker.getToRemoveBlocks();
      if (toRemoveBlocks.isEmpty()) {
        workerCommand = Command.newBuilder().setCommandType(CommandType.Nothing).build();
      } else {
        workerCommand = Command.newBuilder().setCommandType(CommandType.Free)
            .addAllData(toRemoveBlocks).build();
      }
    }

    // Update the TS again
    worker.updateLastUpdatedTimeMs();

    // Should not reach here
    Preconditions.checkNotNull(workerCommand, "Worker heartbeat response command is null!");

    return workerCommand;
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
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#BLOCKS} specified.
   * An exclusive lock is required.
   *
   * @param workerInfo The worker metadata object
   * @param removedBlockIds A list of block ids removed from the worker
   */
  private void processWorkerRemovedBlocks(MasterWorkerInfo workerInfo,
      Collection<Long> removedBlockIds) {
    for (long removedBlockId : removedBlockIds) {
      try (LockResource r = lockBlock(removedBlockId)) {
        Optional<BlockMeta> block = mBlockStore.getBlock(removedBlockId);
        if (block.isPresent()) {
          LOG.debug("Block {} is removed on worker {}.", removedBlockId, workerInfo.getId());
          mBlockStore.removeLocation(removedBlockId, workerInfo.getId());
          if (mBlockStore.getLocations(removedBlockId).size() == 0) {
            mLostBlocks.add(removedBlockId);
          }
        }
        // Remove the block even if its metadata has been deleted already.
        workerInfo.removeBlock(removedBlockId);
      }
    }
  }

  /**
   * Updates the worker and block metadata for blocks added to a worker.
   *
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#BLOCKS} specified.
   * An exclusive lock is required.
   *
   * @param workerInfo The worker metadata object
   * @param addedBlockIds A mapping from storage tier alias to a list of block ids added
   */
  private void processWorkerAddedBlocks(MasterWorkerInfo workerInfo,
      Map<BlockLocation, List<Long>> addedBlockIds) {
    long invalidBlockCount = 0;
    for (Map.Entry<BlockLocation, List<Long>> entry : addedBlockIds.entrySet()) {
      for (long blockId : entry.getValue()) {
        try (LockResource r = lockBlock(blockId)) {
          Optional<BlockMeta> block = mBlockStore.getBlock(blockId);
          if (block.isPresent()) {
            workerInfo.addBlock(blockId);
            BlockLocation location = entry.getKey();
            Preconditions.checkState(location.getWorkerId() == workerInfo.getId(),
                "BlockLocation has a different workerId %s from the request sender's workerId %s",
                location.getWorkerId(), workerInfo.getId());
            mBlockStore.addLocation(blockId, location);
            mLostBlocks.remove(blockId);
          } else {
            invalidBlockCount++;
            LOG.debug("Invalid block: {} from worker {}.", blockId,
                workerInfo.getWorkerAddress().getHost());
          }
        }
      }
    }
    if (invalidBlockCount > 0) {
      LOG.warn("{} invalid blocks found on worker {} in total", invalidBlockCount,
          workerInfo.getWorkerAddress().getHost());
    }
  }

  /**
   * Checks the blocks on the worker. For blocks not present in Alluxio anymore,
   * they will be marked to-be-removed from the worker.
   *
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#USAGE} specified.
   * A shared lock is required.
   *
   * @param workerInfo The worker metadata object
   */
  private void processWorkerOrphanedBlocks(MasterWorkerInfo workerInfo) {
    long orphanedBlockCount = 0;
    for (long block : workerInfo.getBlocks()) {
      if (!mBlockStore.getBlock(block).isPresent()) {
        orphanedBlockCount++;
        LOG.debug("Requesting delete for orphaned block: {} from worker {}.", block,
            workerInfo.getWorkerAddress().getHost());
        workerInfo.updateToRemovedBlock(true, block);
      }
    }
    if (orphanedBlockCount > 0) {
      LOG.warn("{} blocks marked as orphaned from worker {}", orphanedBlockCount,
          workerInfo.getWorkerAddress().getHost());
    }
  }

  @Override
  public boolean isBlockLost(long blockId) {
    return mLostBlocks.contains(blockId);
  }

  @Override
  public Iterator<Long> getLostBlocksIterator() {
    return mLostBlocks.iterator();
  }

  @Override
  public int getLostBlocksCount() {
    return mLostBlocks.size();
  }

  /**
   * Generates block info, including worker locations, for a block id.
   * This requires no locks on the {@link MasterWorkerInfo} because it is only reading
   * final fields.
   *
   * @param blockId a block id
   * @return optional block info, empty if the block does not exist
   */
  private Optional<BlockInfo> generateBlockInfo(long blockId) throws UnavailableException {
    if (mSafeModeManager.isInSafeMode()) {
      throw new UnavailableException(ExceptionMessage.MASTER_IN_SAFEMODE.getMessage());
    }

    BlockMeta block;
    List<BlockLocation> blockLocations;
    try (LockResource r = lockBlock(blockId)) {
      Optional<BlockMeta> blockOpt = mBlockStore.getBlock(blockId);
      if (!blockOpt.isPresent()) {
        return Optional.empty();
      }
      block = blockOpt.get();
      blockLocations = new ArrayList<>(mBlockStore.getLocations(blockId));
    }

    // Sort the block locations by their alias ordinal in the master storage tier mapping
    Collections.sort(blockLocations,
        Comparator.comparingInt(o -> mGlobalStorageTierAssoc.getOrdinal(o.getTier())));

    List<alluxio.wire.BlockLocation> locations = new ArrayList<>();
    for (BlockLocation location : blockLocations) {
      MasterWorkerInfo workerInfo =
          mWorkers.getFirstByField(ID_INDEX, location.getWorkerId());
      if (workerInfo != null) {
        // worker metadata is intentionally not locked here because:
        // - it would be an incorrect order (correct order is lock worker first, then block)
        // - only uses getters of final variables
        locations.add(new alluxio.wire.BlockLocation().setWorkerId(location.getWorkerId())
            .setWorkerAddress(workerInfo.getWorkerAddress())
            .setTierAlias(location.getTier()).setMediumType(location.getMediumType()));
      }
    }
    return Optional.of(
        new BlockInfo().setBlockId(blockId).setLength(block.getLength()).setLocations(locations));
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
      long masterWorkerTimeoutMs = ServerConfiguration.getMs(PropertyKey.MASTER_WORKER_TIMEOUT_MS);
      for (MasterWorkerInfo worker : mWorkers) {
        try (LockResource r = worker.lockWorkerMeta(
            EnumSet.of(WorkerMetaLockSection.BLOCKS), false)) {
          // This is not locking because the field is atomic
          final long lastUpdate = mClock.millis() - worker.getLastUpdatedTimeMs();
          if (lastUpdate > masterWorkerTimeoutMs) {
            LOG.error("The worker {}({}) timed out after {}ms without a heartbeat!", worker.getId(),
                worker.getWorkerAddress(), lastUpdate);
            processLostWorker(worker);
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
   * Forces all workers to be lost. This should only be used for testing.
   */
  @VisibleForTesting
  public void forgetAllWorkers() {
    for (MasterWorkerInfo worker : mWorkers) {
      try (LockResource r = worker.lockWorkerMeta(
          EnumSet.of(WorkerMetaLockSection.BLOCKS), false)) {
        processLostWorker(worker);
      }
    }
  }

  /**
   * Updates the metadata for the specified lost worker.
   *
   * You should lock externally with {@link MasterWorkerInfo#lockWorkerMeta(EnumSet, boolean)}
   * with {@link WorkerMetaLockSection#BLOCKS} specified.
   * An exclusive lock is required.
   *
   * @param worker the worker metadata
   */
  private void processLostWorker(MasterWorkerInfo worker) {
    mLostWorkers.add(worker);
    mWorkers.remove(worker);
    WorkerNetAddress workerAddress = worker.getWorkerAddress();
    for (Consumer<Address> function : mWorkerLostListeners) {
      function.accept(new Address(workerAddress.getHost(), workerAddress.getRpcPort()));
    }
    processWorkerRemovedBlocks(worker, worker.getBlocks());
  }

  LockResource lockBlock(long blockId) {
    return new LockResource(mBlockLocks.get(blockId));
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
    /**
     * Registers metric gauges.
     *
     * @param master the block master handle
     */
    @VisibleForTesting
    public static void registerGauges(final DefaultBlockMaster master) {
      MetricsSystem.registerGaugeIfAbsent(MetricKey.CLUSTER_CAPACITY_TOTAL.getName(),
          master::getCapacityBytes);

      MetricsSystem.registerGaugeIfAbsent(MetricKey.CLUSTER_CAPACITY_USED.getName(),
          master::getUsedBytes);

      MetricsSystem.registerGaugeIfAbsent(MetricKey.CLUSTER_CAPACITY_FREE.getName(),
          () -> master.getCapacityBytes() - master.getUsedBytes());

      MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_UNIQUE_BLOCKS.getName(),
          () -> master.mBlockStore.size());

      MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_TOTAL_BLOCK_REPLICA_COUNT.getName(),
          () -> master.getBlockReplicaCount());
      for (int i = 0; i < master.getGlobalStorageTierAssoc().size(); i++) {
        String alias = master.getGlobalStorageTierAssoc().getAlias(i);
        // TODO(lu) Add template to dynamically construct metric key
        MetricsSystem.registerGaugeIfAbsent(
            MetricKey.CLUSTER_CAPACITY_TOTAL.getName() + MetricInfo.TIER + alias,
            new Gauge<Long>() {
              @Override
              public Long getValue() {
                return master.getTotalBytesOnTiers().getOrDefault(alias, 0L);
              }
            });

        MetricsSystem.registerGaugeIfAbsent(
            MetricKey.CLUSTER_CAPACITY_USED.getName() + MetricInfo.TIER + alias, new Gauge<Long>() {
              @Override
              public Long getValue() {
                return master.getUsedBytesOnTiers().getOrDefault(alias, 0L);
              }
            });
        MetricsSystem.registerGaugeIfAbsent(
            MetricKey.CLUSTER_CAPACITY_FREE.getName() + MetricInfo.TIER + alias, new Gauge<Long>() {
              @Override
              public Long getValue() {
                return master.getTotalBytesOnTiers().getOrDefault(alias, 0L)
                    - master.getUsedBytesOnTiers().getOrDefault(alias, 0L);
              }
            });
      }

      MetricsSystem.registerGaugeIfAbsent(MetricKey.CLUSTER_WORKERS.getName(),
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return master.getWorkerCount();
            }
          });
      MetricsSystem.registerGaugeIfAbsent(MetricKey.CLUSTER_LOST_WORKERS.getName(),
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return master.getLostWorkerCount();
            }
          });
    }

    private Metrics() {} // prevent instantiation
  }

  private long getBlockReplicaCount() {
    long ret = 0;
    for (MasterWorkerInfo worker : mWorkers) {
      ret += worker.getBlockCount();
    }
    return ret;
  }
}
