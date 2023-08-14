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
import alluxio.DefaultStorageTierAssoc;
import alluxio.Server;
import alluxio.StorageTierAssoc;
import alluxio.client.block.options.GetWorkerReportOptions;
import alluxio.client.block.options.GetWorkerReportOptions.WorkerRange;
import alluxio.clock.SystemClock;
import alluxio.collections.ConcurrentHashSet;
import alluxio.collections.IndexDefinition;
import alluxio.collections.IndexedSet;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.BlockInfoException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.runtime.UnavailableRuntimeException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.exception.status.NotFoundException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.DecommissionWorkerPOptions;
import alluxio.grpc.GetRegisterLeasePRequest;
import alluxio.grpc.GrpcService;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.NodeState;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.RegisterWorkerPRequest;
import alluxio.grpc.RemoveDisabledWorkerPOptions;
import alluxio.grpc.ServiceType;
import alluxio.grpc.StorageList;
import alluxio.grpc.WorkerLostStorageInfo;
import alluxio.heartbeat.FixedIntervalSupplier;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.CoreMaster;
import alluxio.master.CoreMasterContext;
import alluxio.master.WorkerState;
import alluxio.master.block.meta.MasterWorkerInfo;
import alluxio.master.block.meta.WorkerMetaLockSection;
import alluxio.master.journal.JournalContext;
import alluxio.master.journal.SingleEntryJournaled;
import alluxio.master.journal.checkpoint.CheckpointName;
import alluxio.master.journal.checkpoint.Checkpointed;
import alluxio.master.metastore.BlockMetaStore;
import alluxio.master.metastore.BlockMetaStore.Block;
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
import alluxio.security.authentication.ClientContextServerInjector;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.executor.ExecutorServiceFactory;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.proto.BlockLocationUtils;
import alluxio.wire.Address;
import alluxio.wire.BlockInfo;
import alluxio.wire.RegisterLease;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Striped;
import io.grpc.ServerInterceptors;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This block master manages the metadata for all the blocks and block workers in Alluxio.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public class DefaultBlockMaster extends CoreMaster implements BlockMaster {
  private static final Set<Class<? extends Server>> DEPS =
      ImmutableSet.of(MetricsMaster.class);

  /**
   * The number of container ids to 'reserve' before having to journal container id state. This
   * allows the master to return container ids within the reservation, without having to write to
   * the journal.
   */
  private final long mContainerIdReservationSize = Configuration.getInt(
      PropertyKey.MASTER_CONTAINER_ID_RESERVATION_SIZE);

  /** The only valid key for {@link #mWorkerInfoCache}. */
  private static final String WORKER_INFO_CACHE_KEY = "WorkerInfoKey";

  private final ExecutorService mContainerIdDetector = Executors
      .newSingleThreadExecutor(
        ThreadFactoryUtils.build("default-block-master-container-id-detection-%d", true));

  private volatile boolean mContainerIdDetectorIsIdle = true;

  // Worker metadata management.
  private static final IndexDefinition<MasterWorkerInfo, Long> ID_INDEX =
      IndexDefinition.ofUnique(MasterWorkerInfo::getId);

  private static final IndexDefinition<MasterWorkerInfo, WorkerNetAddress> ADDRESS_INDEX =
      IndexDefinition.ofUnique(MasterWorkerInfo::getWorkerAddress);

  /**
   * Mapping between all possible storage level aliases and their ordinal position. This mapping
   * forms a total ordering on all storage level aliases in the system, and must be consistent
   * across masters.
   */
  private static final StorageTierAssoc MASTER_STORAGE_TIER_ASSOC =
      new DefaultStorageTierAssoc(
          PropertyKey.MASTER_TIERED_STORE_GLOBAL_LEVELS,
          PropertyKey.Template.MASTER_TIERED_STORE_GLOBAL_LEVEL_ALIAS);

  private static final Logger LOG = LoggerFactory.getLogger(DefaultBlockMaster.class);

  private static final String WORKER_DISABLED =
      "Worker with address %s is manually decommissioned and marked not able to join "
          + "the cluster again. If you want this worker to register to the cluster again, "
          + "use `bin/alluxio fsadmin enableWorker -h <workerHost>` command.";

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
  private final BlockMetaStore mBlockMetaStore;

  /** Keeps track of blocks which are no longer in Alluxio storage. */
  private final ConcurrentHashSet<Long> mLostBlocks = new ConcurrentHashSet<>(64, 0.90f, 64);

  /** This state must be journaled. */
  @GuardedBy("itself")
  private final BlockContainerIdGenerator mBlockContainerIdGenerator =
      new BlockContainerIdGenerator();

  /** Keeps track of workers which are in communication with the master. */
  private final IndexedSet<MasterWorkerInfo> mWorkers =
      new IndexedSet<>(ID_INDEX, ADDRESS_INDEX);
  /** Keeps track of workers which are no longer in communication with the master. */
  private final IndexedSet<MasterWorkerInfo> mLostWorkers =
      new IndexedSet<>(ID_INDEX, ADDRESS_INDEX);
  /** Worker is not visualable until registration completes. */
  private final IndexedSet<MasterWorkerInfo> mTempWorkers =
      new IndexedSet<>(ID_INDEX, ADDRESS_INDEX);
  private final Set<WorkerNetAddress> mRejectWorkers = new ConcurrentHashSet<>();
  /**
   * Keeps track of workers which have been decommissioned.
   * For we need to distinguish the lost worker accidentally and the decommissioned worker manually.
   */
  private final IndexedSet<MasterWorkerInfo> mDecommissionedWorkers =
      new IndexedSet<>(ID_INDEX, ADDRESS_INDEX);

  /**
   * Tracks the open register streams.
   * A stream will be closed if it is completed, aborted due to an error,
   * or recycled due to inactivity by {@link WorkerRegisterStreamGCExecutor}.
   */
  private final Map<Long, WorkerRegisterContext> mActiveRegisterContexts =
      new ConcurrentHashMap<>();

  /** Listeners to call when lost workers are found. */
  private final List<Consumer<Address>> mLostWorkerFoundListeners
      = new ArrayList<>();

  /** Listeners to call when workers are lost. */
  private final List<Consumer<Address>> mWorkerLostListeners = new ArrayList<>();

  /** Listeners to call when workers are delete. */
  private final List<Consumer<Address>> mWorkerDeleteListeners = new ArrayList<>();

  /** Listeners to call when a new worker registers. */
  private final List<BiConsumer<Address, List<ConfigProperty>>> mWorkerRegisteredListeners
      = new ArrayList<>();

  /** Handle to the metrics master. */
  private final MetricsMaster mMetricsMaster;

  /* The value of the 'next container id' last journaled. */
  @GuardedBy("mBlockContainerIdGenerator")
  private volatile long mJournaledNextContainerId = 0;

  /**
   * A loading cache for worker info list, refresh periodically.
   * This cache only has a single key {@link  #WORKER_INFO_CACHE_KEY}.
   */
  private final LoadingCache<String, List<WorkerInfo>> mWorkerInfoCache;

  private final RegisterLeaseManager mRegisterLeaseManager = new RegisterLeaseManager();

  private final HashMap<Long, WorkerNetAddress> mWorkerIdMap = new HashMap<>();

  private final boolean mWorkerRegisterToAllMasters = Configuration.getBoolean(
      PropertyKey.WORKER_REGISTER_TO_ALL_MASTERS);

  private final boolean mStandbyMasterRpcEnabled = Configuration.getBoolean(
      PropertyKey.STANDBY_MASTER_GRPC_ENABLED);

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

  private DefaultBlockMaster(MetricsMaster metricsMaster, CoreMasterContext masterContext,
      Clock clock, ExecutorServiceFactory executorServiceFactory, BlockMetaStore blockMetaStore) {
    super(masterContext, clock, executorServiceFactory);
    Preconditions.checkNotNull(metricsMaster, "metricsMaster");

    mBlockMetaStore = blockMetaStore;
    mMetricsMaster = metricsMaster;
    Metrics.registerGauges(this);

    mWorkerInfoCache = CacheBuilder.newBuilder()
        .refreshAfterWrite(Configuration
            .getMs(PropertyKey.MASTER_WORKER_INFO_CACHE_REFRESH_TIME), TimeUnit.MILLISECONDS)
        .build(new CacheLoader<String, List<WorkerInfo>>() {
          @Override
          public List<WorkerInfo> load(String key) {
            return constructWorkerInfoList();
          }
        });

    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_LOST_BLOCK_COUNT.getName(),
        this::getLostBlocksCount);
    MetricsSystem.registerCachedGaugeIfAbsent(MetricKey.MASTER_TO_REMOVE_BLOCK_COUNT.getName(),
        this::getToRemoveBlockCount, 30, TimeUnit.SECONDS);
  }

  /**
   * Creates a new instance of {@link DefaultBlockMaster}.
   * Used for tests where we manually control the clock.
   *
   * @param metricsMaster the metrics master
   * @param masterContext the context for Alluxio master
   * @param clock the clock to use for determining the time
   * @param executorServiceFactory a factory for creating the executor service to use for running
   *        maintenance threads
   */
  @VisibleForTesting
  public DefaultBlockMaster(MetricsMaster metricsMaster, CoreMasterContext masterContext,
      Clock clock, ExecutorServiceFactory executorServiceFactory) {
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
        new GrpcService(ServerInterceptors
            .intercept(new BlockMasterClientServiceHandler(this),
                new ClientContextServerInjector())));
    services.put(ServiceType.BLOCK_MASTER_WORKER_SERVICE,
        new GrpcService(ServerInterceptors
            .intercept(new BlockMasterWorkerServiceHandler(this),
                new ClientContextServerInjector())));
    return services;
  }

  @Override
  public Map<ServiceType, GrpcService> getStandbyServices() {
    if (Configuration.getBoolean(PropertyKey.WORKER_REGISTER_TO_ALL_MASTERS)) {
      return getServices();
    }
    return Collections.emptyMap();
  }

  @Override
  public boolean processJournalEntry(JournalEntry entry) {
    // TODO(gene): A better way to process entries besides a huge switch?
    if (entry.hasBlockContainerIdGenerator()) {
      mJournaledNextContainerId = (entry.getBlockContainerIdGenerator()).getNextContainerId();
      mBlockContainerIdGenerator.setNextContainerId((mJournaledNextContainerId));
    } else if (entry.hasDeleteBlock()) {
      mBlockMetaStore.removeBlock(entry.getDeleteBlock().getBlockId());
    } else if (entry.hasBlockInfo()) {
      BlockInfoEntry blockInfoEntry = entry.getBlockInfo();
      long length = blockInfoEntry.getLength();
      Optional<BlockMeta> block = mBlockMetaStore.getBlock(blockInfoEntry.getBlockId());
      if (block.isPresent()) {
        // If we write multiple replicas, multiple streams will all write BlockInfoEntry
        // when they CommitBlock. We rely on the idempotence to handle duplicate entries
        // and only warning when there are inconsistencies.
        long oldLen = block.get().getLength();
        if (oldLen != Constants.UNKNOWN_SIZE) {
          LOG.warn("Attempting to update block length ({}) to a different length ({}).", oldLen,
              length);
          return true;
        }
      }
      mBlockMetaStore.putBlock(blockInfoEntry.getBlockId(),
          BlockMeta.newBuilder().setLength(blockInfoEntry.getLength()).build());
      // This can be called when
      // 1. The master is replaying the journal.
      // 2. A standby master is applying a journal entry from the primary master.
      if (blockInfoEntry.hasBlockLocation()) {
        alluxio.grpc.BlockLocation blockLocation = blockInfoEntry.getBlockLocation();
        long workerId = blockLocation.getWorkerId();
        MasterWorkerInfo worker = mWorkers.getFirstByField(ID_INDEX, workerId);
        if (worker == null) {
          // The master is replaying journal or somehow the worker is not there anymore
          // We do not add the BlockLocation because the workerId is not reliable anymore
          // If the worker comes back, it will register and BlockLocation will be added then
          return true;
        }
        // The master is running and the journal is from an existing worker
        mBlockMetaStore.addLocation(blockInfoEntry.getBlockId(), BlockLocationUtils.getCached(
            workerId, blockLocation.getTierAlias(), blockLocation.getMediumType())
        );

        worker.addBlock(blockInfoEntry.getBlockId());
        LOG.debug("Added BlockLocation for {} to worker {}", blockInfoEntry.getBlockId(), workerId);
      }
    } else {
      return false;
    }
    return true;
  }

  @Override
  public void resetState() {
    mBlockMetaStore.clear();
    mJournaledNextContainerId = 0;
    mBlockContainerIdGenerator.setNextContainerId(0);
  }

  @Override
  public CheckpointName getCheckpointName() {
    if (mBlockMetaStore instanceof Checkpointed) {
      return ((Checkpointed) mBlockMetaStore).getCheckpointName();
    }
    return CheckpointName.BLOCK_MASTER;
  }

  @Override
  public CompletableFuture<Void> writeToCheckpoint(File directory,
                                                   ExecutorService executorService) {
    if (mBlockMetaStore instanceof Checkpointed) {
      SingleEntryJournaled containerIdJournal = new DefaultBlockMasterContainerIdJournaled();
      containerIdJournal.processJournalEntry(getContainerIdJournalEntry());
      return CompletableFuture.allOf((
          (Checkpointed) mBlockMetaStore).writeToCheckpoint(directory, executorService),
          containerIdJournal.writeToCheckpoint(directory, executorService));
    }
    return super.writeToCheckpoint(directory, executorService);
  }

  @Override
  public CompletableFuture<Void> restoreFromCheckpoint(File directory,
                                                       ExecutorService executorService) {
    if (mBlockMetaStore instanceof Checkpointed) {
      SingleEntryJournaled containerIdJournal = new DefaultBlockMasterContainerIdJournaled();
      return CompletableFuture.allOf((
          (Checkpointed) mBlockMetaStore).restoreFromCheckpoint(directory, executorService),
          containerIdJournal.restoreFromCheckpoint(directory, executorService)
              .thenRun(() -> processJournalEntry(containerIdJournal.getEntry())));
    }
    return super.restoreFromCheckpoint(directory, executorService);
  }

  @Override
  public CloseableIterator<JournalEntry> getJournalEntryIterator() {
    CloseableIterator<Block> blockStoreIterator = mBlockMetaStore.getCloseableIterator();
    Iterator<JournalEntry> journalIterator = new Iterator<JournalEntry>() {
      @Override
      public boolean hasNext() {
        return blockStoreIterator.hasNext();
      }

      @Override
      public JournalEntry next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        /*
         * When the BlockStore is RocksBlockMetaStore, thread safety is embedded in the iterator.
         * So no need to worry if the RocksDB is closed while this iterator is active.
         */
        Block block = blockStoreIterator.next();
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

    CloseableIterator<JournalEntry> journalCloseableIterator =
        CloseableIterator.create(journalIterator, (whatever) -> blockStoreIterator.close());

    return CloseableIterator.concat(
        CloseableIterator.noopCloseable(
            CommonUtils.singleElementIterator(getContainerIdJournalEntry())),
        journalCloseableIterator);
  }

  /**
   * Periodically checks the open worker register streams.
   * If a stream has been active for a while, close the stream, recycle resources and locks,
   * and propagate an error to the worker side.
   */
  public class WorkerRegisterStreamGCExecutor implements HeartbeatExecutor {
    private final long mTimeout = Configuration.global()
        .getMs(PropertyKey.MASTER_WORKER_REGISTER_STREAM_RESPONSE_TIMEOUT);

    @Override
    public void heartbeat(long timeLimitMs) {
      AtomicInteger removedSessions = new AtomicInteger(0);
      mActiveRegisterContexts.entrySet().removeIf((entry) -> {
        WorkerRegisterContext context = entry.getValue();
        final long clockTime = mClock.millis();
        final long lastActivityTime = context.getLastActivityTimeMs();
        final long staleTime = clockTime - lastActivityTime;
        if (staleTime < mTimeout) {
          return false;
        }
        String msg = String.format(
            "ClockTime: %d, LastActivityTime: %d. Worker %d register stream hanging for %sms!"
                + " Tune up %s if this is undesired.",
            clockTime, lastActivityTime, context.getWorkerInfo().getId(), staleTime,
            PropertyKey.MASTER_WORKER_REGISTER_STREAM_RESPONSE_TIMEOUT);
        Exception e = new TimeoutException(msg);
        try {
          context.closeWithError(e);
        } catch (Throwable t) {
          t.addSuppressed(e);
          LOG.error("Failed to close an open register stream for worker {}. "
              + "The stream has been open for {}ms.", context.getWorkerId(), staleTime, t);
          // Do not remove the entry so this will be retried
          return false;
        }
        removedSessions.getAndDecrement();
        return true;
      });
      if (removedSessions.get() > 0) {
        LOG.info("Removed {} stale worker registration streams", removedSessions.get());
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
    if (isLeader || mWorkerRegisterToAllMasters) {
      getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_LOST_WORKER_DETECTION, new LostWorkerDetectionHeartbeatExecutor(),
          () -> new FixedIntervalSupplier(
              Configuration.getMs(PropertyKey.MASTER_LOST_WORKER_DETECTION_INTERVAL)),
          Configuration.global(), mMasterContext.getUserState()));
    }

    // This periodically scans all open register streams and closes hanging ones
    getExecutorService().submit(new HeartbeatThread(
          HeartbeatContext.MASTER_WORKER_REGISTER_SESSION_CLEANER,
            new WorkerRegisterStreamGCExecutor(),
            () -> new FixedIntervalSupplier(Configuration.getMs(
                PropertyKey.MASTER_WORKER_REGISTER_STREAM_RESPONSE_TIMEOUT)),
            Configuration.global(), mMasterContext.getUserState()));
  }

  @Override
  public void stop() throws IOException {
    LOG.info("Next container id before close: {}", mBlockContainerIdGenerator.peekNewContainerId());
    super.stop();
  }

  @Override
  public void close() throws IOException {
    super.close();
    mBlockMetaStore.close();

    mContainerIdDetector.shutdown();
    try {
      mContainerIdDetector.awaitTermination(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Container id detection executor did not shut down in a timely manner: {}",
          e.toString());
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
  public int getDecommissionedWorkerCount() {
    return mDecommissionedWorkers.size();
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
  public long getUniqueBlockCount() {
    return mBlockMetaStore.size();
  }

  @Override
  public long getBlockReplicaCount() {
    long ret = 0;
    for (MasterWorkerInfo worker : mWorkers) {
      ret += worker.getBlockCount();
    }
    return ret;
  }

  @Override
  public StorageTierAssoc getGlobalStorageTierAssoc() {
    return MASTER_STORAGE_TIER_ASSOC;
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
    // TODO(jiacheng): investigate why this cache is refreshed so many times by the
    //  alluxio.master.scheduler.Scheduler L239
    List<WorkerInfo> workerInfoList = new ArrayList<>(mWorkers.size());
    for (MasterWorkerInfo worker : mWorkers) {
      // extractWorkerInfo handles the locking internally
      workerInfoList.add(extractWorkerInfo(worker,
          GetWorkerReportOptions.WorkerInfoField.ALL, WorkerState.LIVE));
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
      workerInfoList.add(extractWorkerInfo(worker,
          GetWorkerReportOptions.WorkerInfoField.ALL, WorkerState.LOST));
    }
    workerInfoList.sort(new WorkerInfo.LastContactSecComparator());
    return workerInfoList;
  }

  @Override
  public void removeDisabledWorker(RemoveDisabledWorkerPOptions requestOptions)
          throws NotFoundException {
    if (mStandbyMasterRpcEnabled && mPrimarySelector.getStateUnsafe() == NodeState.STANDBY) {
      throw new UnavailableRuntimeException(
          "RemoveDisabledWorker operation is not supported on standby masters");
    }
    String workerHostName = requestOptions.getWorkerHostname();
    long workerWebPort = requestOptions.getWorkerWebPort();
    AtomicBoolean found = new AtomicBoolean(false);
    mRejectWorkers.removeIf(entry -> {
      if (entry.getHost().equals(workerHostName) && entry.getWebPort() == workerWebPort) {
        LOG.info("Received admin command to re-accept worker {}. The worker should be "
            + "accepted to the cluster when it registers again.", entry);
        found.set(true);
        return true;
      }
      return false;
    });
    if (!found.get()) {
      LOG.info("Received admin command to re-accept worker {} but the worker is "
          + "not decommissioned. The worker will be able to register to the cluster normally. "
          + "No further action is required.", workerHostName);
    }
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
    Set<MasterWorkerInfo> selectedDecommissionedWorkers = new HashSet<>();
    WorkerRange workerRange = options.getWorkerRange();
    switch (workerRange) {
      case ALL:
        selectedLiveWorkers.addAll(mWorkers);
        selectedLostWorkers.addAll(mLostWorkers);
        selectedDecommissionedWorkers.addAll(mDecommissionedWorkers);
        break;
      case LIVE:
        selectedLiveWorkers.addAll(mWorkers);
        break;
      case LOST:
        selectedLostWorkers.addAll(mLostWorkers);
        break;
      case DECOMMISSIONED:
        selectedDecommissionedWorkers.addAll(mDecommissionedWorkers);
        break;
      case SPECIFIED:
        Set<String> addresses = options.getAddresses();
        Set<String> workerNames = new HashSet<>();

        selectedLiveWorkers = selectInfoByAddress(addresses, mWorkers, workerNames);
        selectedLostWorkers = selectInfoByAddress(addresses, mLostWorkers, workerNames);
        selectedDecommissionedWorkers = selectInfoByAddress(addresses,
            mDecommissionedWorkers, workerNames);

        if (!addresses.isEmpty()) {
          String info = String.format("Unrecognized worker names: %s%n"
                  + "Supported worker names: %s%n",
                  addresses, workerNames);
          throw new InvalidArgumentException(info);
        }
        break;
      default:
        throw new InvalidArgumentException("Unrecognized worker range: " + workerRange);
    }

    List<WorkerInfo> workerInfoList = new ArrayList<>(
        selectedLiveWorkers.size() + selectedLostWorkers.size()
            + selectedDecommissionedWorkers.size());
    for (MasterWorkerInfo worker : selectedLiveWorkers) {
      // extractWorkerInfo handles the locking internally
      if (mRejectWorkers.contains(worker.getWorkerAddress())) {
        workerInfoList.add(extractWorkerInfo(worker, options.getFieldRange(),
            WorkerState.DISABLED));
      } else {
        workerInfoList.add(extractWorkerInfo(worker, options.getFieldRange(), WorkerState.LIVE));
      }
    }
    for (MasterWorkerInfo worker : selectedLostWorkers) {
      // extractWorkerInfo handles the locking internally
      if (mRejectWorkers.contains(worker.getWorkerAddress())) {
        workerInfoList.add(extractWorkerInfo(worker, options.getFieldRange(),
            WorkerState.DISABLED));
      } else {
        workerInfoList.add(extractWorkerInfo(worker, options.getFieldRange(), WorkerState.LOST));
      }
    }
    for (MasterWorkerInfo worker : selectedDecommissionedWorkers) {
      // extractWorkerInfo handles the locking internally
      if (mRejectWorkers.contains(worker.getWorkerAddress())) {
        workerInfoList.add(extractWorkerInfo(worker, options.getFieldRange(),
            WorkerState.DISABLED));
      } else {
        workerInfoList.add(extractWorkerInfo(worker, options.getFieldRange(),
            WorkerState.DECOMMISSIONED));
      }
    }
    return workerInfoList;
  }

  /**
   * Locks the {@link MasterWorkerInfo} properly and convert it to a {@link WorkerInfo}.
   */
  private WorkerInfo extractWorkerInfo(MasterWorkerInfo worker,
      Set<GetWorkerReportOptions.WorkerInfoField> fieldRange, WorkerState workerState) {
    try (LockResource r = worker.lockWorkerMetaForInfo(fieldRange)) {
      return worker.generateWorkerInfo(fieldRange, workerState);
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
  public void removeBlocks(Collection<Long> blockIds, boolean delete) throws UnavailableException {
    try (JournalContext journalContext = createJournalContext()) {
      for (long blockId : blockIds) {
        Set<Long> workerIds;
        try (LockResource r = lockBlock(blockId)) {
          Optional<BlockMeta> block = mBlockMetaStore.getBlock(blockId);
          if (!block.isPresent()) {
            continue;
          }
          List<BlockLocation> locations = mBlockMetaStore.getLocations(blockId);
          workerIds = new HashSet<>(locations.size());
          for (BlockLocation loc : locations) {
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
            mBlockMetaStore.removeBlock(blockId);
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
          // No need to update if the worker is lost or decommissioned
          // When that lost/decommissioned worker registers again, those removed blocks
          // will not be recognized, and the master will instruct the worker to remove them anyway
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
  public boolean isRejected(WorkerNetAddress address) {
    return mRejectWorkers.contains(address);
  }

  @Override
  public void decommissionWorker(DecommissionWorkerPOptions requestOptions)
      throws NotFoundException {
    String workerHostName = requestOptions.getWorkerHostname();
    long workerWebPort = requestOptions.getWorkerWebPort();
    boolean canRegisterAgain = requestOptions.getCanRegisterAgain();
    LOG.info("Decommissioning worker {}:{}", requestOptions.getWorkerHostname(),
        requestOptions.getWorkerWebPort());
    for (MasterWorkerInfo workerInfo : mWorkers) {
      WorkerNetAddress address = workerInfo.getWorkerAddress();
      if (workerHostName.equals(address.getHost()) && workerWebPort == address.getWebPort()) {
        LOG.info("Found worker to decommission {}", workerInfo.getWorkerAddress());
        try (LockResource r = workerInfo.lockWorkerMeta(
            EnumSet.of(WorkerMetaLockSection.BLOCKS), false)) {
          processDecommissionedWorker(workerInfo, canRegisterAgain);
        }
        LOG.info("Worker {}@{}:{} has been added to the decommissionedWorkers set.",
            workerInfo.getId(), workerHostName, workerWebPort);
        return;
      }
    }
    // The worker is not active, but it has been decommissioned from a previous call
    for (MasterWorkerInfo workerInfo : mDecommissionedWorkers) {
      WorkerNetAddress address = workerInfo.getWorkerAddress();
      if (workerHostName.equals(address.getHost()) && workerWebPort == address.getWebPort()) {
        LOG.info("Worker {}@{}:{} has been decommissioned already",
            workerInfo.getId(), workerHostName, workerWebPort);
        return;
      }
    }
    // If the worker is about to register, it may register back even if we decommission it
    // here. So we let the admin wait until the worker is registered, to reduce the number of
    // states to manage.
    for (MasterWorkerInfo workerInfo : mTempWorkers) {
      WorkerNetAddress address = workerInfo.getWorkerAddress();
      if (workerHostName.equals(address.getHost()) && workerWebPort == address.getWebPort()) {
        throw new NotFoundException(ExceptionMessage.WORKER_DECOMMISSIONED_BEFORE_REGISTER
            .getMessage(workerHostName + ":" + workerWebPort));
      }
    }
    // If the worker is lost, we guess it is more likely that the worker will not come back
    // immediately
    for (MasterWorkerInfo workerInfo : mLostWorkers) {
      WorkerNetAddress address = workerInfo.getWorkerAddress();
      if (workerHostName.equals(address.getHost()) && workerWebPort == address.getWebPort()) {
        LOG.info("Found worker to decommission {} from lost workers",
            workerInfo.getWorkerAddress());
        try (LockResource r = workerInfo.lockWorkerMeta(
            EnumSet.of(WorkerMetaLockSection.BLOCKS), false)) {
          processDecommissionedWorker(workerInfo, canRegisterAgain);
        }
        LOG.info("A lost worker {}@{}:{} has been added to the decommissionedWorkers set.",
            workerInfo.getId(), workerHostName, workerWebPort);
        return;
      }
    }
    throw new NotFoundException(ExceptionMessage.WORKER_NOT_FOUND
        .getMessage(workerHostName + ":" + workerWebPort));
  }

  /**
   * @return mJournaledNextContainerId
   */
  @Override
  public long getJournaledNextContainerId() {
    return mJournaledNextContainerId;
  }

  /**
   * @return a new block container id
   */
  @Override
  public long getNewContainerId() throws UnavailableException {
    long containerId = mBlockContainerIdGenerator.getNewContainerId();
    if (containerId >= (mJournaledNextContainerId - mContainerIdReservationSize / 2)) {
      if (containerId >= mJournaledNextContainerId) {
        synchronized (mBlockContainerIdGenerator) {
          // This container id is not safe with respect to the last journaled container id.
          // Therefore, journal the new state of the container id. This implies that when a master
          // crashes, the container ids within the reservation which have not been used yet will
          // never be used. This is a tradeoff between fully utilizing the container id space, vs.
          // improving master scalability.

          // Set the next id to journal with a reservation of container ids, to avoid having to
          // write to the journal for ids within the reservation.
          long possibleMaxContainerId = mBlockContainerIdGenerator.getNextContainerId();
          if (possibleMaxContainerId >= mJournaledNextContainerId) {
            mJournaledNextContainerId = possibleMaxContainerId + mContainerIdReservationSize;
            try (JournalContext journalContext = createJournalContext()) {
              // This must be flushed while holding the lock on mBlockContainerIdGenerator, in
              // order to prevent subsequent calls to return ids that have not been journaled
              // and flushed.
              journalContext.append(getContainerIdJournalEntry());
            }
          }
        }
      } else {
        if (mContainerIdDetectorIsIdle) {
          synchronized (mBlockContainerIdGenerator) {
            if (mContainerIdDetectorIsIdle) {
              mContainerIdDetectorIsIdle = false;
              mContainerIdDetector.submit(() -> {
                try {
                  synchronized (mBlockContainerIdGenerator) {
                    long possibleMaxContainerId = mBlockContainerIdGenerator.getNextContainerId();

                    if (possibleMaxContainerId
                        >= (mJournaledNextContainerId - mContainerIdReservationSize / 2)) {
                      mJournaledNextContainerId = possibleMaxContainerId
                          + mContainerIdReservationSize;
                      try (JournalContext journalContext = createJournalContext()) {
                        journalContext.append(getContainerIdJournalEntry());
                      }
                    }
                  }
                } catch (UnavailableException e) {
                  LOG.error("Container Id Detector failed", e);
                }

                mContainerIdDetectorIsIdle = true;
              });
            }
          }
        }
      }
    }

    return containerId;
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
      /*
       * If the worker is not recognized:
       * 1. [Probably] The worker has been decommissioned and removed from the active worker list
       * 2. [Possible] The worker has not finished its register process. Maybe the master has
       *    failed over and the worker has not registered to this new primary.
       * 3. [Unlikely] The worker does not belong to this cluster and has never registered.
       *    This is unlikely because the worker has an ID and it must be from some master.
       * 4. [Unlikely] The worker is lost to the master. This is unlikely because the CommitBlock
       *    call is from the worker. This is more possibly the master is busy and did not
       *    handle the worker's heartbeat message for too long.
       */
      worker = mDecommissionedWorkers.getFirstByField(ID_INDEX, workerId);
      if (worker == null) {
        throw new NotFoundException(ExceptionMessage.NO_WORKER_FOUND.getMessage(workerId));
      } else {
        WorkerNetAddress addr = worker.getWorkerAddress();
        LOG.info("Committing blocks from a decommissioned worker {}",
            addr.getHost() + ":" + addr.getRpcPort());
        /*
         * Even though the worker is now decommissioned, the master still accepts the block
         * and updates the BlockLocation normally.
         * Updating the BlockLocation is not strictly necessary, because when the worker
         * registers again after restart, all locations will be rebuilt.
         * But for simplicity, the location is still updated.
         * A disabled worker is allowed to commit block, so ongoing operations will succeed.
         */
      }
    }

    try (JournalContext journalContext = createJournalContext()) {
      // Lock the worker metadata here to preserve the lock order
      // The worker metadata must be locked before the blocks
      try (LockResource lr = worker.lockWorkerMeta(
          EnumSet.of(WorkerMetaLockSection.USAGE, WorkerMetaLockSection.BLOCKS), false)) {
        try (LockResource r = lockBlock(blockId)) {
          Optional<BlockMeta> block = mBlockMetaStore.getBlock(blockId);
          if (!block.isPresent() || block.get().getLength() != length) {
            if (block.isPresent() && block.get().getLength() != Constants.UNKNOWN_SIZE) {
              LOG.warn("Rejecting attempt to change block length from {} to {}",
                  block.get().getLength(), length);
            } else {
              mBlockMetaStore.putBlock(blockId, BlockMeta.newBuilder().setLength(length).build());
              BlockInfoEntry.Builder blockInfoBuilder =
                  BlockInfoEntry.newBuilder().setBlockId(blockId).setLength(length);
              if (mWorkerRegisterToAllMasters) {
                blockInfoBuilder
                    .setBlockId(blockId)
                    .setLength(length)
                    .setBlockLocation(
                        alluxio.grpc.BlockLocation.newBuilder()
                            .setWorkerId(workerId)
                            .setMediumType(mediumType)
                            .setTierAlias(tierAlias)
                            // Worker addresses are not journaled because adding a block location
                            // into the meta store only needs a worker id.
                            .build()
                    );
              }
              journalContext.append(
                  JournalEntry.newBuilder().setBlockInfo(blockInfoBuilder.build()).build());
            }
          }
          // Update the block metadata with the new worker location.
          mBlockMetaStore.addLocation(blockId, BlockLocationUtils.getCached(
              workerId, tierAlias, mediumType));
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
  public void commitBlockInUFS(long blockId, long length, JournalContext journalContext) {
    LOG.debug("Commit block in ufs. blockId: {}, length: {}", blockId, length);
    try (LockResource r = lockBlock(blockId)) {
      if (mBlockMetaStore.getBlock(blockId).isPresent()) {
        // Block metadata already exists, so do not need to create a new one.
        return;
      }
      mBlockMetaStore.putBlock(blockId, BlockMeta.newBuilder().setLength(length).build());
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
      generateBlockInfo(blockId).ifPresent(ret::add);
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
    for (IndexedSet<MasterWorkerInfo> workers: Arrays.asList(mTempWorkers,
        mLostWorkers, mDecommissionedWorkers)) {
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
    for (IndexedSet<MasterWorkerInfo> workers: Arrays.asList(mTempWorkers,
        mLostWorkers, mDecommissionedWorkers)) {
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
  protected MasterWorkerInfo recordWorkerRegistration(long workerId) {
    for (IndexedSet<MasterWorkerInfo> workers: Arrays.asList(mTempWorkers,
        mLostWorkers, mDecommissionedWorkers)) {
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
    if (mStandbyMasterRpcEnabled && mPrimarySelector.getStateUnsafe() == NodeState.STANDBY) {
      throw new UnavailableRuntimeException(
          "GetWorkerId operation is not supported on standby masters");
    }
    if (isRejected(workerNetAddress)) {
      String msg = String.format(WORKER_DISABLED, workerNetAddress);
      LOG.warn("{}", msg);
      throw new UnavailableRuntimeException(msg);
    }
    LOG.info("Worker {} requesting for an ID", workerNetAddress);
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
  public void notifyWorkerId(long workerId, WorkerNetAddress workerNetAddress) {
    MasterWorkerInfo existingWorker = mWorkers.getFirstByField(ID_INDEX, workerId);
    if (existingWorker != null) {
      LOG.warn("A registered worker {} comes again from {}",
          workerId, existingWorker.getWorkerAddress());
      return;
    }

    existingWorker = findUnregisteredWorker(workerId);
    if (existingWorker != null) {
      LOG.warn("An unregistered worker {} comes again from {}",
          workerId, existingWorker.getWorkerAddress());
      return;
    }

    if (!mTempWorkers.add(new MasterWorkerInfo(workerId, workerNetAddress))) {
      throw new RuntimeException("Duplicated worker ID for " + workerId + ": " + workerNetAddress);
    }
    LOG.info("notifyWorkerId(): WorkerNetAddress: {} id: {}", workerNetAddress, workerId);
  }

  @Override
  public Optional<RegisterLease> tryAcquireRegisterLease(GetRegisterLeasePRequest request) {
    return mRegisterLeaseManager.tryAcquireLease(request);
  }

  @Override
  public boolean hasRegisterLease(long workerId) {
    return mRegisterLeaseManager.hasLease(workerId);
  }

  @Override
  public void releaseRegisterLease(long workerId) {
    mRegisterLeaseManager.releaseLease(workerId);
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
    if (isRejected(worker.getWorkerAddress())) {
      throw new UnavailableRuntimeException(String.format(WORKER_DISABLED,
          worker.getWorkerAddress()));
    }

    worker.setBuildVersion(options.getBuildVersion());

    // Gather all blocks on this worker.
    int totalSize = currentBlocksOnLocation.values().stream().mapToInt(List::size).sum();
    Set<Long> blocks = new LongOpenHashSet(totalSize);
    for (List<Long> blockIds : currentBlocksOnLocation.values()) {
      blocks.addAll(blockIds);
    }

    // Lock all the locks
    try (LockResource r = worker.lockWorkerMeta(EnumSet.of(
        WorkerMetaLockSection.STATUS,
        WorkerMetaLockSection.USAGE,
        WorkerMetaLockSection.BLOCKS), false)) {
      // Detect any lost blocks on this worker.
      Set<Long> removedBlocks = worker.register(MASTER_STORAGE_TIER_ASSOC, storageTiers,
          totalBytesOnTiers, usedBytesOnTiers, blocks);
      processWorkerRemovedBlocks(worker, removedBlocks, false);
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
  public MasterWorkerInfo getWorker(long workerId) throws NotFoundException {
    MasterWorkerInfo worker = mWorkers.getFirstByField(ID_INDEX, workerId);

    if (worker == null) {
      worker = findUnregisteredWorker(workerId);
    }

    if (worker == null) {
      throw new NotFoundException(ExceptionMessage.NO_WORKER_FOUND.getMessage(workerId));
    }

    return worker;
  }

  private MasterWorkerInfo getLiveOrDecommissionedWorker(long workerId) {
    MasterWorkerInfo worker = mWorkers.getFirstByField(ID_INDEX, workerId);
    if (worker != null) {
      return worker;
    }
    // If not found in the decommissioned worker, this returns null
    return mDecommissionedWorkers.getFirstByField(ID_INDEX, workerId);
  }

  private void processDecommissionedWorkerBlocks(MasterWorkerInfo workerInfo) {
    processWorkerRemovedBlocks(workerInfo, workerInfo.getBlocks(), false);
  }

  /**
   * Updates the metadata for the specified decommissioned worker.
   * @param worker the master worker info
   */
  private void processDecommissionedWorker(MasterWorkerInfo worker, boolean canRegisterAgain) {
    WorkerNetAddress address = worker.getWorkerAddress();
    if (canRegisterAgain) {
      LOG.info("Worker with address {} is decommissioned but will be accepted when it "
          + "registers again.", address);
    } else {
      LOG.info("Worker with address {} will be rejected on register/heartbeat", address);
      mRejectWorkers.add(address);
    }

    mDecommissionedWorkers.add(worker);
    // Remove worker from all other possible states
    mWorkers.remove(worker);
    mTempWorkers.remove(worker);
    mLostWorkers.remove(worker);
    // Invalidate cache to trigger new build of worker info list
    mWorkerInfoCache.invalidate(WORKER_INFO_CACHE_KEY);

    WorkerNetAddress workerNetAddress = worker.getWorkerAddress();
    // TODO(bzheng888): Maybe need a new listener such as WorkerDecommissionListener.
    for (Consumer<Address> function : mWorkerLostListeners) {
      function.accept(new Address(workerNetAddress.getHost(), workerNetAddress.getRpcPort()));
    }
    processDecommissionedWorkerBlocks(worker);
  }

  @Override
  public void workerRegisterStream(WorkerRegisterContext context,
                                  RegisterWorkerPRequest chunk, boolean isFirstMsg) {
    // TODO(jiacheng): find a place to check the lease
    if (isFirstMsg) {
      workerRegisterStart(context, chunk);
    } else {
      workerRegisterBatch(context, chunk);
    }
  }

  protected void workerRegisterStart(WorkerRegisterContext context,
      RegisterWorkerPRequest chunk) {
    MasterWorkerInfo workerInfo = context.getWorkerInfo();
    Preconditions.checkState(workerInfo != null,
        "No workerInfo metadata found in the WorkerRegisterContext!");
    if (isRejected(workerInfo.getWorkerAddress())) {
      throw new UnavailableRuntimeException(String.format(WORKER_DISABLED,
          workerInfo.getWorkerAddress()));
    }
    final List<String> storageTiers = chunk.getStorageTiersList();
    final Map<String, Long> totalBytesOnTiers = chunk.getTotalBytesOnTiersMap();
    final Map<String, Long> usedBytesOnTiers = chunk.getUsedBytesOnTiersMap();
    final Map<String, StorageList> lostStorage = chunk.getLostStorageMap();
    final Map<alluxio.proto.meta.Block.BlockLocation, List<Long>> currentBlocksOnLocation =
        BlockMasterWorkerServiceHandler.reconstructBlocksOnLocationMap(
            chunk.getCurrentBlocksList(), context.getWorkerId());
    RegisterWorkerPOptions options = chunk.getOptions();
    mActiveRegisterContexts.put(workerInfo.getId(), context);

    // The workerInfo is locked so we can operate on its blocks without race conditions
    // We start with assuming all blocks in (mBlocks + mToRemoveBlocks) do not exist.
    // With each batch we receive, we mark them not-to-be-removed.
    // Eventually what's left in the mToRemove will be the ones that do not exist anymore.
    workerInfo.markAllBlocksToRemove();
    workerInfo.updateUsage(MASTER_STORAGE_TIER_ASSOC, storageTiers,
        totalBytesOnTiers, usedBytesOnTiers);
    processWorkerAddedBlocks(workerInfo, currentBlocksOnLocation);
    processWorkerOrphanedBlocks(workerInfo);
    workerInfo.addLostStorage(lostStorage);
    workerInfo.setBuildVersion(options.getBuildVersion());

    // TODO(jiacheng): This block can be moved to a non-locked section
    if (options.getConfigsCount() > 0) {
      for (BiConsumer<Address, List<ConfigProperty>> function : mWorkerRegisteredListeners) {
        WorkerNetAddress workerAddress = workerInfo.getWorkerAddress();
        function.accept(new Address(workerAddress.getHost(), workerAddress.getRpcPort()),
                options.getConfigsList());
      }
    }
  }

  protected void workerRegisterBatch(WorkerRegisterContext context, RegisterWorkerPRequest chunk) {
    final Map<alluxio.proto.meta.Block.BlockLocation, List<Long>> currentBlocksOnLocation =
            BlockMasterWorkerServiceHandler.reconstructBlocksOnLocationMap(
                chunk.getCurrentBlocksList(), context.getWorkerId());
    MasterWorkerInfo workerInfo = context.getWorkerInfo();
    Preconditions.checkState(workerInfo != null,
        "No workerInfo metadata found in the WorkerRegisterContext!");
    if (isRejected(workerInfo.getWorkerAddress())) {
      throw new UnavailableRuntimeException(String.format(WORKER_DISABLED,
          workerInfo.getWorkerAddress()));
    }
    // Even if we add the BlockLocation before the workerInfo is fully registered,
    // it should be fine because the block can be read on this workerInfo.
    // If the stream fails in the middle, the blocks recorded on the MasterWorkerInfo
    // will be removed by processLostWorker()
    processWorkerAddedBlocks(workerInfo, currentBlocksOnLocation);

    processWorkerOrphanedBlocks(workerInfo);

    // Update the TS at the end of the process
    workerInfo.updateLastUpdatedTimeMs();
  }

  @Override
  public void workerRegisterFinish(WorkerRegisterContext context) {
    MasterWorkerInfo workerInfo = context.getWorkerInfo();
    Preconditions.checkState(workerInfo != null,
        "No workerInfo metadata found in the WorkerRegisterContext!");
    if (isRejected(workerInfo.getWorkerAddress())) {
      throw new UnavailableRuntimeException(String.format(WORKER_DISABLED,
          workerInfo.getWorkerAddress()));
    }
    // Detect any lost blocks on this workerInfo.
    Set<Long> removedBlocks;
    if (workerInfo.mIsRegistered) {
      // This is a re-register of an existing workerInfo. Assume the new block ownership data is
      // more up-to-date and update the existing block information.
      LOG.info("re-registering an existing workerId: {}", workerInfo.getId());

      // The toRemoveBlocks field now contains all the updates
      // after all the blocks have been processed.
      removedBlocks = workerInfo.getToRemoveBlocks();
    } else {
      removedBlocks = Collections.emptySet();
    }
    LOG.info("Found {} blocks to remove from the workerInfo", removedBlocks.size());
    processWorkerRemovedBlocks(workerInfo, removedBlocks, true);

    // Mark registered successfully
    workerInfo.mIsRegistered = true;
    recordWorkerRegistration(workerInfo.getId());

    // Update the TS at the end of the process
    workerInfo.updateLastUpdatedTimeMs();

    // Invalidate cache to trigger new build of workerInfo info list
    mWorkerInfoCache.invalidate(WORKER_INFO_CACHE_KEY);
    LOG.info("Worker successfully registered: {}", workerInfo);
    mActiveRegisterContexts.remove(workerInfo.getId());
    mRegisterLeaseManager.releaseLease(workerInfo.getId());
  }

  @Override
  public Command workerHeartbeat(long workerId, Map<String, Long> capacityBytesOnTiers,
      Map<String, Long> usedBytesOnTiers, List<Long> removedBlockIds,
      Map<BlockLocation, List<Long>> addedBlocks,
      Map<String, StorageList> lostStorage,
      List<Metric> metrics) {
    MasterWorkerInfo worker = mWorkers.getFirstByField(ID_INDEX, workerId);
    if (worker == null) {
      /*
       * If the worker is not recognized:
       * 1. The worker never registered to the cluster, or the master has restarted/failover
       * 2. The worker has been decommissioned and removed from the active worker list
       */
      worker = mDecommissionedWorkers.getFirstByField(ID_INDEX, workerId);
      if (worker != null) {
        WorkerNetAddress workerAddr = worker.getWorkerAddress();
        if (isRejected(worker.getWorkerAddress())) {
          LOG.info("Received heartbeat from a disabled worker {}",
              workerAddr.getHost() + ":" + workerAddr.getRpcPort());
          return Command.newBuilder().setCommandType(CommandType.Disabled).build();
        }
        LOG.info("Received heartbeat from a decommissioned worker {}",
            workerAddr.getHost() + ":" + workerAddr.getRpcPort());
        return Command.newBuilder().setCommandType(CommandType.Decommissioned).build();
      }
      LOG.warn("Could not find worker id: {} for heartbeat.", workerId);
      return Command.newBuilder().setCommandType(CommandType.Register).build();
    }
    if (isRejected(worker.getWorkerAddress())) {
      throw new UnavailableRuntimeException(String.format(WORKER_DISABLED,
          worker.getWorkerAddress()));
    }

    // Update the TS before the heartbeat so even if the worker heartbeat processing
    // is time-consuming or triggers GC, the worker does not get marked as lost
    // by the LostWorkerDetectionHeartbeatExecutor
    worker.updateLastUpdatedTimeMs();

    /*
     * In 2.x, a standby master needs to wait for the journal entry for those blocks to arrive
     * before it can process a worker heartbeat.
     * In 3.x since the block info are not written and propagated by journal to a standby master,
     * this wait is unnecessary.
     * The code is kept around just in case (until later we are sure about removing it).
     */
    // if (mWorkerRegisterToAllMasters && mPrimarySelector.getState() == NodeState.STANDBY) {
    //   waitBlockIdPresent(
    //       addedBlocks.values().stream().flatMap(Collection::stream)
    //           .collect(Collectors.toList()), workerId);
    // }

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

      processWorkerRemovedBlocks(worker, removedBlockIds, false);
      processWorkerAddedBlocks(worker, addedBlocks);
      Set<Long> toRemoveBlocks = worker.getToRemoveBlocks();
      if (toRemoveBlocks.isEmpty() || mPrimarySelector.getStateUnsafe() == NodeState.STANDBY) {
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

  /**
   * Waits for the block id being presents.
   * If workers register to standby masters, when a block is created,
   * heartbeats might come before the standby applies the journal.
   * To prevent this, we wait as best efforts before ignore unknown block ids.
   */
  private void waitBlockIdPresent(Collection<Long> blockIds, long workerId) {
    final List<Long> blockIdsToWait = new ArrayList<>();
    for (long addedBlockId : blockIds) {
      if (!mBlockMetaStore.getBlock(addedBlockId).isPresent()) {
        blockIdsToWait.add(addedBlockId);
      }
    }
    try {
      CommonUtils.waitFor(
          "Wait for blocks being committed on master before adding block locations",
          () -> {
            for (long blockId: blockIdsToWait) {
              if (!mBlockMetaStore.getBlock(blockId).isPresent()) {
                return false;
              }
            }
            return true;
          },
          WaitForOptions.defaults().setInterval(200).setTimeoutMs(1000)
      );
    } catch (InterruptedException | TimeoutException e) {
      StringBuilder sb = new StringBuilder();
      sb.append("[");
      for (long blockIdToWait : blockIdsToWait) {
        if (!mBlockMetaStore.getBlock(blockIdToWait).isPresent()) {
          sb.append(blockIdToWait);
          sb.append(" ,");
        }
      }
      sb.append("]");
      LOG.warn("Adding block ids {} for worker {} but these blocks don't exist. "
          + "These blocks will be ignored", sb, workerId);
    }
  }

  @Override
  public Clock getClock() {
    return mClock;
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
      Collection<Long> removedBlockIds, boolean sendCommand) {
    for (long removedBlockId : removedBlockIds) {
      try (LockResource r = lockBlock(removedBlockId)) {
        Optional<BlockMeta> block = mBlockMetaStore.getBlock(removedBlockId);
        if (block.isPresent()) {
          LOG.debug("Block {} is removed on worker {}.", removedBlockId, workerInfo.getId());
          mBlockMetaStore.removeLocation(removedBlockId, workerInfo.getId());
          if (mBlockMetaStore.getLocations(removedBlockId).size() == 0) {
            mLostBlocks.add(removedBlockId);
          }
        }
        // Remove the block even if its metadata has been deleted already.
        if (sendCommand) {
          workerInfo.scheduleRemoveFromWorker(removedBlockId);
        } else {
          workerInfo.removeBlockFromWorkerMeta(removedBlockId);
        }
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
          Optional<BlockMeta> block = mBlockMetaStore.getBlock(blockId);
          if (block.isPresent()) {
            workerInfo.addBlock(blockId);
            BlockLocation location = entry.getKey();
            Preconditions.checkState(location.getWorkerId() == workerInfo.getId(),
                "BlockLocation has a different workerId %s from the request sender's workerId %s",
                location.getWorkerId(), workerInfo.getId());
            mBlockMetaStore.addLocation(blockId, BlockLocationUtils.getCached(location));
            mLostBlocks.remove(blockId);
          } else {
            invalidBlockCount++;
            // The block is not recognized and should therefore be purged from the worker
            // The file may have been removed when the worker was lost
            workerInfo.scheduleRemoveFromWorker(blockId);
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
      if (!mBlockMetaStore.getBlock(block).isPresent()) {
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

  private long getToRemoveBlockCount() {
    long ret = 0;
    for (MasterWorkerInfo worker : mWorkers) {
      try (LockResource r = worker.lockWorkerMeta(
          EnumSet.of(WorkerMetaLockSection.BLOCKS), true)) {
        ret += worker.getToRemoveBlockCount();
      }
    }
    return ret;
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
      Optional<BlockMeta> blockOpt = mBlockMetaStore.getBlock(blockId);
      if (!blockOpt.isPresent()) {
        return Optional.empty();
      }
      block = blockOpt.get();
      blockLocations = new ArrayList<>(mBlockMetaStore.getLocations(blockId));
    }

    // Sort the block locations by their alias ordinal in the master storage tier mapping
    blockLocations.sort(Comparator.comparingInt(
            o -> MASTER_STORAGE_TIER_ASSOC.getOrdinal(o.getTier())));

    List<alluxio.wire.BlockLocation> locations = new ArrayList<>(blockLocations.size());
    for (BlockLocation location : blockLocations) {
      // Decommissioned workers are not included in the available locations
      // Note that this may introduce a short unavailability on the block, before
      // this worker registers again (and wipes out the decommissioned state).
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
  public final class LostWorkerDetectionHeartbeatExecutor implements HeartbeatExecutor {

    /**
     * Constructs a new {@link LostWorkerDetectionHeartbeatExecutor}.
     */
    public LostWorkerDetectionHeartbeatExecutor() {}

    @Override
    public void heartbeat(long timeLimitMs) {
      long masterWorkerTimeoutMs = Configuration.getMs(PropertyKey.MASTER_WORKER_TIMEOUT_MS);
      long masterWorkerDeleteTimeoutMs =
          Configuration.getMs(PropertyKey.MASTER_LOST_WORKER_DELETION_TIMEOUT_MS);
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
      for (MasterWorkerInfo worker : mLostWorkers) {
        try (LockResource r = worker.lockWorkerMeta(
            EnumSet.of(WorkerMetaLockSection.BLOCKS), false)) {
          final long lastUpdate = mClock.millis() - worker.getLastUpdatedTimeMs();
          if ((lastUpdate - masterWorkerTimeoutMs) > masterWorkerDeleteTimeoutMs) {
            LOG.error("The lost worker {}({}) timed out after {}ms without a heartbeat! "
                + "Master will forget about this worker.", worker.getId(),
                worker.getWorkerAddress(), lastUpdate);
            deleteWorkerMetadata(worker);
          }
        }
      }
      for (MasterWorkerInfo worker : mDecommissionedWorkers) {
        try (LockResource r = worker.lockWorkerMeta(
            EnumSet.of(WorkerMetaLockSection.BLOCKS), false)) {
          final long lastUpdate = mClock.millis() - worker.getLastUpdatedTimeMs();
          if ((lastUpdate - masterWorkerTimeoutMs) > masterWorkerDeleteTimeoutMs) {
            LOG.error("The decommissioned worker {}({}) timed out after {}ms without a heartbeat! "
                + "Master will forget about this worker.", worker.getId(),
                worker.getWorkerAddress(), lastUpdate);
            deleteWorkerMetadata(worker);
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
    // Invalidate cache to trigger new build of worker info list
    mWorkerInfoCache.invalidate(WORKER_INFO_CACHE_KEY);
    // If a worker is gone before registering, avoid it getting stuck in mTempWorker forever
    mTempWorkers.remove(worker);
    WorkerNetAddress workerAddress = worker.getWorkerAddress();
    for (Consumer<Address> function : mWorkerLostListeners) {
      function.accept(new Address(workerAddress.getHost(), workerAddress.getRpcPort()));
    }
    // We only remove the blocks from master locations but do not
    // mark these blocks to-remove from the worker.
    // So if the worker comes back again the blocks are kept.
    processWorkerRemovedBlocks(worker, worker.getBlocks(), false);
    BlockLocationUtils.evictByWorkerId(worker.getId());
  }

  private void deleteWorkerMetadata(MasterWorkerInfo worker) {
    mWorkers.remove(worker);
    mLostWorkers.remove(worker);
    // If a worker is gone before registering, avoid it getting stuck in mTempWorker forever
    mTempWorkers.remove(worker);
    mDecommissionedWorkers.remove(worker);
    WorkerNetAddress workerAddress = worker.getWorkerAddress();
    for (Consumer<Address> function : mWorkerDeleteListeners) {
      function.accept(new Address(workerAddress.getHost(), workerAddress.getRpcPort()));
    }
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
  public void registerWorkerDeleteListener(Consumer<Address> function) {
    mWorkerDeleteListeners.add(function);
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
              master::getUniqueBlockCount);

      MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_TOTAL_BLOCK_REPLICA_COUNT.getName(),
              master::getBlockReplicaCount);
      for (int i = 0; i < master.getGlobalStorageTierAssoc().size(); i++) {
        String alias = master.getGlobalStorageTierAssoc().getAlias(i);
        // TODO(lu) Add template to dynamically construct metric key
        MetricsSystem.registerGaugeIfAbsent(
            MetricKey.CLUSTER_CAPACITY_TOTAL.getName() + MetricInfo.TIER + alias,
                () -> master.getTotalBytesOnTiers().getOrDefault(alias, 0L));

        MetricsSystem.registerGaugeIfAbsent(
            MetricKey.CLUSTER_CAPACITY_USED.getName() + MetricInfo.TIER + alias,
                () -> master.getUsedBytesOnTiers().getOrDefault(alias, 0L));
        MetricsSystem.registerGaugeIfAbsent(
            MetricKey.CLUSTER_CAPACITY_FREE.getName() + MetricInfo.TIER + alias,
                () -> master.getTotalBytesOnTiers().getOrDefault(alias, 0L)
                - master.getUsedBytesOnTiers().getOrDefault(alias, 0L));
      }

      MetricsSystem.registerGaugeIfAbsent(MetricKey.CLUSTER_WORKERS.getName(),
              master::getWorkerCount);
      MetricsSystem.registerGaugeIfAbsent(MetricKey.CLUSTER_LOST_WORKERS.getName(),
              master::getLostWorkerCount);

      MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_CACHED_BLOCK_LOCATIONS.getName(),
          BlockLocationUtils::getCachedBlockLocationSize);
    }

    private Metrics() {} // prevent instantiation
  }

  /**
   * @return the block meta store
   */
  @VisibleForTesting
  public BlockMetaStore getBlockMetaStore() {
    return mBlockMetaStore;
  }
}
