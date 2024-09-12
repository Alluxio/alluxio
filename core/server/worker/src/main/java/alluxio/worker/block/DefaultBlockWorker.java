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

import static alluxio.worker.block.BlockMetadataManager.WORKER_STORAGE_TIER_ASSOC;

import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.Server;
import alluxio.Sessions;
import alluxio.annotation.SuppressFBWarnings;
import alluxio.client.file.FileSystemContext;
import alluxio.collections.PrefixList;
import alluxio.conf.Configuration;
import alluxio.conf.ConfigurationValueOptions;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.BlockDoesNotExistRuntimeException;
import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.grpc.AsyncCacheRequest;
import alluxio.grpc.Block;
import alluxio.grpc.BlockChecksum;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.grpc.UfsReadOptions;
import alluxio.heartbeat.FixedIntervalSupplier;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.NioHeapBufferPool;
import alluxio.proto.dataserver.Protocol;
import alluxio.retry.RetryUtils;
import alluxio.security.user.ServerUserState;
import alluxio.util.CRC64;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.FileUtils;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AbstractWorker;
import alluxio.worker.SessionCleaner;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.file.FileSystemMasterClient;
import alluxio.worker.grpc.GrpcExecutors;
import alluxio.worker.page.PagedBlockStore;

import com.codahale.metrics.CachedGauge;
import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The class is responsible for managing all top level components of the Block Worker.
 *
 * This includes:
 *
 * Periodic Threads: {@link BlockMasterSync} (Worker to Master continuous communication)
 *
 * Logic: {@link DefaultBlockWorker} (Logic for all block related storage operations)
 */
@NotThreadSafe
public class DefaultBlockWorker extends AbstractWorker implements BlockWorker {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultBlockWorker.class);
  public static final int CACHEGAUGE_UPDATE_INTERVAL = 5000;

  /** Used to close resources during stop. */
  protected final Closer mResourceCloser = Closer.create();
  /**
   * Block master clients. commitBlock is the only reason to keep a pool of block master clients
   * on each worker. We should either improve our RPC model in the master or get rid of the
   * necessity to call commitBlock in the workers.
   */
  private final BlockMasterClientPool mBlockMasterClientPool;

  /** Client for all file system master communication. */
  protected final FileSystemMasterClient mFileSystemMasterClient;

  /** Block store delta reporter for master heartbeat. */
  private final BlockHeartbeatReporter mHeartbeatReporter;
  /** Session metadata, used to keep track of session heartbeats. */
  private final Sessions mSessions;
  /** Block Store manager. */
  protected final BlockStore mBlockStore;
  /** List of paths to always keep in memory. */
  private final PrefixList mWhitelist;

  /**
   * The worker ID for this worker. This is initialized in {@link #start(WorkerNetAddress)} and may
   * be updated by the block sync thread if the master requests re-registration.
   */
  protected final AtomicReference<Long> mWorkerId;

  private final CacheRequestManager mCacheManager;
  private final FuseManager mFuseManager;

  protected WorkerNetAddress mAddress;
  private final ExecutorService mChecksumCalculationThreadPool;
  private final Optional<RateLimiter> mChecksumCalculationRateLimiter;
  private final boolean mChecksumCalculationUsingBufferPool;

  /**
   * Constructs a default block worker.
   *
   * @param blockMasterClientPool a client pool for talking to the block master
   * @param fileSystemMasterClient a client for talking to the file system master
   * @param sessions an object for tracking and cleaning up client sessions
   * @param blockStore an Alluxio block store
   * @param workerId worker id
   */
  @VisibleForTesting
  public DefaultBlockWorker(BlockMasterClientPool blockMasterClientPool,
      FileSystemMasterClient fileSystemMasterClient, Sessions sessions, BlockStore blockStore,
      AtomicReference<Long> workerId) {
    super(
        Configuration.getBoolean(PropertyKey.WORKER_REGISTER_TO_ALL_MASTERS)
            ? ExecutorServiceFactories.cachedThreadPool("block-worker-executor")
            : ExecutorServiceFactories.fixedThreadPool("block-worker-executor", 5)
    );
    mBlockMasterClientPool = mResourceCloser.register(blockMasterClientPool);
    mFileSystemMasterClient = mResourceCloser.register(fileSystemMasterClient);
    mHeartbeatReporter = new BlockHeartbeatReporter();
    /* Metrics reporter that listens on block events and increases metrics counters. */
    BlockMetricsReporter metricsReporter = new BlockMetricsReporter();
    mSessions = sessions;
    mBlockStore = mResourceCloser.register(blockStore);
    mWorkerId = workerId;
    mBlockStore.registerBlockStoreEventListener(mHeartbeatReporter);
    mBlockStore.registerBlockStoreEventListener(metricsReporter);
    FileSystemContext fsContext = mResourceCloser.register(
        FileSystemContext.create(ClientContext.create(Configuration.global()), this));
    mCacheManager = new CacheRequestManager(
        GrpcExecutors.CACHE_MANAGER_EXECUTOR, this, fsContext);
    mFuseManager = mResourceCloser.register(new FuseManager(fsContext));
    mWhitelist = new PrefixList(Configuration.getList(PropertyKey.WORKER_WHITELIST));
    mChecksumCalculationThreadPool = ExecutorServiceFactories.fixedThreadPool(
        "checksum-calculation-pool",
        Configuration.getInt(PropertyKey.WORKER_BLOCK_CHECKSUM_CALCULATION_THREAD_POOL_SIZE))
        .create();
    long checksumThroughputThreshold =
        Configuration.getBytes(PropertyKey.WORKER_BLOCK_CHECKSUM_CALCULATION_THROUGHPUT_THRESHOLD);
    if (checksumThroughputThreshold <= 0) {
      mChecksumCalculationRateLimiter = Optional.empty();
    } else {
      // The min precision is 1kb to avoid data overflow
      mChecksumCalculationRateLimiter =
          Optional.of(RateLimiter.create(
              Math.max(Math.toIntExact(checksumThroughputThreshold / 1024), 1)));
    }
    mChecksumCalculationUsingBufferPool =
        Configuration.getBoolean(PropertyKey.WORKER_BLOCK_CHECKSUM_CALCULATION_USE_BUFFER_POOL);
    Metrics.registerGauges(this);
  }

  /**
   * get the LocalBlockStore that manages local blocks.
   *
   * @return the LocalBlockStore that manages local blocks
   * */
  @Override
  public BlockStore getBlockStore() {
    return mBlockStore;
  }

  @Override
  public WorkerNetAddress getWorkerAddress() {
    return mAddress;
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return new HashSet<>();
  }

  @Override
  public String getName() {
    return Constants.BLOCK_WORKER_NAME;
  }

  @Override
  public Map<ServiceType, GrpcService> getServices() {
    return Collections.emptyMap();
  }

  @Override
  public AtomicReference<Long> getWorkerId() {
    return mWorkerId;
  }

  /**
   * Runs the block worker. The thread must be called after all services (e.g., web, dataserver)
   * started.
   *
   * BlockWorker doesn't support being restarted!
   */
  @Override
  public void start(WorkerNetAddress address) throws IOException {
    super.start(address);
    mAddress = address;

    // Acquire worker Id.
    askForWorkerId(address);

    Preconditions.checkNotNull(mWorkerId, "mWorkerId");
    Preconditions.checkNotNull(mAddress, "mAddress");

    // Setup BlockMasterSync
    setupBlockMasterSync();

    // Setup PinListSyncer
    PinListSync pinListSync = mResourceCloser.register(
        new PinListSync(this, mFileSystemMasterClient));
    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_PIN_LIST_SYNC, pinListSync,
            () -> new FixedIntervalSupplier(
                Configuration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)),
            Configuration.global(), ServerUserState.global()));

    // Setup session cleaner
    SessionCleaner sessionCleaner = mResourceCloser
        .register(new SessionCleaner(mSessions, mBlockStore));
    getExecutorService().submit(sessionCleaner);

    // Setup storage checker
    if (Configuration.getBoolean(PropertyKey.WORKER_STORAGE_CHECKER_ENABLED)) {
      StorageChecker storageChecker = mResourceCloser.register(new StorageChecker());
      getExecutorService()
          .submit(new HeartbeatThread(HeartbeatContext.WORKER_STORAGE_HEALTH, storageChecker,
              () -> new FixedIntervalSupplier(
                  Configuration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)),
                  Configuration.global(), ServerUserState.global()));
    }

    // Mounts the embedded Fuse application
    if (Configuration.getBoolean(PropertyKey.WORKER_FUSE_ENABLED)) {
      mFuseManager.start();
    }
  }

  protected void setupBlockMasterSync() throws IOException {
    BlockMasterSync blockMasterSync = mResourceCloser
        .register(new BlockMasterSync(this, mWorkerId, mAddress, mBlockMasterClientPool));
    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_BLOCK_SYNC, blockMasterSync,
            () -> new FixedIntervalSupplier(
                Configuration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)),
            Configuration.global(), ServerUserState.global()));
  }

  /**
   * Ask the master for a workerId. Should not be called outside of testing
   *
   * @param address the address this worker operates on
   */
  @VisibleForTesting
  public void askForWorkerId(WorkerNetAddress address) {
    BlockMasterClient blockMasterClient = mBlockMasterClientPool.acquire();
    try {
      RetryUtils.retry("create worker id", () -> mWorkerId.set(blockMasterClient.getId(address)),
              RetryUtils.defaultWorkerMasterClientRetry());
    } catch (Exception e) {
      throw new RuntimeException("Failed to create a worker id from block master: "
              + e.getMessage());
    } finally {
      mBlockMasterClientPool.release(blockMasterClient);
    }
  }

  /**
   * Stops the block worker. This method should only be called to terminate the worker.
   *
   * BlockWorker doesn't support being restarted!
   */
  @Override
  public void stop() throws IOException {
    // Stop the base. (closes executors.)
    // This is intentionally called first in order to send interrupt signals to heartbeat threads.
    // Otherwise, if the heartbeat threads are not interrupted then the shutdown can hang.
    super.stop();
    // Stop heart-beat executors and clients.
    mResourceCloser.close();
  }

  @Override
  public void abortBlock(long sessionId, long blockId) throws IOException {
    mBlockStore.abortBlock(sessionId, blockId);
    Metrics.WORKER_ACTIVE_CLIENTS.dec();
  }

  @Override
  public void commitBlock(long sessionId, long blockId, boolean pinOnCreate) {
    mBlockStore.commitBlock(sessionId, blockId, pinOnCreate);
  }

  @Override
  public void commitBlockInUfs(long blockId, long length) {
    BlockMasterClient blockMasterClient = mBlockMasterClientPool.acquire();
    try {
      blockMasterClient.commitBlockInUfs(blockId, length);
    } catch (AlluxioStatusException e) {
      throw AlluxioRuntimeException.from(e);
    } finally {
      mBlockMasterClientPool.release(blockMasterClient);
    }
  }

  @Override
  public String createBlock(long sessionId, long blockId, int tier,
      CreateBlockOptions createBlockOptions) {
    try {
      return mBlockStore.createBlock(sessionId, blockId, tier, createBlockOptions);
    } catch (ResourceExhaustedRuntimeException e) {
      // mAddress is null if the worker is not started
      if (mAddress == null) {
        throw new ResourceExhaustedRuntimeException(
            ExceptionMessage.CANNOT_REQUEST_SPACE.getMessage(mWorkerId.get(), blockId), e, false);
      }
      InetSocketAddress address =
          InetSocketAddress.createUnresolved(mAddress.getHost(), mAddress.getRpcPort());
      throw new ResourceExhaustedRuntimeException(
          ExceptionMessage.CANNOT_REQUEST_SPACE.getMessageWithUrl(
              RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL, address, blockId), e, false);
    }
  }

  @Override
  public BlockWriter createBlockWriter(long sessionId, long blockId)
      throws IOException {
    return mBlockStore.createBlockWriter(sessionId, blockId);
  }

  @Override
  public BlockHeartbeatReport getReport() {
    return mHeartbeatReporter.generateReportAndClear();
  }

  @Override
  public BlockStoreMeta getStoreMeta() {
    return mBlockStore.getBlockStoreMeta();
  }

  @Override
  public BlockStoreMeta getStoreMetaFull() {
    return mBlockStore.getBlockStoreMetaFull();
  }

  @Override
  public List<String> getWhiteList() {
    return mWhitelist.getList();
  }

  @Override
  public BlockReader createUfsBlockReader(long sessionId, long blockId, long offset,
      boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws IOException {
    return mBlockStore.createUfsBlockReader(sessionId, blockId, offset, positionShort, options);
  }

  @Override
  public void removeBlock(long sessionId, long blockId)
      throws IOException {
    mBlockStore.removeBlock(sessionId, blockId);
  }

  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
  @Override
  public void freeWorker() throws IOException {
    List<String> paths = new ArrayList<>();
    if (Configuration.global().get(PropertyKey.WORKER_BLOCK_STORE_TYPE) == BlockStoreType.FILE) {
      int tierCount = Configuration.global().getInt(PropertyKey.WORKER_TIERED_STORE_LEVELS);
      for (int i = 0; i < tierCount; i++) {
        paths.addAll(Configuration.global().getList(PropertyKey
                .Template.WORKER_TIERED_STORE_LEVEL_DIRS_PATH.format(i)));
      }
    } else if (Configuration.global()
        .get(PropertyKey.WORKER_BLOCK_STORE_TYPE) == BlockStoreType.PAGE) {
      paths.addAll(Configuration.global().getList(PropertyKey.WORKER_PAGE_STORE_DIRS));
    } else {
      throw new IllegalStateException("Unknown WORKER_BLOCK_STORE_TYPE.");
    }

    List<String> failDeleteDirs = new ArrayList<>();
    for (String tmpPath : paths) {
      File[] files = new File(tmpPath).listFiles();
      Preconditions.checkNotNull(files, "The path does not denote a directory.");
      for (File file : files) {
        try {
          FileUtils.deletePathRecursively(file.getPath());
        } catch (IOException ie) {
          failDeleteDirs.add(file.getPath());
        }
      }
    }
    if (!failDeleteDirs.isEmpty()) {
      LOG.info("Some directories fail to be deleted: " + failDeleteDirs);
      throw new IOException(failDeleteDirs.toString());
    }
    LOG.info("All blocks and directories in worker {} are freed.", getWorkerId());
  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes) {
    mBlockStore.requestSpace(sessionId, blockId, additionalBytes);
  }

  @Override
  @Deprecated
  public void asyncCache(AsyncCacheRequest request) {
    CacheRequest cacheRequest =
        CacheRequest.newBuilder().setBlockId(request.getBlockId()).setLength(request.getLength())
            .setOpenUfsBlockOptions(request.getOpenUfsBlockOptions())
            .setSourceHost(request.getSourceHost()).setSourcePort(request.getSourcePort())
            .setAsync(true).build();
    try {
      mCacheManager.submitRequest(cacheRequest);
    } catch (Exception e) {
      LOG.warn("Failed to submit async cache request. request: {}", request, e);
    }
  }

  @Override
  public void cache(CacheRequest request) throws AlluxioException, IOException {
    // todo(bowen): paged block store handles caching from UFS automatically and on-the-fly
    //  this will cause an unnecessary extra read of the block
    if (mBlockStore instanceof PagedBlockStore) {
      return;
    }
    mCacheManager.submitRequest(request);
  }

  @Override
  public CompletableFuture<List<BlockStatus>> load(List<Block> blocks, UfsReadOptions options) {
    return mBlockStore.load(blocks, options);
  }

  @Override
  public void updatePinList(Set<Long> pinnedInodes) {
    mBlockStore.updatePinnedInodes(pinnedInodes);
  }

  @Override
  public FileInfo getFileInfo(long fileId) throws IOException {
    return mFileSystemMasterClient.getFileInfo(fileId);
  }

  /**
   * Closes a UFS block for a client session. It also commits the block to Alluxio block store
   * if the UFS block has been cached successfully.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   */

  @Override
  public BlockReader createBlockReader(long sessionId, long blockId, long offset,
      boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws IOException {
    BlockReader reader =
        mBlockStore.createBlockReader(sessionId, blockId, offset, positionShort, options);
    Metrics.WORKER_ACTIVE_CLIENTS.inc();
    return reader;
  }

  @Override
  public void clearMetrics() {
    // TODO(lu) Create a metrics worker and move this method to metrics worker
    MetricsSystem.resetAllMetrics();
  }

  @Override
  public alluxio.wire.Configuration getConfiguration(GetConfigurationPOptions options) {
    // NOTE(cc): there is no guarantee that the returned cluster and path configurations are
    // consistent snapshot of the system's state at a certain time, the path configuration might
    // be in a newer state. But it's guaranteed that the hashes are respectively correspondent to
    // the properties.
    alluxio.wire.Configuration.Builder builder = alluxio.wire.Configuration.newBuilder();

    if (!options.getIgnoreClusterConf()) {
      for (PropertyKey key : Configuration.keySet()) {
        if (key.isBuiltIn()) {
          Source source = Configuration.getSource(key);
          Object value = Configuration.getOrDefault(key, null,
                  ConfigurationValueOptions.defaults().useDisplayValue(true)
                          .useRawValue(options.getRawValue()));
          builder.addClusterProperty(key.getName(), value, source);
        }
      }
      // NOTE(cc): assumes that Configuration is read-only when master is running, otherwise,
      // the following hash might not correspond to the above cluster configuration.
      builder.setClusterConfHash(Configuration.hash());
      builder.setClusterConfLastUpdateTime(Configuration.getLastUpdateTime());
    }
    return builder.build();
  }

  @Override
  public void cleanupSession(long sessionId) {
    mBlockStore.cleanupSession(sessionId);
    Metrics.WORKER_ACTIVE_CLIENTS.dec();
  }

  /**
   * This class contains some metrics related to the block worker.
   * This class is public because the metric names are referenced in
   * {@link alluxio.web.WebInterfaceAbstractMetricsServlet}.
   */
  @ThreadSafe
  public static final class Metrics {
    public static final Counter WORKER_ACTIVE_CLIENTS =
        MetricsSystem.counter(MetricKey.WORKER_ACTIVE_CLIENTS.getName());
    public static final Counter WORKER_ACTIVE_OPERATIONS =
        MetricsSystem.counter(MetricKey.WORKER_ACTIVE_OPERATIONS.getName());

    /**
     * Registers metric gauges.
     *
     * @param blockWorker the BlockWorker
     */
    public static void registerGauges(final BlockWorker blockWorker) {
      CachedGauge<BlockWorkerMetrics> cache =
          new CachedGauge<BlockWorkerMetrics>(CACHEGAUGE_UPDATE_INTERVAL, TimeUnit.MILLISECONDS) {
            @Override
            protected BlockWorkerMetrics loadValue() {
              BlockStoreMeta meta = blockWorker.getStoreMetaFull();
              BlockWorkerMetrics metrics = BlockWorkerMetrics.from(meta, WORKER_STORAGE_TIER_ASSOC);
              return metrics;
            }
          };
      MetricsSystem.registerCachedGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.WORKER_CAPACITY_TOTAL.getName()),
          () -> cache.getValue().getCapacityBytes());

      MetricsSystem.registerCachedGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.WORKER_CAPACITY_USED.getName()),
          () -> cache.getValue().getUsedBytes());

      MetricsSystem.registerCachedGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.WORKER_CAPACITY_FREE.getName()),
          () -> cache.getValue().getCapacityFree());

      for (int i = 0; i < WORKER_STORAGE_TIER_ASSOC.size(); i++) {
        String tier = WORKER_STORAGE_TIER_ASSOC.getAlias(i);
        // TODO(lu) Add template to dynamically generate MetricKey
        MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
            MetricKey.WORKER_CAPACITY_TOTAL.getName() + MetricInfo.TIER + tier),
            () -> cache.getValue().getCapacityBytesOnTiers().getOrDefault(tier, 0L));

        MetricsSystem.registerCachedGaugeIfAbsent(MetricsSystem.getMetricName(
            MetricKey.WORKER_CAPACITY_USED.getName() + MetricInfo.TIER + tier),
            () -> cache.getValue().getUsedBytesOnTiers().getOrDefault(tier, 0L));

        MetricsSystem.registerCachedGaugeIfAbsent(MetricsSystem.getMetricName(
            MetricKey.WORKER_CAPACITY_FREE.getName() + MetricInfo.TIER + tier),
            () -> cache.getValue().getFreeBytesOnTiers().getOrDefault(tier, 0L));
      }
      MetricsSystem.registerCachedGaugeIfAbsent(MetricsSystem.getMetricName(
          MetricKey.WORKER_BLOCKS_CACHED.getName()),
          () -> cache.getValue().getNumberOfBlocks());
    }

    private Metrics() {} // prevent instantiation
  }

  /**
   * StorageChecker periodically checks the health of each storage path and report missing blocks to
   * {@link BlockWorker}.
   */
  @NotThreadSafe
  public final class StorageChecker implements HeartbeatExecutor {

    @Override
    public void heartbeat(long timeLimitMs) {
      try {
        mBlockStore.removeInaccessibleStorage();
      } catch (Exception e) {
        LOG.warn("Failed to check storage: {}", e.toString());
        LOG.debug("Exception: ", e);
      }
    }

    @Override
    public void close() {
      // Nothing to clean up
    }
  }

  @Override
  public Map<Long, BlockChecksum> calculateBlockChecksum(List<Long> blockIds) {
    int chunkSize = 1024 * 1024 * 8; //8MB
    HashMap<Long, BlockChecksum> result = new HashMap<>();
    List<Future<?>> futures = new ArrayList<>();
    for (long blockId : blockIds) {
      Future<?> future = mChecksumCalculationThreadPool.submit(() -> {
        ByteBuffer bf = null;
        try (BlockReader br = mBlockStore.createBlockReader(
            Sessions.WORKER_CHECKSUM_CHECK_SESSION_ID,
            blockId, 0, false, Protocol.OpenUfsBlockOptions.getDefaultInstance())) {
          CRC64 crc64 = new CRC64();
          if (mChecksumCalculationUsingBufferPool) {
            bf = NioHeapBufferPool.acquire(chunkSize);
          } else {
            bf = ByteBuffer.allocate(chunkSize);
          }
          ByteBuf bb = Unpooled.wrappedBuffer(bf);
          while (true) {
            bb.clear();
            long bytesRead = br.transferTo(bb);
            if (bytesRead < 0) {
              break;
            }
            crc64.update(bf.array(), Math.toIntExact(bytesRead));
            int permits = Math.toIntExact(Math.max(1, bytesRead / 1024));
            if (mChecksumCalculationRateLimiter.isPresent()) {
              mChecksumCalculationRateLimiter.get().acquire(permits);
            }
          }
          result.put(blockId,
              BlockChecksum.newBuilder()
                  .setBlockId(blockId).setBlockLength(br.getLength())
                  .setChecksum(String.valueOf(crc64.getValue())).build());
        } catch (BlockDoesNotExistRuntimeException e) {
          LOG.warn("Block {} not found during CRC calculation", blockId);
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          if (bf != null && mChecksumCalculationUsingBufferPool) {
            NioHeapBufferPool.release(bf);
          }
        }
      });
      futures.add(future);
    }
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }
}
