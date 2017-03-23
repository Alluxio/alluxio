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
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.metrics.MetricsSystem;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.BlockWorkerClientService;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AbstractWorker;
import alluxio.worker.SessionCleaner;
import alluxio.worker.SessionCleanupCallback;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.block.options.OpenUfsBlockOptions;
import alluxio.worker.file.FileSystemMasterClient;

import com.codahale.metrics.Gauge;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * The class is responsible for managing all top level components of the Block Worker.
 *
 * This includes:
 *
 * Servers: {@link BlockWorkerClientServiceHandler} (RPC Server)
 *
 * Periodic Threads: {@link BlockMasterSync} (Worker to Master continuous communication)
 *
 * Logic: {@link DefaultBlockWorker} (Logic for all block related storage operations)
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class DefaultBlockWorker extends AbstractWorker implements BlockWorker {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultBlockWorker.class);

  /** Runnable responsible for heartbeating and registration with master. */
  private BlockMasterSync mBlockMasterSync;

  /** Runnable responsible for fetching pinlist from master. */
  private PinListSync mPinListSync;

  /** Runnable responsible for clean up potential zombie sessions. */
  private SessionCleaner mSessionCleaner;

  /** Client for all block master communication. */
  private final BlockMasterClient mBlockMasterClient;

  /** Client for all file system master communication. */
  private final FileSystemMasterClient mFileSystemMasterClient;

  /** Block store delta reporter for master heartbeat. */
  private BlockHeartbeatReporter mHeartbeatReporter;
  /** Metrics reporter that listens on block events and increases metrics counters. */
  private BlockMetricsReporter mMetricsReporter;
  /** Session metadata, used to keep track of session heartbeats. */
  private Sessions mSessions;
  /** Block Store manager. */
  private BlockStore mBlockStore;
  private WorkerNetAddress mAddress;

  /** The under file system block store. */
  private final UnderFileSystemBlockStore mUnderFileSystemBlockStore;

  /**
   * The worker ID for this worker. This is initialized in {@link #init(WorkerNetAddress)} and may
   * be updated by the block sync thread if the master requests re-registration.
   */
  private AtomicReference<Long> mWorkerId;

  /**
   * Constructs a default block worker.
   *
   * @param workerId a reference for the id of this worker
   *
   * @throws IOException if an IO exception occurs
   */
  public DefaultBlockWorker(AtomicReference<Long> workerId) throws IOException {
    this(new BlockMasterClient(NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC)),
        new FileSystemMasterClient(NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC)),
        new Sessions(), new TieredBlockStore(), workerId);
  }

  /**
   * Constructs a default block worker.
   *
   * @param blockMasterClient a client for talking to the block master
   * @param fileSystemMasterClient a client for talking to the file system master
   * @param sessions an object for tracking and cleaning up client sessions
   * @param blockStore an Alluxio block store
   * @param workerId a reference for the id of this worker
   * @throws IOException if an IO exception occurs
   */
  public DefaultBlockWorker(BlockMasterClient blockMasterClient,
      FileSystemMasterClient fileSystemMasterClient, Sessions sessions, BlockStore blockStore,
      AtomicReference<Long> workerId) throws IOException {
    super(Executors.newFixedThreadPool(4,
        ThreadFactoryUtils.build("block-worker-heartbeat-%d", true)));
    mBlockMasterClient = blockMasterClient;
    mFileSystemMasterClient = fileSystemMasterClient;
    mHeartbeatReporter = new BlockHeartbeatReporter();
    mMetricsReporter = new BlockMetricsReporter();
    mSessions = sessions;
    mBlockStore = blockStore;
    mWorkerId = workerId;

    mBlockStore.registerBlockStoreEventListener(mHeartbeatReporter);
    mBlockStore.registerBlockStoreEventListener(mMetricsReporter);

    mUnderFileSystemBlockStore = new UnderFileSystemBlockStore(mBlockStore);
    Metrics.registerGauges(this);
  }

  @Override
  public BlockStore getBlockStore() {
    return mBlockStore;
  }

  @Override
  public BlockWorkerClientServiceHandler getWorkerServiceHandler() {
    return new BlockWorkerClientServiceHandler(this);
  }

  @Override
  public void init(WorkerNetAddress workerAddress) {
    mAddress = workerAddress;
    try {
      mWorkerId.set(mBlockMasterClient.getId(workerAddress));
    } catch (Exception e) {
      LOG.error("Failed to get a worker id from block master", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<>();
    services.put(Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME,
        new BlockWorkerClientService.Processor<>(getWorkerServiceHandler()));
    return services;
  }

  /**
   * Runs the block worker. The thread must be called after all services (e.g., web, dataserver)
   * started.
   *
   * @throws IOException if a non-Alluxio related exception occurs
   */
  @Override
  public void start() throws IOException {
    Preconditions.checkNotNull(mWorkerId, "mWorkerId");
    Preconditions.checkNotNull(mAddress, "mAddress");

    // Setup BlockMasterSync
    mBlockMasterSync = new BlockMasterSync(this, mWorkerId, mAddress, mBlockMasterClient);

    // Setup PinListSyncer
    mPinListSync = new PinListSync(this, mFileSystemMasterClient);

    // Setup session cleaner
    mSessionCleaner = new SessionCleaner(new SessionCleanupCallback() {
      /**
       * Cleans up after sessions, to prevent zombie sessions holding local resources.
       */
      @Override
      public void cleanupSessions() {
        for (long session : mSessions.getTimedOutSessions()) {
          mSessions.removeSession(session);
          mBlockStore.cleanupSession(session);
          mUnderFileSystemBlockStore.cleanupSession(session);
        }
      }
    });

    // Setup space reserver
    if (Configuration.getBoolean(PropertyKey.WORKER_TIERED_STORE_RESERVER_ENABLED)) {
      getExecutorService().submit(
          new HeartbeatThread(HeartbeatContext.WORKER_SPACE_RESERVER, new SpaceReserver(this),
              Configuration.getInt(PropertyKey.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS)));
    }

    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_BLOCK_SYNC, mBlockMasterSync,
            Configuration.getInt(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)));

    // Start the pinlist syncer to perform the periodical fetching
    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_PIN_LIST_SYNC, mPinListSync,
            Configuration.getInt(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)));

    // Start the session cleanup checker to perform the periodical checking
    getExecutorService().submit(mSessionCleaner);
  }

  /**
   * Stops the block worker. This method should only be called to terminate the worker.
   *
   * @throws IOException if the data server fails to close
   */
  @Override
  public void stop() throws IOException {
    mSessionCleaner.stop();
    mBlockMasterClient.close();
    mFileSystemMasterClient.close();
    // Use shutdownNow because HeartbeatThreads never finish until they are interrupted
    getExecutorService().shutdownNow();
  }

  @Override
  public void abortBlock(long sessionId, long blockId) throws BlockAlreadyExistsException,
      BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    mBlockStore.abortBlock(sessionId, blockId);
  }

  @Override
  public void accessBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    mBlockStore.accessBlock(sessionId, blockId);
  }

  @Override
  public void commitBlock(long sessionId, long blockId)
      throws BlockAlreadyExistsException, BlockDoesNotExistException, InvalidWorkerStateException,
      IOException, WorkerOutOfSpaceException {
    // NOTE: this may be invoked multiple times due to retry on client side.
    // TODO(binfan): find a better way to handle retry logic
    try {
      mBlockStore.commitBlock(sessionId, blockId);
    } catch (BlockAlreadyExistsException e) {
      LOG.debug("Block {} has been in block store, this could be a retry due to master-side RPC "
          + "failure, therefore ignore the exception", blockId, e);
    }

    // TODO(calvin): Reconsider how to do this without heavy locking.
    // Block successfully committed, update master with new block metadata
    Long lockId = mBlockStore.lockBlock(sessionId, blockId);
    try {
      BlockMeta meta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
      BlockStoreLocation loc = meta.getBlockLocation();
      Long length = meta.getBlockSize();
      BlockStoreMeta storeMeta = mBlockStore.getBlockStoreMeta();
      Long bytesUsedOnTier = storeMeta.getUsedBytesOnTiers().get(loc.tierAlias());
      mBlockMasterClient.commitBlock(mWorkerId.get(), bytesUsedOnTier, loc.tierAlias(), blockId,
          length);
    } catch (AlluxioTException | IOException | ConnectionFailedException e) {
      throw new IOException(ExceptionMessage.FAILED_COMMIT_BLOCK_TO_MASTER.getMessage(blockId), e);
    } finally {
      mBlockStore.unlockBlock(lockId);
    }
  }

  @Override
  public String createBlock(long sessionId, long blockId, String tierAlias, long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(tierAlias);
    TempBlockMeta createdBlock = mBlockStore.createBlock(sessionId, blockId, loc, initialBytes);
    return createdBlock.getPath();
  }

  @Override
  public void createBlockRemote(long sessionId, long blockId, String tierAlias, long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(tierAlias);
    mBlockStore.createBlock(sessionId, blockId, loc, initialBytes);
  }

  @Override
  public void freeSpace(long sessionId, long availableBytes, String tierAlias)
      throws WorkerOutOfSpaceException, BlockDoesNotExistException, IOException,
      BlockAlreadyExistsException, InvalidWorkerStateException {
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    mBlockStore.freeSpace(sessionId, availableBytes, location);
  }

  @Override
  public BlockWriter getTempBlockWriterRemote(long sessionId, long blockId)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      IOException {
    return mBlockStore.getBlockWriter(sessionId, blockId);
  }

  @Override
  public BlockHeartbeatReport getReport() {
    return mHeartbeatReporter.generateReport();
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
  public BlockMeta getVolatileBlockMeta(long blockId) throws BlockDoesNotExistException {
    return mBlockStore.getVolatileBlockMeta(blockId);
  }

  @Override
  public BlockMeta getBlockMeta(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    return mBlockStore.getBlockMeta(sessionId, blockId, lockId);
  }

  @Override
  public boolean hasBlockMeta(long blockId) {
    return mBlockStore.hasBlockMeta(blockId);
  }

  @Override
  public long lockBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    return mBlockStore.lockBlock(sessionId, blockId);
  }

  @Override
  public long lockBlockNoException(long sessionId, long blockId) {
    return mBlockStore.lockBlockNoException(sessionId, blockId);
  }

  @Override
  public void moveBlock(long sessionId, long blockId, String tierAlias)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    // TODO(calvin): Move this logic into BlockStore#moveBlockInternal if possible
    // Because the move operation is expensive, we first check if the operation is necessary
    BlockStoreLocation dst = BlockStoreLocation.anyDirInTier(tierAlias);
    long lockId = mBlockStore.lockBlock(sessionId, blockId);
    try {
      BlockMeta meta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
      if (meta.getBlockLocation().belongsTo(dst)) {
        return;
      }
    } finally {
      mBlockStore.unlockBlock(lockId);
    }
    // Execute the block move if necessary
    mBlockStore.moveBlock(sessionId, blockId, dst);
  }

  @Override
  public String readBlock(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    BlockMeta meta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
    return meta.getPath();
  }

  @Override
  public BlockReader readBlockRemote(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    return mBlockStore.getBlockReader(sessionId, blockId, lockId);
  }

  @Override
  public BlockReader readUfsBlock(long sessionId, long blockId, long offset, boolean noCache)
      throws BlockDoesNotExistException, IOException {
    return mUnderFileSystemBlockStore.getBlockReader(sessionId, blockId, offset, noCache);
  }

  @Override
  public void removeBlock(long sessionId, long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    mBlockStore.removeBlock(sessionId, blockId);
  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException {
    mBlockStore.requestSpace(sessionId, blockId, additionalBytes);
  }

  @Override
  public void unlockBlock(long lockId) throws BlockDoesNotExistException {
    mBlockStore.unlockBlock(lockId);
  }

  @Override
  // TODO(calvin): Remove when lock and reads are separate operations.
  public boolean unlockBlock(long sessionId, long blockId) {
    return mBlockStore.unlockBlock(sessionId, blockId);
  }

  @Override
  public void sessionHeartbeat(long sessionId) {
    mSessions.sessionHeartbeat(sessionId);
  }

  @Override
  public void updatePinList(Set<Long> pinnedInodes) {
    mBlockStore.updatePinnedInodes(pinnedInodes);
  }

  @Override
  public FileInfo getFileInfo(long fileId) throws IOException {
    try {
      return mFileSystemMasterClient.getFileInfo(fileId);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  @Override
  public boolean openUfsBlock(long sessionId, long blockId, OpenUfsBlockOptions options)
      throws BlockAlreadyExistsException {
    return mUnderFileSystemBlockStore.acquireAccess(sessionId, blockId, options);
  }

  @Override
  public void closeUfsBlock(long sessionId, long blockId)
      throws BlockAlreadyExistsException, IOException, WorkerOutOfSpaceException {
    mUnderFileSystemBlockStore.closeReaderOrWriter(sessionId, blockId);
    if (mBlockStore.getTempBlockMeta(sessionId, blockId) != null) {
      try {
        commitBlock(sessionId, blockId);
      } catch (BlockDoesNotExistException e) {
        // This can only happen if the session is expired. Ignore this exception if that happens.
        LOG.warn("Block {} does not exist while being committed.", blockId);
      } catch (InvalidWorkerStateException e) {
        // This can happen if there are multiple sessions writing to the same block.
        // BlockStore#getTempBlockMeta does not check whether the temp block belongs to
        // the sessionId.
        LOG.debug("Invalid worker state while committing block.", e);
      }
    }
    mUnderFileSystemBlockStore.releaseAccess(sessionId, blockId);
  }

  /**
   * This class contains some metrics related to the block worker.
   * This class is public because the metric names are referenced in
   * {@link alluxio.web.WebInterfaceWorkerMetricsServlet}.
   */
  @ThreadSafe
  public static final class Metrics {
    public static final String CAPACITY_TOTAL = "CapacityTotal";
    public static final String CAPACITY_USED = "CapacityUsed";
    public static final String CAPACITY_FREE = "CapacityFree";
    public static final String BLOCKS_CACHED = "BlocksCached";

    /**
     * Registers metric gauges.
     *
     * @param blockWorker the block worker handle
     */
    public static void registerGauges(final BlockWorker blockWorker) {
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getWorkerMetricName(CAPACITY_TOTAL),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return blockWorker.getStoreMeta().getCapacityBytes();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getWorkerMetricName(CAPACITY_USED),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return blockWorker.getStoreMeta().getUsedBytes();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getWorkerMetricName(CAPACITY_FREE),
          new Gauge<Long>() {
            @Override
            public Long getValue() {
              return blockWorker.getStoreMeta().getCapacityBytes() - blockWorker.getStoreMeta()
                  .getUsedBytes();
            }
          });

      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getWorkerMetricName(BLOCKS_CACHED),
          new Gauge<Integer>() {
            @Override
            public Integer getValue() {
              return blockWorker.getStoreMetaFull().getNumberOfBlocks();
            }
          });
    }

    private Metrics() {} // prevent instantiation
  }
}
