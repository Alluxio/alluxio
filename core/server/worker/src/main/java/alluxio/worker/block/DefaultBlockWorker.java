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

import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.RuntimeConstants;
import alluxio.Server;
import alluxio.Sessions;
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.AsyncCacheRequest;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.MasterClientContext;
import alluxio.metrics.MetricInfo;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.proto.dataserver.Protocol;
import alluxio.retry.RetryPolicy;
import alluxio.retry.RetryUtils;
import alluxio.retry.TimeoutRetry;
import alluxio.security.user.ServerUserState;
import alluxio.underfs.UfsManager;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.BlockReadRequest;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AbstractWorker;
import alluxio.worker.SessionCleaner;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.io.DelegatingBlockReader;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.file.FileSystemMasterClient;
import alluxio.worker.grpc.GrpcExecutors;

import com.codahale.metrics.Counter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;
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
  private static final long UFS_BLOCK_OPEN_TIMEOUT_MS =
      ServerConfiguration.getMs(PropertyKey.WORKER_UFS_BLOCK_OPEN_TIMEOUT_MS);

  /** Runnable responsible for heartbeating and registration with master. */
  private BlockMasterSync mBlockMasterSync;

  /** Runnable responsible for fetching pinlist from master. */
  private PinListSync mPinListSync;

  /** Runnable responsible for clean up potential zombie sessions. */
  private SessionCleaner mSessionCleaner;

  /** Used to close resources during stop. */
  private final Closer mResourceCloser;
  /**
   * Block master clients. commitBlock is the only reason to keep a pool of block master clients
   * on each worker. We should either improve our RPC model in the master or get rid of the
   * necessity to call commitBlock in the workers.
   */
  private final BlockMasterClientPool mBlockMasterClientPool;

  /** Client for all file system master communication. */
  private final FileSystemMasterClient mFileSystemMasterClient;

  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();

  /** Block store delta reporter for master heartbeat. */
  private BlockHeartbeatReporter mHeartbeatReporter;
  /** Metrics reporter that listens on block events and increases metrics counters. */
  private BlockMetricsReporter mMetricsReporter;
  /** Checker for storage paths. **/
  private StorageChecker mStorageChecker;
  /** Session metadata, used to keep track of session heartbeats. */
  private Sessions mSessions;
  /** Block Store manager. */
  private final BlockStore mLocalBlockStore;
  private WorkerNetAddress mAddress;

  /** The under file system block store. */
  private final UnderFileSystemBlockStore mUnderFileSystemBlockStore;

  /**
   * The worker ID for this worker. This is initialized in {@link #start(WorkerNetAddress)} and may
   * be updated by the block sync thread if the master requests re-registration.
   */
  private final AtomicReference<Long> mWorkerId;

  private final FileSystemContext mFsContext;
  private final CacheRequestManager mCacheManager;
  private final FuseManager mFuseManager;
  private final UfsManager mUfsManager;

  /**
   * Constructs a default block worker.
   *
   * @param ufsManager ufs manager
   */
  DefaultBlockWorker(UfsManager ufsManager) {
    this(new BlockMasterClientPool(),
        new FileSystemMasterClient(MasterClientContext
            .newBuilder(ClientContext.create(ServerConfiguration.global())).build()),
        new Sessions(), new TieredBlockStore(), ufsManager);
  }

  /**
   * Constructs a default block worker.
   *
   * @param blockMasterClientPool a client pool for talking to the block master
   * @param fileSystemMasterClient a client for talking to the file system master
   * @param sessions an object for tracking and cleaning up client sessions
   * @param blockStore an Alluxio block store
   * @param ufsManager ufs manager
   */
  @VisibleForTesting
  public DefaultBlockWorker(BlockMasterClientPool blockMasterClientPool,
      FileSystemMasterClient fileSystemMasterClient, Sessions sessions, BlockStore blockStore,
      UfsManager ufsManager) {
    super(ExecutorServiceFactories.fixedThreadPool("block-worker-executor", 5));
    mResourceCloser = Closer.create();
    mBlockMasterClientPool = mResourceCloser.register(blockMasterClientPool);
    mFileSystemMasterClient = mResourceCloser.register(fileSystemMasterClient);
    mHeartbeatReporter = new BlockHeartbeatReporter();
    mMetricsReporter = new BlockMetricsReporter();
    mSessions = sessions;
    mLocalBlockStore = mResourceCloser.register(blockStore);
    mWorkerId = new AtomicReference<>(-1L);
    mLocalBlockStore.registerBlockStoreEventListener(mHeartbeatReporter);
    mLocalBlockStore.registerBlockStoreEventListener(mMetricsReporter);
    mUfsManager = ufsManager;
    mFsContext = mResourceCloser.register(
        FileSystemContext.create(null, ServerConfiguration.global(), this));
    mCacheManager = new CacheRequestManager(
        GrpcExecutors.CACHE_MANAGER_EXECUTOR, this, mFsContext);
    mFuseManager = mResourceCloser.register(new FuseManager(mFsContext));
    mUnderFileSystemBlockStore = new UnderFileSystemBlockStore(mLocalBlockStore, ufsManager);

    Metrics.registerGauges(this);
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
    BlockMasterClient blockMasterClient = mBlockMasterClientPool.acquire();
    try {
      RetryUtils.retry("create worker id", () -> mWorkerId.set(blockMasterClient.getId(address)),
          RetryUtils.defaultWorkerMasterClientRetry(ServerConfiguration
              .getDuration(PropertyKey.WORKER_MASTER_CONNECT_RETRY_TIMEOUT)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create a worker id from block master: "
          + e.getMessage());
    } finally {
      mBlockMasterClientPool.release(blockMasterClient);
    }

    Preconditions.checkNotNull(mWorkerId, "mWorkerId");
    Preconditions.checkNotNull(mAddress, "mAddress");

    // Setup BlockMasterSync
    mBlockMasterSync = mResourceCloser
        .register(new BlockMasterSync(this, mWorkerId, mAddress, mBlockMasterClientPool));
    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_BLOCK_SYNC, mBlockMasterSync,
            (int) ServerConfiguration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS),
            ServerConfiguration.global(), ServerUserState.global()));

    // Setup PinListSyncer
    mPinListSync = mResourceCloser.register(new PinListSync(this, mFileSystemMasterClient));
    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_PIN_LIST_SYNC, mPinListSync,
            (int) ServerConfiguration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS),
            ServerConfiguration.global(), ServerUserState.global()));

    // Setup session cleaner
    mSessionCleaner = mResourceCloser
        .register(new SessionCleaner(mSessions, mLocalBlockStore, mUnderFileSystemBlockStore));
    getExecutorService().submit(mSessionCleaner);

    // Setup storage checker
    if (ServerConfiguration.getBoolean(PropertyKey.WORKER_STORAGE_CHECKER_ENABLED)) {
      mStorageChecker = mResourceCloser.register(new StorageChecker());
      getExecutorService()
          .submit(new HeartbeatThread(HeartbeatContext.WORKER_STORAGE_HEALTH, mStorageChecker,
              (int) ServerConfiguration.getMs(PropertyKey.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS),
                  ServerConfiguration.global(), ServerUserState.global()));
    }

    // Mounts the embedded Fuse application
    if (ServerConfiguration.getBoolean(PropertyKey.WORKER_FUSE_ENABLED)) {
      mFuseManager.start();
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
  public void abortBlock(long sessionId, long blockId) throws BlockAlreadyExistsException,
      BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    mLocalBlockStore.abortBlock(sessionId, blockId);
    Metrics.WORKER_ACTIVE_CLIENTS.dec();
  }

  @Override
  public void accessBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    mLocalBlockStore.accessBlock(sessionId, blockId);
  }

  @Override
  public void commitBlock(long sessionId, long blockId, boolean pinOnCreate)
      throws BlockAlreadyExistsException, BlockDoesNotExistException, InvalidWorkerStateException,
      IOException, WorkerOutOfSpaceException {
    long lockId = BlockWorker.INVALID_LOCK_ID;
    try {
      lockId = mLocalBlockStore.commitBlockLocked(sessionId, blockId, pinOnCreate);
    } catch (BlockAlreadyExistsException e) {
      LOG.debug("Block {} has been in block store, this could be a retry due to master-side RPC "
          + "failure, therefore ignore the exception", blockId, e);
    }

    // TODO(calvin): Reconsider how to do this without heavy locking.
    // Block successfully committed, update master with new block metadata
    if (lockId == BlockWorker.INVALID_LOCK_ID) {
      lockId = mLocalBlockStore.lockBlock(sessionId, blockId);
    }
    BlockMasterClient blockMasterClient = mBlockMasterClientPool.acquire();
    try {
      BlockMeta meta = mLocalBlockStore.getBlockMeta(sessionId, blockId, lockId);
      BlockStoreLocation loc = meta.getBlockLocation();
      String mediumType = loc.mediumType();
      Long length = meta.getBlockSize();
      BlockStoreMeta storeMeta = mLocalBlockStore.getBlockStoreMeta();
      Long bytesUsedOnTier = storeMeta.getUsedBytesOnTiers().get(loc.tierAlias());
      blockMasterClient.commitBlock(mWorkerId.get(), bytesUsedOnTier, loc.tierAlias(), mediumType,
          blockId, length);
    } catch (Exception e) {
      throw new IOException(ExceptionMessage.FAILED_COMMIT_BLOCK_TO_MASTER.getMessage(blockId), e);
    } finally {
      mBlockMasterClientPool.release(blockMasterClient);
      mLocalBlockStore.unlockBlock(lockId);
      Metrics.WORKER_ACTIVE_CLIENTS.dec();
    }
  }

  @Override
  public void commitBlockInUfs(long blockId, long length) throws IOException {
    BlockMasterClient blockMasterClient = mBlockMasterClientPool.acquire();
    try {
      blockMasterClient.commitBlockInUfs(blockId, length);
    } catch (Exception e) {
      throw new IOException(ExceptionMessage.FAILED_COMMIT_BLOCK_TO_MASTER.getMessage(blockId), e);
    } finally {
      mBlockMasterClientPool.release(blockMasterClient);
    }
  }

  @Override
  public String createBlock(long sessionId, long blockId, int tier,
      String medium, long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    BlockStoreLocation loc;
    if (medium.isEmpty()) {
      loc = BlockStoreLocation.anyDirInTier(mStorageTierAssoc.getAlias(tier));
    } else {
      loc = BlockStoreLocation.anyDirInAnyTierWithMedium(medium);
    }
    TempBlockMeta createdBlock;
    try {
      createdBlock = mLocalBlockStore.createBlock(sessionId, blockId,
          AllocateOptions.forCreate(initialBytes, loc));
    } catch (WorkerOutOfSpaceException e) {
      LOG.error(
          "Failed to create block. SessionId: {}, BlockId: {}, "
              + "TierAlias:{}, Medium:{}, InitialBytes:{}, Error:{}",
          sessionId, blockId, mStorageTierAssoc.getAlias(tier), medium, initialBytes, e);

      InetSocketAddress address =
          InetSocketAddress.createUnresolved(mAddress.getHost(), mAddress.getRpcPort());
      throw new WorkerOutOfSpaceException(ExceptionMessage.CANNOT_REQUEST_SPACE
          .getMessageWithUrl(RuntimeConstants.ALLUXIO_DEBUG_DOCS_URL, address, blockId), e);
    }
    Metrics.WORKER_ACTIVE_CLIENTS.inc();
    return createdBlock.getPath();
  }

  @Override
  public TempBlockMeta getTempBlockMeta(long sessionId, long blockId) {
    return mLocalBlockStore.getTempBlockMeta(sessionId, blockId);
  }

  @Override
  public BlockWriter createBlockWriter(long sessionId, long blockId)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      IOException {
    return mLocalBlockStore.getBlockWriter(sessionId, blockId);
  }

  @Override
  public BlockHeartbeatReport getReport() {
    return mHeartbeatReporter.generateReport();
  }

  @Override
  public BlockStoreMeta getStoreMeta() {
    return mLocalBlockStore.getBlockStoreMeta();
  }

  @Override
  public BlockStoreMeta getStoreMetaFull() {
    return mLocalBlockStore.getBlockStoreMetaFull();
  }

  @Override
  public BlockMeta getVolatileBlockMeta(long blockId) throws BlockDoesNotExistException {
    return mLocalBlockStore.getVolatileBlockMeta(blockId);
  }

  @Override
  public BlockMeta getBlockMeta(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    return mLocalBlockStore.getBlockMeta(sessionId, blockId, lockId);
  }

  @Override
  public boolean hasBlockMeta(long blockId) {
    return mLocalBlockStore.hasBlockMeta(blockId);
  }

  @Override
  public long lockBlock(long sessionId, long blockId) {
    long lockId = mLocalBlockStore.lockBlockNoException(sessionId, blockId);
    if (lockId != INVALID_LOCK_ID) {
      Metrics.WORKER_ACTIVE_CLIENTS.inc();
    }
    return lockId;
  }

  @Override
  public void moveBlock(long sessionId, long blockId, int tier)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    // TODO(calvin): Move this logic into BlockStore#moveBlockInternal if possible
    // Because the move operation is expensive, we first check if the operation is necessary
    BlockStoreLocation dst = BlockStoreLocation.anyDirInTier(mStorageTierAssoc.getAlias(tier));
    long lockId = mLocalBlockStore.lockBlock(sessionId, blockId);
    try {
      BlockMeta meta = mLocalBlockStore.getBlockMeta(sessionId, blockId, lockId);
      if (meta.getBlockLocation().belongsTo(dst)) {
        return;
      }
    } finally {
      mLocalBlockStore.unlockBlock(lockId);
    }
    // Execute the block move if necessary
    mLocalBlockStore.moveBlock(sessionId, blockId, AllocateOptions.forMove(dst));
  }

  @Override
  public void moveBlockToMedium(long sessionId, long blockId, String mediumType)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    BlockStoreLocation dst = BlockStoreLocation.anyDirInAnyTierWithMedium(mediumType);
    long lockId = mLocalBlockStore.lockBlock(sessionId, blockId);
    try {
      BlockMeta meta = mLocalBlockStore.getBlockMeta(sessionId, blockId, lockId);
      if (meta.getBlockLocation().belongsTo(dst)) {
        return;
      }
    } finally {
      mLocalBlockStore.unlockBlock(lockId);
    }
    // Execute the block move if necessary
    mLocalBlockStore.moveBlock(sessionId, blockId, AllocateOptions.forMove(dst));
  }

  /**
   * Creates the block reader to read the local cached block starting from given block offset.
   * Owner of this block reader must close it or lock will leak.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to read
   * @param offset the offset within this block
   * @return the block reader for the block or null if block not found
   */
  @Nullable
  private BlockReader createLocalBlockReader(long sessionId, long blockId, long offset)
      throws IOException {
    long lockId = mLocalBlockStore.lockBlockNoException(sessionId, blockId);
    if (lockId == BlockWorker.INVALID_LOCK_ID) {
      return null;
    }
    try {
      BlockReader reader = mLocalBlockStore.getBlockReader(sessionId, blockId, lockId);
      ((FileChannel) reader.getChannel()).position(offset);
      mLocalBlockStore.accessBlock(sessionId, blockId);
      return new DelegatingBlockReader(reader, () -> {
        try {
          mLocalBlockStore.unlockBlock(lockId);
        } catch (BlockDoesNotExistException e) {
          throw new IOException(e);
        }
      });
    } catch (Exception e) {
      try {
        mLocalBlockStore.unlockBlock(lockId);
      } catch (Exception ee) {
        LOG.warn("Failed to unlock block blockId={}, lockId={}", blockId, lockId, ee);
      }
      throw new IOException(String.format("Failed to get local block reader, sessionId=%d, "
              + "blockId=%d, offset=%d", sessionId, blockId, offset), e);
    }
  }

  @Override
  public BlockReader createUfsBlockReader(long sessionId, long blockId, long offset,
      boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws BlockDoesNotExistException, IOException {
    try {
      openUfsBlock(sessionId, blockId, options);
      BlockReader reader = mUnderFileSystemBlockStore.getBlockReader(sessionId, blockId, offset,
          positionShort, options.getUser());
      return new DelegatingBlockReader(reader, () -> {
        try {
          closeUfsBlock(sessionId, blockId);
        } catch (BlockAlreadyExistsException | IOException | WorkerOutOfSpaceException e) {
          throw new IOException(e);
        }
      });
    } catch (Exception e) {
      try {
        closeUfsBlock(sessionId, blockId);
      } catch (Exception ee) {
        LOG.warn("Failed to close UFS block", ee);
      }
      throw new IOException(String.format("Failed to read from UFS, sessionId=%d, "
              + "blockId=%d, offset=%d, positionShort=%s, options=%s: %s",
          sessionId, blockId, offset, positionShort, options, e.toString()), e);
    }
  }

  @Override
  public void removeBlock(long sessionId, long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    mLocalBlockStore.removeBlock(sessionId, blockId);
  }

  @Override
  public void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException {
    mLocalBlockStore.requestSpace(sessionId, blockId, additionalBytes);
  }

  @Override
  public void unlockBlock(long lockId) throws BlockDoesNotExistException {
    mLocalBlockStore.unlockBlock(lockId);
    Metrics.WORKER_ACTIVE_CLIENTS.dec();
  }

  @Override
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
    mCacheManager.submitRequest(request);
  }

  @Override
  public void updatePinList(Set<Long> pinnedInodes) {
    mLocalBlockStore.updatePinnedInodes(pinnedInodes);
  }

  @Override
  public FileInfo getFileInfo(long fileId) throws IOException {
    return mFileSystemMasterClient.getFileInfo(fileId);
  }

  /**
   * Opens a UFS block. It registers the block metadata information to the UFS block store. It
   * returns false if the number of concurrent readers on this block exceeds a threshold.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   * @param options the options
   * @return whether the UFS block is successfully opened
   * @throws BlockAlreadyExistsException if the UFS block already exists in the
   *         UFS block store
   */
  @VisibleForTesting
  public boolean openUfsBlock(long sessionId, long blockId, Protocol.OpenUfsBlockOptions options)
      throws BlockAlreadyExistsException {
    if (!options.hasUfsPath() && options.hasBlockInUfsTier() && options.getBlockInUfsTier()) {
      // This is a fallback UFS block read. Reset the UFS block path according to the UfsBlock flag.
      UfsManager.UfsClient ufsClient;
      try {
        ufsClient = mUfsManager.get(options.getMountId());
      } catch (alluxio.exception.status.NotFoundException
          | alluxio.exception.status.UnavailableException e) {
        LOG.warn("Can not open UFS block: mount id {} not found {}",
            options.getMountId(), e.toString());
        return false;
      }
      options = options.toBuilder().setUfsPath(
          alluxio.worker.BlockUtils.getUfsBlockPath(ufsClient, blockId)).build();
    }
    return mUnderFileSystemBlockStore.acquireAccess(sessionId, blockId, options);
  }

  /**
   * Closes a UFS block for a client session. It also commits the block to Alluxio block store
   * if the UFS block has been cached successfully.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   * @throws BlockAlreadyExistsException if it fails to commit the block to Alluxio block store
   *         because the block exists in the Alluxio block store
   * @throws BlockDoesNotExistException if the UFS block does not exist in the
   *         UFS block store
   * @throws WorkerOutOfSpaceException the the worker does not have enough space to commit the block
   */
  @VisibleForTesting
  public void closeUfsBlock(long sessionId, long blockId)
      throws BlockAlreadyExistsException, IOException, WorkerOutOfSpaceException {
    try {
      mUnderFileSystemBlockStore.closeReaderOrWriter(sessionId, blockId);
      if (mLocalBlockStore.getTempBlockMeta(sessionId, blockId) != null) {
        try {
          commitBlock(sessionId, blockId, false);
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
    } finally {
      mUnderFileSystemBlockStore.releaseAccess(sessionId, blockId);
    }
  }

  @Override
  public BlockReader createBlockReader(BlockReadRequest request) throws
      BlockDoesNotExistException, IOException {
    long sessionId = request.getSessionId();
    long blockId = request.getId();
    RetryPolicy retryPolicy = new TimeoutRetry(UFS_BLOCK_OPEN_TIMEOUT_MS, Constants.SECOND_MS);
    while (retryPolicy.attempt()) {
      BlockReader reader = createLocalBlockReader(sessionId, blockId, request.getStart());
      if (reader != null) {
        Metrics.WORKER_ACTIVE_CLIENTS.inc();
        return reader;
      }
      boolean checkUfs =
          request.isPersisted() || (request.getOpenUfsBlockOptions() != null && request
              .getOpenUfsBlockOptions().hasBlockInUfsTier() && request.getOpenUfsBlockOptions()
              .getBlockInUfsTier());
      if (!checkUfs) {
        throw new BlockDoesNotExistException(ExceptionMessage.NO_BLOCK_ID_FOUND, blockId);
      }
      // When the block does not exist in Alluxio but exists in UFS, try to open the UFS block.
      try {
        Metrics.WORKER_ACTIVE_CLIENTS.inc();
        return createUfsBlockReader(request.getSessionId(), request.getId(), request.getStart(),
            request.isPositionShort(), request.getOpenUfsBlockOptions());
      } catch (Exception e) {
        throw new UnavailableException(
            String.format("Failed to read block ID=%s from tiered storage and UFS tier: %s",
                request.getId(), e.toString()));
      }
    }
    throw new UnavailableException(ExceptionMessage.UFS_BLOCK_ACCESS_TOKEN_UNAVAILABLE
        .getMessage(request.getId(), request.getOpenUfsBlockOptions().getUfsPath()));
  }

  @Override
  public void clearMetrics() {
    // TODO(lu) Create a metrics worker and move this method to metrics worker
    MetricsSystem.resetAllMetrics();
  }

  @Override
  public void cleanupSession(long sessionId) {
    mLocalBlockStore.cleanupSession(sessionId);
    mUnderFileSystemBlockStore.cleanupSession(sessionId);
    Metrics.WORKER_ACTIVE_CLIENTS.dec();
  }

  /**
   * This class contains some metrics related to the block worker.
   * This class is public because the metric names are referenced in
   * {@link alluxio.web.WebInterfaceAbstractMetricsServlet}.
   */
  @ThreadSafe
  public static final class Metrics {
    private static final Counter WORKER_ACTIVE_CLIENTS =
        MetricsSystem.counter(MetricKey.WORKER_ACTIVE_CLIENTS.getName());

    /**
     * Registers metric gauges.
     *
     * @param blockWorker the block worker handle
     */
    public static void registerGauges(final BlockWorker blockWorker) {
      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.WORKER_CAPACITY_TOTAL.getName()),
          () -> blockWorker.getStoreMeta().getCapacityBytes());

      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.WORKER_CAPACITY_USED.getName()),
          () -> blockWorker.getStoreMeta().getUsedBytes());

      MetricsSystem.registerGaugeIfAbsent(
          MetricsSystem.getMetricName(MetricKey.WORKER_CAPACITY_FREE.getName()),
          () -> blockWorker.getStoreMeta().getCapacityBytes() - blockWorker.getStoreMeta()
                      .getUsedBytes());

      StorageTierAssoc assoc = blockWorker.getStoreMeta().getStorageTierAssoc();
      for (int i = 0; i < assoc.size(); i++) {
        String tier = assoc.getAlias(i);
        // TODO(lu) Add template to dynamically generate MetricKey
        MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
            MetricKey.WORKER_CAPACITY_TOTAL.getName() + MetricInfo.TIER + tier),
            () -> blockWorker.getStoreMeta().getCapacityBytesOnTiers().getOrDefault(tier, 0L));

        MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
            MetricKey.WORKER_CAPACITY_USED.getName() + MetricInfo.TIER + tier),
            () -> blockWorker.getStoreMeta().getUsedBytesOnTiers().getOrDefault(tier, 0L));

        MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
            MetricKey.WORKER_CAPACITY_FREE.getName() + MetricInfo.TIER + tier),
            () -> blockWorker.getStoreMeta().getCapacityBytesOnTiers().getOrDefault(tier, 0L)
                - blockWorker.getStoreMeta().getUsedBytesOnTiers().getOrDefault(tier, 0L));
      }
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName(
          MetricKey.WORKER_BLOCKS_CACHED.getName()),
          () -> blockWorker.getStoreMetaFull().getNumberOfBlocks());
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
    public void heartbeat() {
      try {
        mLocalBlockStore.checkStorage();
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
}
