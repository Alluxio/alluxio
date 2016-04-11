/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
import alluxio.Sessions;
import alluxio.exception.AlluxioException;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ConnectionFailedException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.thrift.BlockWorkerClientService;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.FileUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.AbstractWorker;
import alluxio.worker.WorkerContext;
import alluxio.worker.WorkerIdRegistry;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;
import alluxio.worker.file.FileSystemMasterClient;

import com.google.common.base.Throwables;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The class is responsible for managing all top level components of the Block Worker.
 *
 * This includes:
 *
 * Servers: {@link BlockWorkerClientServiceHandler} (RPC Server)
 *
 * Periodic Threads: {@link BlockMasterSync} (Worker to Master continuous communication)
 *
 * Logic: {@link BlockWorker} (Logic for all block related storage operations)
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1624)
public final class BlockWorker extends AbstractWorker {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Runnable responsible for heartbeating and registration with master. */
  private BlockMasterSync mBlockMasterSync;

  /** Runnable responsible for fetching pinlist from master. */
  private PinListSync mPinListSync;

  /** Runnable responsible for clean up potential zombie sessions. */
  private SessionCleaner mSessionCleanerThread;

  /** Logic for handling RPC requests. */
  private final BlockWorkerClientServiceHandler mServiceHandler;

  /** Client for all block master communication. */
  private final BlockMasterClient mBlockMasterClient;

  /** Client for all file system master communication. */
  private final FileSystemMasterClient mFileSystemMasterClient;

  /** Configuration object. */
  private final Configuration mConf;

  /** Space reserver for the block data manager. */
  private SpaceReserver mSpaceReserver = null;
  /** Block store delta reporter for master heartbeat. */
  private BlockHeartbeatReporter mHeartbeatReporter;
  /** Metrics reporter that listens on block events and increases metrics counters. */
  private BlockMetricsReporter mMetricsReporter;
  /** Session metadata, used to keep track of session heartbeats. */
  private Sessions mSessions;
  /** Block Store manager. */
  private BlockStore mBlockStore;

  /**
   * @return the worker data service bind host
   */
  public BlockStore getBlockStore() {
    return mBlockStore;
  }

  /**
   * @return the worker service handler
   */
  public BlockWorkerClientServiceHandler getWorkerServiceHandler() {
    return mServiceHandler;
  }

  /**
   * Creates a new instance of {@link BlockWorker}.
   *
   * @throws IOException for other exceptions
   */
  public BlockWorker() throws IOException {
    super(Executors.newFixedThreadPool(4,
        ThreadFactoryUtils.build("block-worker-heartbeat-%d", true)));
    mConf = WorkerContext.getConf();

    // Setup BlockMasterClient
    mBlockMasterClient = new BlockMasterClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, mConf), mConf);

    mFileSystemMasterClient = new FileSystemMasterClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, mConf), mConf);

    // Setup RPC ServerHandler
    mServiceHandler = new BlockWorkerClientServiceHandler(this);

    // Setup BlockHeartbeatReporter
    mHeartbeatReporter = new BlockHeartbeatReporter();
    // Setup BlockMetricsReporter
    mMetricsReporter = new BlockMetricsReporter(WorkerContext.getWorkerSource());
    // Setup Sessions
    mSessions = new Sessions();
    // Setup the BlockStore
    mBlockStore = new TieredBlockStore();

    // Register the heartbeat reporter so it can record block store changes
    mBlockStore.registerBlockStoreEventListener(mHeartbeatReporter);
    mBlockStore.registerBlockStoreEventListener(mMetricsReporter);
  }

  @Override
  public Map<String, TProcessor> getServices() {
    Map<String, TProcessor> services = new HashMap<String, TProcessor>();
    services.put(
        Constants.BLOCK_WORKER_CLIENT_SERVICE_NAME,
        new BlockWorkerClientService.Processor<BlockWorkerClientServiceHandler>(
            getWorkerServiceHandler()));
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
    WorkerNetAddress netAddress;
    try {
      netAddress = WorkerContext.getNetAddress();
      WorkerIdRegistry.registerWithBlockMaster(mBlockMasterClient, netAddress);
    } catch (ConnectionFailedException e) {
      LOG.error("Failed to get a worker id from block master", e);
      throw Throwables.propagate(e);
    }

    // Setup BlockMasterSync
    mBlockMasterSync =
        new BlockMasterSync(this, netAddress, mBlockMasterClient);

    // Setup PinListSyncer
    mPinListSync = new PinListSync(this, mFileSystemMasterClient);

    // Setup session cleaner
    mSessionCleanerThread = new SessionCleaner(this);

    // Setup space reserver
    if (mConf.getBoolean(Constants.WORKER_TIERED_STORE_RESERVER_ENABLED)) {
      mSpaceReserver = new SpaceReserver(this);
    }

    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_BLOCK_SYNC, mBlockMasterSync,
            WorkerContext.getConf().getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)));

    // Start the pinlist syncer to perform the periodical fetching
    getExecutorService()
        .submit(new HeartbeatThread(HeartbeatContext.WORKER_PIN_LIST_SYNC, mPinListSync,
            WorkerContext.getConf().getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)));

    // Start the session cleanup checker to perform the periodical checking
    getExecutorService().submit(mSessionCleanerThread);

    // Start the space reserver
    if (mSpaceReserver != null) {
      getExecutorService().submit(mSpaceReserver);
    }
  }

  /**
   * Stops the block worker. This method should only be called to terminate the worker.
   *
   * @throws IOException if the data server fails to close
   */
  @Override
  public void stop() throws IOException {
    mSessionCleanerThread.stop();
    mBlockMasterClient.close();
    if (mSpaceReserver != null) {
      mSpaceReserver.stop();
    }
    mFileSystemMasterClient.close();
    // Use shutdownNow because HeartbeatThreads never finish until they are interrupted
    getExecutorService().shutdownNow();
  }

  /**
   * Aborts the temporary block created by the session.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to be aborted
   * @throws BlockAlreadyExistsException if blockId already exists in committed blocks
   * @throws BlockDoesNotExistException if the temporary block cannot be found
   * @throws InvalidWorkerStateException if blockId does not belong to sessionId
   * @throws IOException if temporary block cannot be deleted
   */
  public void abortBlock(long sessionId, long blockId) throws BlockAlreadyExistsException,
      BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    mBlockStore.abortBlock(sessionId, blockId);
  }

  /**
   * Access the block for a given session. This should be called to update the evictor when
   * necessary.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to access
   * @throws BlockDoesNotExistException this exception is not thrown in the tiered block store
   *         implementation
   */
  public void accessBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    mBlockStore.accessBlock(sessionId, blockId);
  }

  /**
   * Cleans up after sessions, to prevent zombie sessions. This method is called periodically by
   * {@link SessionCleaner} thread.
   */
  public void cleanupSessions() {
    for (long session : mSessions.getTimedOutSessions()) {
      mSessions.removeSession(session);
      mBlockStore.cleanupSession(session);
    }
  }

  /**
   * Commits a block to Alluxio managed space. The block must be temporary. The block is persisted
   * after {@link BlockStore#commitBlock(long, long)}. The block will not be accessible until
   * {@link BlockMasterClient#commitBlock(long, long, String, long, long)} succeeds.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to commit
   * @throws BlockAlreadyExistsException if blockId already exists in committed blocks
   * @throws BlockDoesNotExistException if the temporary block cannot be found
   * @throws InvalidWorkerStateException if blockId does not belong to sessionId
   * @throws IOException if the block cannot be moved from temporary path to committed path
   * @throws WorkerOutOfSpaceException if there is no more space left to hold the block
   */
  public void commitBlock(long sessionId, long blockId)
      throws BlockAlreadyExistsException, BlockDoesNotExistException, InvalidWorkerStateException,
      IOException, WorkerOutOfSpaceException {
    mBlockStore.commitBlock(sessionId, blockId);

    // TODO(calvin): Reconsider how to do this without heavy locking.
    // Block successfully committed, update master with new block metadata
    Long lockId = mBlockStore.lockBlock(sessionId, blockId);
    try {
      BlockMeta meta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
      BlockStoreLocation loc = meta.getBlockLocation();
      Long length = meta.getBlockSize();
      BlockStoreMeta storeMeta = mBlockStore.getBlockStoreMeta();
      Long bytesUsedOnTier = storeMeta.getUsedBytesOnTiers().get(loc.tierAlias());
      mBlockMasterClient.commitBlock(WorkerIdRegistry.getWorkerId(), bytesUsedOnTier,
          loc.tierAlias(), blockId, length);
    } catch (IOException ioe) {
      throw new IOException("Failed to commit block to master.", ioe);
    } catch (ConnectionFailedException e) {
      throw new IOException("Failed to commit block to master.", e);
    } finally {
      mBlockStore.unlockBlock(lockId);
    }
  }

  /**
   * Creates a block in Alluxio managed space. The block will be temporary until it is committed.
   * Throws an {@link IllegalArgumentException} if the location does not belong to tiered storage.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to create
   * @param tierAlias the alias of the tier to place the new block in,
   *        {@link BlockStoreLocation#ANY_TIER} for any tier
   * @param initialBytes the initial amount of bytes to be allocated
   * @return a string representing the path to the local file
   * @throws BlockAlreadyExistsException if blockId already exists, either temporary or committed,
   *         or block in eviction plan already exists
   * @throws WorkerOutOfSpaceException if this Store has no more space than the initialBlockSize
   * @throws IOException if blocks in eviction plan fail to be moved or deleted
   */
  public String createBlock(long sessionId, long blockId, String tierAlias, long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(tierAlias);
    TempBlockMeta createdBlock = mBlockStore.createBlockMeta(sessionId, blockId, loc, initialBytes);
    String blockPath = createdBlock.getPath();
    createBlockFile(blockPath);
    return blockPath;
  }

  /**
   * Creates a block. This method is only called from a data server.
   * Calls {@link #getTempBlockWriterRemote(long, long)} to get a writer for writing to the block.
   * Throws an {@link IllegalArgumentException} if the location does not belong to tiered storage.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to be created
   * @param tierAlias the alias of the tier to place the new block in
   * @param initialBytes the initial amount of bytes to be allocated
   * @throws BlockAlreadyExistsException if blockId already exists, either temporary or committed,
   *         or block in eviction plan already exists
   * @throws WorkerOutOfSpaceException if this Store has no more space than the initialBlockSize
   * @throws IOException if blocks in eviction plan fail to be moved or deleted
   */
  public void createBlockRemote(long sessionId, long blockId, String tierAlias, long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(tierAlias);
    TempBlockMeta createdBlock = mBlockStore.createBlockMeta(sessionId, blockId, loc, initialBytes);
    createBlockFile(createdBlock.getPath());
  }

  /**
   * Frees space to make a specific amount of bytes available in the tier.
   *
   * @param sessionId the session id
   * @param availableBytes the amount of free space in bytes
   * @param tierAlias the alias of the tier to free space
   * @throws WorkerOutOfSpaceException if there is not enough space
   * @throws BlockDoesNotExistException if blocks can not be found
   * @throws IOException if blocks fail to be moved or deleted on file system
   * @throws BlockAlreadyExistsException if blocks to move already exists in destination location
   * @throws InvalidWorkerStateException if blocks to move/evict is uncommitted
   */
  public void freeSpace(long sessionId, long availableBytes, String tierAlias)
      throws WorkerOutOfSpaceException, BlockDoesNotExistException, IOException,
      BlockAlreadyExistsException, InvalidWorkerStateException {
    BlockStoreLocation location = BlockStoreLocation.anyDirInTier(tierAlias);
    mBlockStore.freeSpace(sessionId, availableBytes, location);
  }

  /**
   * Opens a {@link BlockWriter} for an existing temporary block. This method is only called from a
   * data server.
   *
   * The temporary block must already exist with
   * {@link #createBlockRemote(long, long, String, long)}.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to be opened for writing
   * @return the block writer for the local block file
   * @throws BlockDoesNotExistException if the block cannot be found
   * @throws IOException if block cannot be created
   */
  public BlockWriter getTempBlockWriterRemote(long sessionId, long blockId)
      throws BlockDoesNotExistException, IOException {
    return mBlockStore.getBlockWriter(sessionId, blockId);
  }

  /**
   * Gets a report for the periodic heartbeat to master. Contains the blocks added since the last
   * heart beat and blocks removed since the last heartbeat.
   *
   * @return a block heartbeat report
   */
  public BlockHeartbeatReport getReport() {
    return mHeartbeatReporter.generateReport();
  }

  /**
   * Gets the metadata for the entire block store. Contains the block mapping per storage dir and
   * the total capacity and used capacity of each tier.
   *
   * @return the block store metadata
   */
  public BlockStoreMeta getStoreMeta() {
    return mBlockStore.getBlockStoreMeta();
  }

  /**
   * Gets the metadata of a block given its blockId or throws IOException. This method does not
   * require a lock id so the block is possible to be moved or removed after it returns.
   *
   * @param blockId the block id
   * @return metadata of the block
   * @throws BlockDoesNotExistException if no {@link BlockMeta} for this blockId is found
   */
  public BlockMeta getVolatileBlockMeta(long blockId) throws BlockDoesNotExistException {
    return mBlockStore.getVolatileBlockMeta(blockId);
  }

  /**
   * Checks if the storage has a given block.
   *
   * @param blockId the block id
   * @return true if the block is contained, false otherwise
   */
  public boolean hasBlockMeta(long blockId) {
    return mBlockStore.hasBlockMeta(blockId);
  }

  /**
   * Obtains a read lock the block.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to be locked
   * @return the lock id that uniquely identifies the lock obtained
   * @throws BlockDoesNotExistException if blockId cannot be found, for example, evicted already
   */
  public long lockBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    return mBlockStore.lockBlock(sessionId, blockId);
  }

  /**
   * Moves a block from its current location to a target location, currently only tier level moves
   * are supported. Throws an {@link IllegalArgumentException} if the tierAlias is out of range of
   * tiered storage.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to move
   * @param tierAlias the alias of the tier to move the block to
   * @throws BlockDoesNotExistException if blockId cannot be found
   * @throws BlockAlreadyExistsException if blockId already exists in committed blocks of the
   *         newLocation
   * @throws InvalidWorkerStateException if blockId has not been committed
   * @throws WorkerOutOfSpaceException if newLocation does not have enough extra space to hold the
   *         block
   * @throws IOException if block cannot be moved from current location to newLocation
   */
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

  /**
   * Gets the path to the block file in local storage. The block must be a permanent block, and the
   * caller must first obtain the lock on the block.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to read
   * @param lockId the id of the lock on this block
   * @return a string representing the path to this block in local storage
   * @throws BlockDoesNotExistException if the blockId cannot be found in committed blocks or lockId
   *         cannot be found
   * @throws InvalidWorkerStateException if sessionId or blockId is not the same as that in the
   *         LockRecord of lockId
   */
  public String readBlock(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException {
    BlockMeta meta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
    return meta.getPath();
  }

  /**
   * Gets the block reader for the block. This method is only called by a data server.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to read
   * @param lockId the id of the lock on this block
   * @return the block reader for the block
   * @throws BlockDoesNotExistException if lockId is not found
   * @throws InvalidWorkerStateException if sessionId or blockId is not the same as that in the
   *         LockRecord of lockId
   * @throws IOException if block cannot be read
   */
  public BlockReader readBlockRemote(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    return mBlockStore.getBlockReader(sessionId, blockId, lockId);
  }

  /**
   * Frees a block from Alluxio managed space.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to be freed
   * @throws InvalidWorkerStateException if blockId has not been committed
   * @throws BlockDoesNotExistException if block cannot be found
   * @throws IOException if block cannot be removed from current path
   */
  public void removeBlock(long sessionId, long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException {
    mBlockStore.removeBlock(sessionId, blockId);
  }

  /**
   * Request an amount of space for a block in its storage directory. The block must be a temporary
   * block.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to allocate space to
   * @param additionalBytes the amount of bytes to allocate
   * @throws BlockDoesNotExistException if blockId can not be found, or some block in eviction plan
   *         cannot be found
   * @throws WorkerOutOfSpaceException if requested space can not be satisfied
   * @throws IOException if blocks in {@link alluxio.worker.block.evictor.EvictionPlan} fail to be
   *         moved or deleted on file system
   */
  public void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException {
    mBlockStore.requestSpace(sessionId, blockId, additionalBytes);
  }

  /**
   * Releases the lock with the specified lock id.
   *
   * @param lockId the id of the lock to release
   * @throws BlockDoesNotExistException if lock id cannot be found
   */
  public void unlockBlock(long lockId) throws BlockDoesNotExistException {
    mBlockStore.unlockBlock(lockId);
  }

  /**
   * Releases the lock with the specified session and block id.
   *
   * @param sessionId the session id
   * @param blockId the block id
   * @throws BlockDoesNotExistException if block id cannot be found
   */
  // TODO(calvin): Remove when lock and reads are separate operations.
  public void unlockBlock(long sessionId, long blockId) throws BlockDoesNotExistException {
    mBlockStore.unlockBlock(sessionId, blockId);
  }

  /**
   * Handles the heartbeat from a client.
   *
   * @param sessionId the id of the client
   * @param metrics the set of metrics the client has gathered since the last heartbeat
   */
  public void sessionHeartbeat(long sessionId, List<Long> metrics) {
    mSessions.sessionHeartbeat(sessionId);
    mMetricsReporter.updateClientMetrics(metrics);
  }

  /**
   * Sets the pinlist for the underlying block store. Typically called by {@link PinListSync}.
   *
   * @param pinnedInodes a set of pinned inodes
   */
  public void updatePinList(Set<Long> pinnedInodes) {
    mBlockStore.updatePinnedInodes(pinnedInodes);
  }

  /**
   * Gets the file information.
   *
   * @param fileId the file id
   * @return the file info
   * @throws IOException if an I/O error occurs
   */
  public FileInfo getFileInfo(long fileId) throws IOException {
    try {
      return mFileSystemMasterClient.getFileInfo(fileId);
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
  }

  /**
   * Creates a file to represent a block denoted by the given block path. This file will be owned
   * by the Alluxio worker but have 777 permissions so processes under users different from the
   * user that launched the Alluxio worker can read and write to the file. The tiered storage
   * directory has the sticky bit so only the worker user can delete or rename files it creates.
   *
   * @param blockPath the block path to create
   * @throws IOException if the file cannot be created in the tiered storage folder
   */
  private void createBlockFile(String blockPath) throws IOException {
    FileUtils.createBlockPath(blockPath);
    FileUtils.createFile(blockPath);
    FileUtils.changeLocalFileToFullPermission(blockPath);
    LOG.info("Created new file block, block path: {}", blockPath);
  }
}
