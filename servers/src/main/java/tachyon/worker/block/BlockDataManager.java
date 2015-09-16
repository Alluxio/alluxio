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

package tachyon.worker.block;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.Sessions;
import tachyon.client.WorkerBlockMasterClient;
import tachyon.client.WorkerFileSystemMasterClient;
import tachyon.conf.TachyonConf;
import tachyon.exception.AlreadyExistsException;
import tachyon.exception.InvalidStateException;
import tachyon.exception.NotFoundException;
import tachyon.exception.OutOfSpaceException;
import tachyon.thrift.FailedToCheckpointException;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.FileUtils;
import tachyon.util.io.PathUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;
import tachyon.worker.WorkerContext;
import tachyon.worker.WorkerSource;
import tachyon.worker.block.io.BlockReader;
import tachyon.worker.block.io.BlockWriter;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * Class is responsible for managing the Tachyon BlockStore and Under FileSystem. This class is
 * thread-safe.
 */
public final class BlockDataManager {
  /** Block store delta reporter for master heartbeat */
  private final BlockHeartbeatReporter mHeartbeatReporter;
  /** Block Store manager */
  private final BlockStore mBlockStore;
  /** Configuration values */
  private final TachyonConf mTachyonConf;
  /** WorkerSource for collecting worker metrics */
  private final WorkerSource mWorkerSource;
  /** Metrics reporter that listens on block events and increases metrics counters*/
  private final BlockMetricsReporter mMetricsReporter;

  /** WorkerBlockMasterClient, only used to inform the master of a new block in commitBlock */
  private WorkerBlockMasterClient mBlockMasterClient;
  /** WorkerFileSystemMasterClient, only used to inform master of a new file in addCheckpoint */
  private WorkerFileSystemMasterClient mFileSystemMasterClient;
  /** UnderFileSystem Client */
  private UnderFileSystem mUfs;
  /** Session metadata, used to keep track of session heartbeats */
  private Sessions mSessions;
  /** Id of this worker */
  private long mWorkerId;

  /**
   * Creates a BlockDataManager based on the configuration values.
   *
   * @param workerSource object for collecting the worker metrics
   * @param workerBlockMasterClient the block Tachyon master client for worker
   * @param workerFileSystemMasterClient the file system Tachyon master client for worker
   * @throws IOException if fail to connect to under filesystem
   */
  public BlockDataManager(WorkerSource workerSource,
      WorkerBlockMasterClient workerBlockMasterClient,
      WorkerFileSystemMasterClient workerFileSystemMasterClient) throws IOException {
    // TODO(jiri): We may not need to assign the conf to a variable
    mTachyonConf = WorkerContext.getConf();
    mHeartbeatReporter = new BlockHeartbeatReporter();
    mBlockStore = new TieredBlockStore();
    mWorkerSource = workerSource;
    mMetricsReporter = new BlockMetricsReporter(mWorkerSource);

    mBlockMasterClient = workerBlockMasterClient;
    mFileSystemMasterClient = workerFileSystemMasterClient;

    // Create Under FileSystem Client
    String ufsAddress =
        mTachyonConf.get(Constants.UNDERFS_ADDRESS);
    mUfs = UnderFileSystem.get(ufsAddress, mTachyonConf);

    // Connect to UFS to handle UFS security
    mUfs.connectFromWorker(mTachyonConf,
        NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC, mTachyonConf));

    // Register the heartbeat reporter so it can record block store changes
    mBlockStore.registerBlockStoreEventListener(mHeartbeatReporter);
    mBlockStore.registerBlockStoreEventListener(mMetricsReporter);
  }

  /**
   * Aborts the temporary block created by the session.
   *
   * @param sessionId The id of the client
   * @param blockId The id of the block to be aborted
   * @throws AlreadyExistsException if blockId already exists in committed blocks
   * @throws NotFoundException if the temporary block cannot be found
   * @throws InvalidStateException if blockId does not belong to sessionId
   * @throws IOException if temporary block cannot be deleted
   */
  public void abortBlock(long sessionId, long blockId) throws AlreadyExistsException,
      NotFoundException, InvalidStateException, IOException {
    mBlockStore.abortBlock(sessionId, blockId);
  }

  /**
   * Access the block for a given session. This should be called to update the evictor when
   * necessary.
   *
   * @param sessionId The id of the client
   * @param blockId The id of the block to access
   * @throws NotFoundException this exception is not thrown in the tiered block store implementation
   */
  public void accessBlock(long sessionId, long blockId) throws NotFoundException {
    mBlockStore.accessBlock(sessionId, blockId);
  }

  /**
   * Add the checkpoint information of a file. The information is from the session
   * <code>sessionId</code>.
   *
   * This method is normally triggered from {@link tachyon.client.FileOutStream#close()} if and only
   * if {@link tachyon.client.WriteType#isThrough()} is true. The current implementation of
   * checkpointing is that through {@link tachyon.client.WriteType} operations write to
   * {@link tachyon.underfs.UnderFileSystem} on the client's write path, but under a session temp
   * directory (temp directory is defined in the worker as {@link #getSessionUfsTmpFolder(long)}).
   *
   * @param sessionId The session id of the client who sends the notification
   * @param fileId The id of the checkpointed file
   * @throws TException if the file does not exist or cannot be renamed
   * @throws IOException if the update to the master fails
   */
  public void addCheckpoint(long sessionId, long fileId) throws TException, IOException {
    // TODO(calvin): This part needs to be changed.
    String srcPath = PathUtils.concatPath(getSessionUfsTmpFolder(sessionId), fileId);
    String ufsDataFolder =
        mTachyonConf.get(Constants.UNDERFS_DATA_FOLDER, Constants.DEFAULT_DATA_FOLDER);
    String dstPath = PathUtils.concatPath(ufsDataFolder, fileId);
    try {
      if (!mUfs.rename(srcPath, dstPath)) {
        throw new FailedToCheckpointException("Failed to rename " + srcPath + " to " + dstPath);
      }
    } catch (IOException ioe) {
      throw new FailedToCheckpointException("Failed to rename " + srcPath + " to " + dstPath);
    }
    long fileSize;
    try {
      fileSize = mUfs.getFileSize(dstPath);
    } catch (IOException ioe) {
      throw new FailedToCheckpointException("Failed to getFileSize " + dstPath);
    }
    mFileSystemMasterClient.addCheckpoint(mWorkerId, fileId, fileSize, dstPath);
  }

  /**
   * Cleans up after sessions, to prevent zombie sessions. This method is called periodically
   * by {@link SessionCleaner} thread.
   */
  public void cleanupSessions() {
    for (long session : mSessions.getTimedOutSessions()) {
      mSessions.removeSession(session);
      mBlockStore.cleanupSession(session);
    }
  }

  /**
   * Commits a block to Tachyon managed space. The block must be temporary. The block is
   * persisted after {@link BlockStore#commitBlock(long, long)}. The block will not be accessible
   * until {@link WorkerBlockMasterClient#commitBlock} succeeds
   *
   * @param sessionId The id of the client
   * @param blockId The id of the block to commit
   * @throws AlreadyExistsException if blockId already exists in committed blocks
   * @throws NotFoundException if the temporary block cannot be found
   * @throws InvalidStateException if blockId does not belong to sessionId
   * @throws IOException if the block cannot be moved from temporary path to committed path
   * @throws OutOfSpaceException if there is no more space left to hold the block
   */
  public void commitBlock(long sessionId, long blockId) throws AlreadyExistsException,
      NotFoundException, InvalidStateException, IOException, OutOfSpaceException {
    mBlockStore.commitBlock(sessionId, blockId);

    // TODO)(calvin): Reconsider how to do this without heavy locking.
    // Block successfully committed, update master with new block metadata
    Long lockId = mBlockStore.lockBlock(sessionId, blockId);
    try {
      BlockMeta meta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
      BlockStoreLocation loc = meta.getBlockLocation();
      int tier = loc.tierAlias();
      Long length = meta.getBlockSize();
      BlockStoreMeta storeMeta = mBlockStore.getBlockStoreMeta();
      Long bytesUsedOnTier = storeMeta.getUsedBytesOnTiers().get(loc.tierAlias() - 1);
      mBlockMasterClient.commitBlock(mWorkerId, bytesUsedOnTier, tier, blockId, length);
    } catch (IOException ioe) {
      throw new IOException("Failed to commit block to master.", ioe);
    } finally {
      mBlockStore.unlockBlock(lockId);
    }
  }

  /**
   * Creates a block in Tachyon managed space. The block will be temporary until it is committed.
   *
   * @param sessionId The id of the client
   * @param blockId The id of the block to create
   * @param tierAlias The alias of the tier to place the new block in, -1 for any tier
   * @param initialBytes The initial amount of bytes to be allocated
   * @return A string representing the path to the local file
   * @throws IllegalArgumentException if location does not belong to tiered storage
   * @throws AlreadyExistsException if blockId already exists, either temporary or committed, or
   *         block in eviction plan already exists
   * @throws OutOfSpaceException if this Store has no more space than the initialBlockSize
   * @throws NotFoundException if blocks in eviction plan can not be found
   * @throws IOException if blocks in eviction plan fail to be moved or deleted
   * @throws InvalidStateException if blocks to be moved/deleted in eviction plan is uncommitted
   */
  // TODO(cc): Exceptions like NotFoundException, IOException and InvalidStateException here
  // involves implementation details, also, AlreadyExistsException has two possible semantic now,
  // these are because we propagate any exception in freeSpaceInternal, revisit this by throwing
  // more general exception.
  public String createBlock(long sessionId, long blockId, int tierAlias, long initialBytes)
      throws AlreadyExistsException, OutOfSpaceException, NotFoundException, IOException,
      InvalidStateException {
    BlockStoreLocation loc =
        tierAlias == -1 ? BlockStoreLocation.anyTier() : BlockStoreLocation.anyDirInTier(tierAlias);
    TempBlockMeta createdBlock = mBlockStore.createBlockMeta(sessionId, blockId, loc, initialBytes);
    return createdBlock.getPath();
  }

  /**
   * Creates a block. This method is only called from a data server.
   *
   * Call {@link #getTempBlockWriterRemote(long, long)} to get a writer for writing to the block.
   *
   * @param sessionId The id of the client
   * @param blockId The id of the block to be created
   * @param tierAlias The alias of the tier to place the new block in, -1 for any tier
   * @param initialBytes The initial amount of bytes to be allocated
   * @throws IllegalArgumentException if location does not belong to tiered storage
   * @throws AlreadyExistsException if blockId already exists, either temporary or committed, or
   *         block in eviction plan already exists
   * @throws OutOfSpaceException if this Store has no more space than the initialBlockSize
   * @throws NotFoundException if blocks in eviction plan can not be found
   * @throws IOException if blocks in eviction plan fail to be moved or deleted
   * @throws InvalidStateException if blocks to be moved/deleted in eviction plan is uncommitted
   */
  // TODO(cc): Exceptions like NotFoundException, IOException and InvalidStateException here
  // involves implementation details, also, AlreadyExistsException has two possible semantic now,
  // these are because we propagate any exception in freeSpaceInternal, revisit this by throwing
  // more general exception.
  public void createBlockRemote(long sessionId, long blockId, int tierAlias, long initialBytes)
      throws AlreadyExistsException, OutOfSpaceException, NotFoundException, IOException,
      InvalidStateException {
    BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(tierAlias);
    TempBlockMeta createdBlock = mBlockStore.createBlockMeta(sessionId, blockId, loc, initialBytes);
    FileUtils.createBlockPath(createdBlock.getPath());
  }

  /**
   * Opens a {@link BlockWriter} for an existing temporary block. This method is only called from a
   * data server.
   *
   * The temporary block must already exist with {@link #createBlockRemote(long, long, int, long)}.
   *
   * @param sessionId The id of the client
   * @param blockId The id of the block to be opened for writing
   * @return the block writer for the local block file
   * @throws NotFoundException if the block cannot be found
   * @throws IOException if block cannot be created
   */
  public BlockWriter getTempBlockWriterRemote(long sessionId, long blockId)
      throws NotFoundException, IOException {
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
   * Gets the temporary folder for the session in the under filesystem.
   *
   * @param sessionId The id of the client
   * @return the path to the under filesystem temporary folder for the client
   */
  public String getSessionUfsTmpFolder(long sessionId) {
    return mSessions.getSessionUfsTempFolder(sessionId);
  }

  /**
   * Gets the metadata of a block given its blockId or throws IOException. This method does not
   * require a lock ID so the block is possible to be moved or removed after it returns.
   *
   * @param blockId the block ID
   * @return metadata of the block
   * @throws NotFoundException if no BlockMeta for this blockId is found
   */
  public BlockMeta getVolatileBlockMeta(long blockId) throws NotFoundException {
    return mBlockStore.getVolatileBlockMeta(blockId);
  }

  /**
   * Checks if the storage has a given block.
   *
   * @param blockId the block ID
   * @return true if the block is contained, false otherwise
   */
  public boolean hasBlockMeta(long blockId) {
    return mBlockStore.hasBlockMeta(blockId);
  }

  /**
   * Obtains a read lock the block.
   *
   * @param sessionId The id of the client
   * @param blockId The id of the block to be locked
   * @return the lockId that uniquely identifies the lock obtained
   * @throws NotFoundException if blockId cannot be found, for example, evicted already.
   */
  public long lockBlock(long sessionId, long blockId) throws NotFoundException {
    return mBlockStore.lockBlock(sessionId, blockId);
  }

  /**
   * Moves a block from its current location to a target location, currently only tier level moves
   * are supported
   *
   * @param sessionId The id of the client
   * @param blockId The id of the block to move
   * @param tierAlias The tier to move the block to
   * @throws IllegalArgumentException if tierAlias is out of range of tiered storage
   * @throws NotFoundException if blockId cannot be found
   * @throws AlreadyExistsException if blockId already exists in committed blocks of the newLocation
   * @throws InvalidStateException if blockId has not been committed
   * @throws OutOfSpaceException if newLocation does not have enough extra space to hold the block
   * @throws IOException if block cannot be moved from current location to newLocation
   */
  public void moveBlock(long sessionId, long blockId, int tierAlias) throws NotFoundException,
      AlreadyExistsException, InvalidStateException, OutOfSpaceException, IOException {
    BlockStoreLocation dst = BlockStoreLocation.anyDirInTier(tierAlias);
    mBlockStore.moveBlock(sessionId, blockId, dst);
  }

  /**
   * Gets the path to the block file in local storage. The block must be a permanent block, and the
   * caller must first obtain the lock on the block.
   *
   * @param sessionId The id of the client
   * @param blockId The id of the block to read
   * @param lockId The id of the lock on this block
   * @return a string representing the path to this block in local storage
   * @throws NotFoundException if the blockId cannot be found in committed blocks or lockId cannot
   *         be found
   * @throws InvalidStateException if sessionId or blockId is not the same as that in the LockRecord
   *         of lockId
   */
  public String readBlock(long sessionId, long blockId, long lockId)
      throws NotFoundException, InvalidStateException {
    BlockMeta meta = mBlockStore.getBlockMeta(sessionId, blockId, lockId);
    return meta.getPath();
  }

  /**
   * Gets the block reader for the block. This method is only called by a data server.
   *
   * @param sessionId The id of the client
   * @param blockId The id of the block to read
   * @param lockId The id of the lock on this block
   * @return the block reader for the block
   * @throws NotFoundException if lockId is not found
   * @throws InvalidStateException if sessionId or blockId is not the same as that in the LockRecord
   *         of lockId
   * @throws IOException if block cannot be read
   */
  public BlockReader readBlockRemote(long sessionId, long blockId, long lockId)
      throws NotFoundException, InvalidStateException, IOException {
    return mBlockStore.getBlockReader(sessionId, blockId, lockId);
  }

  /**
   * Frees a block from Tachyon managed space.
   *
   * @param sessionId The id of the client
   * @param blockId The id of the block to be freed
   * @throws InvalidStateException if blockId has not been committed
   * @throws NotFoundException if block cannot be found
   * @throws IOException if block cannot be removed from current path
   */
  public void removeBlock(long sessionId, long blockId) throws InvalidStateException,
      NotFoundException, IOException {
    mBlockStore.removeBlock(sessionId, blockId);
  }

  /**
   * Request an amount of space for a block in its storage directory. The block must be a temporary
   * block.
   *
   * @param sessionId The id of the client
   * @param blockId The id of the block to allocate space to
   * @param additionalBytes The amount of bytes to allocate
   * @throws NotFoundException if blockId can not be found, or some block in eviction plan cannot be
   *         found
   * @throws OutOfSpaceException if requested space can not be satisfied
   * @throws IOException if blocks in {@link tachyon.worker.block.evictor.EvictionPlan} fail to be
   *         moved or deleted on file system
   * @throws AlreadyExistsException if blocks to move in
   *         {@link tachyon.worker.block.evictor.EvictionPlan} already exists in destination
   *         location
   * @throws InvalidStateException if the space requested is less than current space or blocks to
   *         move/evict in {@link tachyon.worker.block.evictor.EvictionPlan} is uncommitted
   */
  // TODO(cc): Exceptions like IOException AlreadyExistsException and InvalidStateException here
  // involves implementation details, also, NotFoundException has two semantic now, revisit this
  // with a more general exception.
  public void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws NotFoundException, OutOfSpaceException, IOException, AlreadyExistsException,
      InvalidStateException {
    mBlockStore.requestSpace(sessionId, blockId, additionalBytes);
  }

  /**
   * Instantiates the session metadata object. This should only be called once and is a temporary
   * work around.
   *
   * @param sessions The session metadata object
   */
  public void setSessions(Sessions sessions) {
    mSessions = sessions;
  }

  /**
   * Sets the workerId. This should only be called once and is a temporary work around.
   *
   * @param workerId Worker id to update to
   */
  public void setWorkerId(long workerId) {
    mWorkerId = workerId;
  }

  /**
   * Stop the block data manager. This method should only be called when terminating the worker.
   */
  public void stop() {
  }

  /**
   * Relinquishes the lock with the specified lock id.
   *
   * @param lockId The id of the lock to relinquish
   * @throws NotFoundException if lockId cannot be found
   */
  public void unlockBlock(long lockId) throws NotFoundException {
    mBlockStore.unlockBlock(lockId);
  }

  // TODO(calvin): Remove when lock and reads are separate operations.
  public void unlockBlock(long sessionId, long blockId) throws NotFoundException {
    mBlockStore.unlockBlock(sessionId, blockId);
  }

  /**
   * Handles the heartbeat from a client.
   *
   * @param sessionId The id of the client
   * @param metrics The set of metrics the client has gathered since the last heartbeat
   */
  public void sessionHeartbeat(long sessionId, List<Long> metrics) {
    mSessions.sessionHeartbeat(sessionId);
    mMetricsReporter.updateClientMetrics(metrics);
  }

  /**
   * Set the pinlist for the underlying blockstore.
   * Typically called by PinListSync.
   *
   * @param pinnedInodes a set of pinned inodes
   */
  public void updatePinList(Set<Long> pinnedInodes) {
    mBlockStore.updatePinnedInodes(pinnedInodes);
  }
}
