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

import javax.annotation.concurrent.NotThreadSafe;

import tachyon.Sessions;
import tachyon.exception.BlockAlreadyExistsException;
import tachyon.exception.BlockDoesNotExistException;
import tachyon.exception.ConnectionFailedException;
import tachyon.exception.InvalidWorkerStateException;
import tachyon.exception.TachyonException;
import tachyon.exception.WorkerOutOfSpaceException;
import tachyon.thrift.FileInfo;
import tachyon.util.io.FileUtils;
import tachyon.worker.WorkerIdRegistry;
import tachyon.worker.WorkerSource;
import tachyon.worker.block.evictor.EvictionPlan;
import tachyon.worker.block.io.BlockReader;
import tachyon.worker.block.io.BlockWriter;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.TempBlockMeta;
import tachyon.worker.file.FileSystemMasterClient;

/**
 * Class is responsible for managing the Tachyon {@link BlockStore} and under file system.
 */
@NotThreadSafe
// TODO(jiri): Make this class actually thread-safe.
public final class BlockDataManager {

  /** Block store delta reporter for master heartbeat. */
  private BlockHeartbeatReporter mHeartbeatReporter;

  /** Block store manager. */
  private BlockStore mBlockStore;

  /** {@link WorkerSource} for collecting worker metrics. */
  private WorkerSource mWorkerSource;

  /** Metrics reporter that listens on block events and increases metrics counters. */
  private BlockMetricsReporter mMetricsReporter;

  /** WorkerBlockMasterClient, only used to inform the master of a new block in commitBlock. */
  private BlockMasterClient mBlockMasterClient;

  /** WorkerFileSystemMasterClient, only used to inform master of a new file in persistFile. */
  private FileSystemMasterClient mFileSystemMasterClient;

  /** Session metadata, used to keep track of session heartbeats. */
  private Sessions mSessions = new Sessions();

  /**
   * Creates a new instace of {@link BlockDataManager} based on the configuration values.
   *
   * @param workerSource object for collecting the worker metrics
   * @param blockMasterClient the block Tachyon master client for worker
   * @param fileSystemMasterClient the file system Tachyon master client for worker
   * @param blockStore the block store manager
   * @throws IOException if fail to connect to under filesystem
   */
  public BlockDataManager(WorkerSource workerSource, BlockMasterClient blockMasterClient,
      FileSystemMasterClient fileSystemMasterClient, BlockStore blockStore) throws IOException {
    mHeartbeatReporter = new BlockHeartbeatReporter();
    mBlockStore = blockStore;
    mWorkerSource = workerSource;
    mMetricsReporter = new BlockMetricsReporter(mWorkerSource);

    mBlockMasterClient = blockMasterClient;
    mFileSystemMasterClient = fileSystemMasterClient;

    // Register the heartbeat reporter so it can record block store changes
    mBlockStore.registerBlockStoreEventListener(mHeartbeatReporter);
    mBlockStore.registerBlockStoreEventListener(mMetricsReporter);
  }

  /**
   * Aborts the temporary block created by the session.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to be aborted
   * @throws BlockAlreadyExistsException if block id already exists in committed blocks
   * @throws BlockDoesNotExistException if the temporary block cannot be found
   * @throws InvalidWorkerStateException if block id does not belong to getSessionId
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
   * Commits a block to Tachyon managed space. The block must be temporary. The block is persisted
   * after {@link BlockStore#commitBlock(long, long)}. The block will not be accessible until
   * {@link BlockMasterClient#commitBlock(long, long, String, long, long)} succeeds.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to commit
   * @throws BlockAlreadyExistsException if block id already exists in committed blocks
   * @throws BlockDoesNotExistException if the temporary block cannot be found
   * @throws InvalidWorkerStateException if block id does not belong to getSessionId
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
    } catch (IOException e) {
      throw new IOException("Failed to commit block to master.", e);
    } catch (ConnectionFailedException e) {
      throw new IOException("Failed to commit block to master.", e);
    } finally {
      mBlockStore.unlockBlock(lockId);
    }
  }

  /**
   * Creates a block in Tachyon managed space. The block will be temporary until it is committed.
   * Throws an {@link IllegalArgumentException} if the location does not belong to tiered storage.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to create
   * @param tierAlias the alias of the tier to place the new block in,
   *        {@link BlockStoreLocation#ANY_TIER} for any tier
   * @param initialBytes the initial amount of bytes to be allocated
   * @return a string representing the path to the local file
   * @throws BlockAlreadyExistsException if block id already exists, either temporary or committed,
   *         or block in eviction plan already exists
   * @throws WorkerOutOfSpaceException if this Store has no more space than the initialBlockSize
   * @throws IOException if blocks in eviction plan fail to be moved or deleted
   */
  public String createBlock(long sessionId, long blockId, String tierAlias, long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(tierAlias);
    TempBlockMeta createdBlock = mBlockStore.createBlockMeta(sessionId, blockId, loc, initialBytes);
    return createdBlock.getPath();
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
   * @throws BlockAlreadyExistsException if block id already exists, either temporary or committed,
   *         or block in eviction plan already exists
   * @throws WorkerOutOfSpaceException if this Store has no more space than the initialBlockSize
   * @throws IOException if blocks in eviction plan fail to be moved or deleted
   */
  public void createBlockRemote(long sessionId, long blockId, String tierAlias, long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException {
    BlockStoreLocation loc = BlockStoreLocation.anyDirInTier(tierAlias);
    TempBlockMeta createdBlock = mBlockStore.createBlockMeta(sessionId, blockId, loc, initialBytes);
    FileUtils.createBlockPath(createdBlock.getPath());
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
   * Gets the metadata of a block given its block id or throws IOException. This method does not
   * require a lock id so the block is possible to be moved or removed after it returns.
   *
   * @param blockId the block id
   * @return metadata of the block
   * @throws BlockDoesNotExistException if no {@link BlockMeta} for this block id is found
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
   * @throws BlockDoesNotExistException if block id cannot be found, for example, evicted already
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
   * @throws BlockDoesNotExistException if block id cannot be found
   * @throws BlockAlreadyExistsException if block id already exists in committed blocks of the
   *         newLocation
   * @throws InvalidWorkerStateException if block id has not been committed
   * @throws WorkerOutOfSpaceException if newLocation does not have enough extra space to hold the
   *         block
   * @throws IOException if block cannot be moved from current location to newLocation
   */
  public void moveBlock(long sessionId, long blockId, String tierAlias)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException {
    BlockStoreLocation dst = BlockStoreLocation.anyDirInTier(tierAlias);
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
   * @throws BlockDoesNotExistException if the block id cannot be found in committed blocks or lock
   *         id cannot be found
   * @throws InvalidWorkerStateException if session id or block id is not the same as that in the
   *         LockRecord of lock id
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
   * @throws BlockDoesNotExistException if lock id is not found
   * @throws InvalidWorkerStateException if session id or block id is not the same as that in the
   *         LockRecord of lockId
   * @throws IOException if block cannot be read
   */
  public BlockReader readBlockRemote(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException, IOException {
    return mBlockStore.getBlockReader(sessionId, blockId, lockId);
  }

  /**
   * Frees a block from Tachyon managed space.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to be freed
   * @throws InvalidWorkerStateException if block id has not been committed
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
   * @throws BlockDoesNotExistException if block id can not be found, or some block in eviction plan
   *         cannot be found
   * @throws WorkerOutOfSpaceException if requested space can not be satisfied
   * @throws IOException if blocks in {@link EvictionPlan} fail to be moved or deleted on file
   *         system
   */
  public void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException {
    mBlockStore.requestSpace(sessionId, blockId, additionalBytes);
  }

  /**
   * Stops the block data manager. This method should only be called when terminating the worker.
   */
  public void stop() {}

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
    } catch (TachyonException e) {
      throw new IOException(e);
    }
  }
}
