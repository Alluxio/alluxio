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

import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.UfsBlockAccessTokenUnavailableException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.FileInfo;
import alluxio.worker.SessionCleanable;
import alluxio.worker.Worker;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.BlockMeta;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A block worker in the Alluxio system.
 */
public interface BlockWorker extends Worker, SessionCleanable {
  /**
   * @return the worker data service bind host
   */
  BlockStore getBlockStore();

  /**
   * @return the worker id
   */
  AtomicReference<Long> getWorkerId();

  /**
   * @return the worker service handler
   */
  BlockWorkerClientServiceHandler getWorkerServiceHandler();

  /**
   * Aborts the temporary block created by the session.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to be aborted
   * @throws BlockAlreadyExistsException if blockId already exists in committed blocks
   * @throws BlockDoesNotExistException if the temporary block cannot be found
   * @throws InvalidWorkerStateException if blockId does not belong to sessionId
   */
  void abortBlock(long sessionId, long blockId) throws BlockAlreadyExistsException,
      BlockDoesNotExistException, InvalidWorkerStateException, IOException;

  /**
   * Access the block for a given session. This should be called to update the evictor when
   * necessary.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to access
   * @throws BlockDoesNotExistException this exception is not thrown in the tiered block store
   *         implementation
   */
  void accessBlock(long sessionId, long blockId) throws BlockDoesNotExistException;

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
   * @throws WorkerOutOfSpaceException if there is no more space left to hold the block
   */
  void commitBlock(long sessionId, long blockId)
      throws BlockAlreadyExistsException, BlockDoesNotExistException, InvalidWorkerStateException,
      IOException, WorkerOutOfSpaceException;

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
   */
  String createBlock(long sessionId, long blockId, String tierAlias, long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException;

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
   */
  void createBlockRemote(long sessionId, long blockId, String tierAlias, long initialBytes)
      throws BlockAlreadyExistsException, WorkerOutOfSpaceException, IOException;

  /**
   * Frees space to make a specific amount of bytes available in the tier.
   *
   * @param sessionId the session id
   * @param availableBytes the amount of free space in bytes
   * @param tierAlias the alias of the tier to free space
   * @throws WorkerOutOfSpaceException if there is not enough space
   * @throws BlockDoesNotExistException if blocks can not be found
   * @throws BlockAlreadyExistsException if blocks to move already exists in destination location
   * @throws InvalidWorkerStateException if blocks to move/evict is uncommitted
   */
  void freeSpace(long sessionId, long availableBytes, String tierAlias)
      throws WorkerOutOfSpaceException, BlockDoesNotExistException, IOException,
      BlockAlreadyExistsException, InvalidWorkerStateException;

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
   * @throws BlockAlreadyExistsException if a committed block with the same ID exists
   * @throws InvalidWorkerStateException if the worker state is invalid
   */
  BlockWriter getTempBlockWriterRemote(long sessionId, long blockId)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      IOException;

  /**
   * Gets a report for the periodic heartbeat to master. Contains the blocks added since the last
   * heart beat and blocks removed since the last heartbeat.
   *
   * @return a block heartbeat report
   */
  BlockHeartbeatReport getReport();

  /**
   * Gets the metadata for the entire block store. Contains the block mapping per storage dir and
   * the total capacity and used capacity of each tier. This function is cheap.
   *
   * @return the block store metadata
   */
  BlockStoreMeta getStoreMeta();

  /**
   * Similar as {@link BlockWorker#getStoreMeta} except that this also contains full blockId
   * list. This function is expensive.
   *
   * @return the full block store metadata
   */
  BlockStoreMeta getStoreMetaFull();

  /**
   * Gets the metadata of a block given its blockId or throws IOException. This method does not
   * require a lock id so the block is possible to be moved or removed after it returns.
   *
   * @param blockId the block id
   * @return metadata of the block
   * @throws BlockDoesNotExistException if no {@link BlockMeta} for this blockId is found
   */
  BlockMeta getVolatileBlockMeta(long blockId) throws BlockDoesNotExistException;

  /**
   * Gets the metadata of a specific block from local storage.
   * <p>
   * Unlike {@link #getVolatileBlockMeta(long)}, this method requires the lock id returned by a
   * previously acquired {@link #lockBlock(long, long)}.
   *
   * @param sessionId the id of the session to get this file
   * @param blockId the id of the block
   * @param lockId the id of the lock
   * @return metadata of the block
   * @throws BlockDoesNotExistException if the block id can not be found in committed blocks or
   *         lockId can not be found
   * @throws InvalidWorkerStateException if session id or block id is not the same as that in the
   *         LockRecord of lockId
   */
  BlockMeta getBlockMeta(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException;

  /**
   * Checks if the storage has a given block.
   *
   * @param blockId the block id
   * @return true if the block is contained, false otherwise
   */
  boolean hasBlockMeta(long blockId);

  /**
   * Obtains a read lock the block.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to be locked
   * @return the lock id that uniquely identifies the lock obtained
   * @throws BlockDoesNotExistException if blockId cannot be found, for example, evicted already
   */
  long lockBlock(long sessionId, long blockId) throws BlockDoesNotExistException;

  /**
   * Obtains a read lock the block without throwing an exception. If the lock fails, return
   * {@link BlockLockManager#INVALID_LOCK_ID}.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to be locked
   * @return the lock id that uniquely identifies the lock obtained or
   *         {@link BlockLockManager#INVALID_LOCK_ID} if it failed to lock
   */
  long lockBlockNoException(long sessionId, long blockId);

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
   */
  void moveBlock(long sessionId, long blockId, String tierAlias)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException;

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
  String readBlock(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException;

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
   */
  BlockReader readBlockRemote(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException, IOException;

  /**
   * Gets a block reader to read a UFS block. This method is only called by the data server.
   *
   * @param sessionId the client session ID
   * @param blockId the ID of the UFS block to read
   * @param offset the offset within the block
   * @return the block reader instance
   * @throws BlockDoesNotExistException if the block does not exist in the UFS block store
   */
  BlockReader readUfsBlock(long sessionId, long blockId, long offset)
      throws BlockDoesNotExistException, IOException;

  /**
   * Frees a block from Alluxio managed space.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to be freed
   * @throws InvalidWorkerStateException if blockId has not been committed
   * @throws BlockDoesNotExistException if block cannot be found
   */
  void removeBlock(long sessionId, long blockId)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException;

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
   */
  void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException;

  /**
   * Releases the lock with the specified lock id.
   *
   * @param lockId the id of the lock to release
   * @throws BlockDoesNotExistException if lock id cannot be found
   */
  void unlockBlock(long lockId) throws BlockDoesNotExistException;

  /**
   * Releases the lock with the specified session and block id.
   *
   * @param sessionId the session id
   * @param blockId the block id
   * @return false if it fails to unlock due to the lock is not found
   */
  // TODO(calvin): Remove when lock and reads are separate operations.
  boolean unlockBlock(long sessionId, long blockId);

  /**
   * Handles the heartbeat from a client.
   *
   * @param sessionId the id of the client
   */
  void sessionHeartbeat(long sessionId);

  /**
   * Sets the pinlist for the underlying block store. Typically called by {@link PinListSync}.
   *
   * @param pinnedInodes a set of pinned inodes
   */
  void updatePinList(Set<Long> pinnedInodes);

  /**
   * Gets the file information.
   *
   * @param fileId the file id
   * @return the file info
   */
  FileInfo getFileInfo(long fileId) throws IOException;

  /**
   * Opens a UFS block. It registers the block metadata information to the UFS block store. It
   * throws an {@link UfsBlockAccessTokenUnavailableException} if the number of concurrent readers
   * on this block exceeds a threshold.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   * @param options the options
   * @return whether the UFS block is successfully opened
   * @throws BlockAlreadyExistsException if the UFS block already exists in the
   *         {@link UnderFileSystemBlockStore}
   */
  boolean openUfsBlock(long sessionId, long blockId, Protocol.OpenUfsBlockOptions options)
      throws BlockAlreadyExistsException;

  /**
   * Closes a UFS block for a client session. It also commits the block to Alluxio block store
   * if the UFS block has been cached successfully.
   *
   * @param sessionId the session ID
   * @param blockId the block ID
   * @throws BlockAlreadyExistsException if it fails to commit the block to Alluxio block store
   *         because the block exists in the Alluxio block store
   * @throws BlockDoesNotExistException if the UFS block does not exist in the
   *         {@link UnderFileSystemBlockStore}
   * @throws WorkerOutOfSpaceException the the worker does not have enough space to commit the block
   */
  void closeUfsBlock(long sessionId, long blockId)
      throws BlockAlreadyExistsException, BlockDoesNotExistException, IOException,
      WorkerOutOfSpaceException;
}
