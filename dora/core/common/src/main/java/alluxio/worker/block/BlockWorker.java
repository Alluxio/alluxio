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

import alluxio.exception.AlluxioException;
import alluxio.exception.BlockAlreadyExistsException;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.UfsBlockAccessTokenUnavailableException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.grpc.AsyncCacheRequest;
import alluxio.grpc.Block;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.CacheRequest;
import alluxio.grpc.GetConfigurationPOptions;
import alluxio.grpc.UfsReadOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.wire.Configuration;
import alluxio.wire.FileInfo;
import alluxio.worker.DataWorker;
import alluxio.worker.SessionCleanable;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A block worker in the Alluxio system.
 */
public interface BlockWorker extends DataWorker, SessionCleanable {
  /**
   * Aborts the temporary block created by the session.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to be aborted
   */
  void abortBlock(long sessionId, long blockId) throws IOException;

  /**
   * Commits a block to Alluxio managed space. The block must be temporary. The block will not be
   * persisted or accessible before commitBlock succeeds.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to commit
   * @param pinOnCreate whether to pin block on create
   */
  void commitBlock(long sessionId, long blockId, boolean pinOnCreate);

  /**
   * Commits a block in UFS.
   *
   * @param blockId the id of the block to commit
   * @param length length of the block to commit
   */
  void commitBlockInUfs(long blockId, long length);

  /**
   * Creates a block in Alluxio managed space.
   * Calls {@link #createBlockWriter} to get a writer for writing to the block.
   * The block will be temporary until it is committed by {@link #commitBlock} .
   * Throws an {@link IllegalArgumentException} if the location does not belong to tiered storage.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to create
   * @param tier the tier to place the new block in
   *        {@link BlockStoreLocation#ANY_TIER} for any tier
   * @param createBlockOptions the createBlockOptions
   * @return a string representing the path to the local file
   */
  String createBlock(long sessionId, long blockId, int tier,
      CreateBlockOptions createBlockOptions);

  /**
   * Creates a {@link BlockWriter} for an existing temporary block which is already created by
   * {@link #createBlock}.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to be opened for writing
   * @return the block writer for the local block file
   */
  BlockWriter createBlockWriter(long sessionId, long blockId)
      throws IOException;

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
   * Creates the block reader to read from Alluxio block or UFS block.
   * Owner of this block reader must close it or lock will leak.
   *
   * @param sessionId the client session ID
   * @param blockId the ID of the UFS block to read
   * @param offset the offset within the block
   * @param positionShort whether the operation is using positioned read to a small buffer size
   * @param options the options
   * @return a block reader to read data from
   * @throws IOException if it fails to get block reader
   */
  BlockReader createBlockReader(long sessionId, long blockId, long offset,
      boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws IOException;

  /**
   * Creates a block reader to read a UFS block starting from given block offset.
   * Owner of this block reader must close it to cleanup state.
   *
   * @param sessionId the client session ID
   * @param blockId the ID of the UFS block to read
   * @param offset the offset within the block
   * @param positionShort whether the operation is using positioned read to a small buffer size
   * @param options the options
   * @return the block reader instance
   * @throws IOException if it fails to get block reader
   */
  BlockReader createUfsBlockReader(long sessionId, long blockId, long offset, boolean positionShort,
      Protocol.OpenUfsBlockOptions options) throws IOException;

  /**
   * Frees a block from Alluxio managed space.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to be freed
   */
  void removeBlock(long sessionId, long blockId) throws IOException;

  /**
   * Frees all blocks in the current worker by deleting all block store directories.
   * Whether this method returns successfully or exceptionally,
   * the worker should not be used in any way.
   *
   * @throws IOException if free fails
   */
  void freeWorker() throws IOException;

  /**
   * Request an amount of space for a block in its storage directory. The block must be a temporary
   * block.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to allocate space to
   * @param additionalBytes the amount of bytes to allocate
   */
  void requestSpace(long sessionId, long blockId, long additionalBytes);

  /**
   * Submits the async cache request to async cache manager to execute.
   *
   * @param request the async cache request
   *
   * @deprecated This method will be deprecated as of v3.0, use {@link #cache}
   */
  @Deprecated
  void asyncCache(AsyncCacheRequest request);

  /**
   * Submits the cache request to cache manager to execute.
   *
   * @param request the cache request
   */
  void cache(CacheRequest request) throws AlluxioException, IOException;

  /**
   * Load blocks into alluxio.
   *
   * @param fileBlocks list of fileBlocks, one file blocks contains blocks belong to one file
   * @param options read ufs options
   * @return future of load status for failed blocks
   */
  CompletableFuture<List<BlockStatus>> load(List<Block> fileBlocks, UfsReadOptions options);

  /**
   * Sets the pinlist for the underlying block store.
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
   * Clears the worker metrics.
   */
  void clearMetrics();

  /**
   * @param options method options
   * @return configuration information list
   */
  Configuration getConfiguration(GetConfigurationPOptions options);

  /**
   * @return the white list
   */
  List<String> getWhiteList();

  /**
   * @return the block store
   */
  BlockStore getBlockStore();

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
   * Creates a block. This method is only called from a data server.
   * Calls {@link #getTempBlockWriterRemote(long, long)} to get a writer for writing to the block.
   * Throws an {@link IllegalArgumentException} if the location doens not belong to tiered storage.
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
}
