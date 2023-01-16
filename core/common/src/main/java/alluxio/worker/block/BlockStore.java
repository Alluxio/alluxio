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
import alluxio.grpc.Block;
import alluxio.grpc.BlockStatus;
import alluxio.grpc.UfsReadOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.SessionCleanable;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * An abstraction of block store on worker.
 */
public interface BlockStore extends Closeable, SessionCleanable {

  /**
   * Aborts a temporary block. The metadata of this block will not be added, its data will be
   * deleted and the space will be reclaimed. Since a temp block is "private" to the writer, this
   * requires no previously acquired lock.
   *
   * @param sessionId the id of the session
   * @param blockId the id of a temp block
   */
  void abortBlock(long sessionId, long blockId);

  /**
   * Notifies the block store that a block was accessed so the block store could update accordingly
   * the registered listeners such as evictor and allocator on block access.
   * //TODO(beinan): looks like we should not expose this method except the test
   *
   * @param sessionId the id of the session to access a block
   * @param blockId the id of an accessed block
   */
  void accessBlock(long sessionId, long blockId);

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
   * Creates a block in Alluxio managed space.
   * Calls {@link #createBlockWriter} to get a writer for writing to the block.
   * The block will be temporary until it is committed by {@link #commitBlock} .
   * Throws an {@link IllegalArgumentException} if the location does not belong to tiered storage.
   *
   * @param sessionId the id of the client
   * @param blockId the id of the block to create
   * @param tier the tier to place the new block in
   * {@link BlockStoreLocation#ANY_TIER} for any tier
   * @param createBlockOptions the createBlockOptions
   * @return a string representing the path to the local file
   */
  String createBlock(long sessionId, long blockId, int tier,
      CreateBlockOptions createBlockOptions);

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
   * Creates a reader of an existing block to read data from this block.
   * <p>
   * This operation requires the lock id returned by a previously acquired
   * {@link #lockBlock(long, long)}.
   *
   * @param sessionId the id of the session to get the reader
   * @param blockId the id of an existing block
   * @param lockId the id of the lock returned by {@link #lockBlock(long, long)}
   * @return a {@link BlockReader} instance on this block
   * @throws BlockDoesNotExistException if lockId is not found
   * @throws InvalidWorkerStateException if session id or block id is not the same as that in the
   *         LockRecord of lockId
   */
  BlockReader createBlockReader(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException, IOException;

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
      Protocol.OpenUfsBlockOptions options)
      throws IOException;

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
   * Gets the metadata of the entire store in a snapshot. There is no guarantee the state will be
   * consistent with the snapshot after this method is called.
   * This function should be cheap since it is called for every block.
   *
   * @return store metadata
   */
  BlockStoreMeta getBlockStoreMeta();

  /**
   * Similar as {@link #getBlockStoreMeta} except that this includes
   * more information about the block store (e.g. blockId list). This is an expensive operation.
   *
   * @return full store metadata
   */
  BlockStoreMeta getBlockStoreMetaFull();

  /**
   * Gets the temp metadata of a specific block from local storage.
   *
   * @param blockId the id of the block
   * @return metadata of the block if the temp block exists
   */
  Optional<TempBlockMeta> getTempBlockMeta(long blockId);

  /**
   * Checks if the storage has a given block.
   *
   * @param blockId the block id
   * @return true if the block is contained, false otherwise
   */
  boolean hasBlockMeta(long blockId);

  /**
   * Checks if the storage has a given temp block.
   *
   * @param blockId the temp block id
   * @return true if the block is contained, false otherwise
   */
  boolean hasTempBlockMeta(long blockId);

  /**
   * Gets the metadata of a block given its block id or empty if block does not exist.
   * This method does not require a lock id so the block is possible to be moved or removed after it
   * returns.
   *
   * @param blockId the block id
   * @return metadata of the block
   */
  Optional<BlockMeta> getVolatileBlockMeta(long blockId);

  /**
   * Moves an existing block to a new location.
   *
   * @param sessionId the id of the session to move a block
   * @param blockId the id of an existing block
   * @param moveOptions the options for move
   */
  void moveBlock(long sessionId, long blockId, AllocateOptions moveOptions)
      throws IOException;

  /**
   * Pins the block indicating subsequent access.
   *
   * @param sessionId the id of the session to lock this block
   * @param blockId the id of the block to lock
   * @return a lock of block to conveniently unpin the block later, or empty
   * if the block does not exist
   */
  Optional<BlockLock> pinBlock(long sessionId, long blockId);

  /**
   * Unpins an accessed block based on the id (returned by {@link #pinBlock(long, long)}).
   *
   * @param lock the lock returned by {@link #pinBlock(long, long)}
   */
  void unpinBlock(BlockLock lock);

  /**
   * Update the pinned inodes.
   *
   * @param inodes a set of inodes that are currently pinned
   */
  void updatePinnedInodes(Set<Long> inodes);

  /**
   * Registers a {@link BlockStoreEventListener} to this block store.
   *
   * @param listener the listener to those events
   */
  void registerBlockStoreEventListener(BlockStoreEventListener listener);

  /**
   * Removes an existing block. If the block can not be found in this store.
   *
   * @param sessionId the id of the session to remove a block
   * @param blockId the id of an existing block
   */
  void removeBlock(long sessionId, long blockId) throws IOException;

  /**
   * Remove Storage directories that are no longer accessible.
   */
  void removeInaccessibleStorage();

  /**
   * Requests to increase the size of a temp block. Since a temp block is "private" to the writer
   * client, this operation requires no previously acquired lock.
   *
   * @param sessionId the id of the session to request space
   * @param blockId the id of the temp block
   * @param additionalBytes the amount of more space to request in bytes, never be less than 0
   */
  void requestSpace(long sessionId, long blockId, long additionalBytes);

  /**
   * Load blocks into alluxio.
   *
   * @param fileBlocks list of fileBlocks, one file blocks contains blocks belong to one file
   * @param options read ufs options
   * @return future of load status for failed blocks
   */
  CompletableFuture<List<BlockStatus>> load(List<Block> fileBlocks, UfsReadOptions options);

  /**
   * Gets the metadata of a specific block from local storage.
   * <p>
   * This method requires the lock id returned by a previously acquired
   * {@link #lockBlock(long, long)}.
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
   * Locks an existing block and guards subsequent reads on this block.
   *
   * @param sessionId the id of the session to lock this block
   * @param blockId the id of the block to lock
   * @return the lock id (non-negative) if the lock is acquired successfully
   * @throws BlockDoesNotExistException if block id can not be found, for example, evicted already
   */
  long lockBlock(long sessionId, long blockId) throws BlockDoesNotExistException;

  /**
   * Locks an existing block and guards subsequent reads on this block. If the lock fails, return
   * {@link BlockLockManager#INVALID_LOCK_ID}.
   *
   * @param sessionId the id of the session to lock this block
   * @param blockId the id of the block to lock
   * @return the lock id (non-negative) that uniquely identifies the lock obtained or
   *         {@link BlockLockManager#INVALID_LOCK_ID} if it failed to lock
   */
  long lockBlockNoException(long sessionId, long blockId);

  /**
   * Releases an acquired block lock based on a lockId (returned by {@link #lockBlock(long, long)}.
   *
   * @param lockId the id of the lock returned by {@link #lockBlock(long, long)}
   * @throws BlockDoesNotExistException if lockId can not be found
   */
  void unlockBlock(long lockId) throws BlockDoesNotExistException;

  /**
   * Releases an acquired block lock based on a session id and block id.
   * TODO(calvin): temporary, will be removed after changing client side code.
   *
   * @param sessionId the id of the session to lock this block
   * @param blockId the id of the block to lock
   * @return false if it fails to unlock due to the lock is not found
   */
  boolean unlockBlock(long sessionId, long blockId);

  /**
   * Creates a writer to write data to a temp block. Since the temp block is "private" to the
   * writer, this operation requires no previously acquired lock.
   *
   * @param sessionId the id of the session to get the writer
   * @param blockId the id of the temp block
   * @return a {@link BlockWriter} instance on this block
   * @throws BlockDoesNotExistException if the block can not be found
   * @throws BlockAlreadyExistsException if a committed block with the same ID exists
   * @throws InvalidWorkerStateException if the worker state is invalid
   */
  BlockWriter getBlockWriter(long sessionId, long blockId)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      IOException;
}
