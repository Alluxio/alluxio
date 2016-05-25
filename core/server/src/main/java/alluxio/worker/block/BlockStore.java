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
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.worker.block.evictor.EvictionPlan;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;

import java.io.IOException;
import java.util.Set;

/**
 * A blob store interface to represent the local storage managing and serving all the blocks in the
 * local storage.
 */
interface BlockStore {

  /**
   * Locks an existing block and guards subsequent reads on this block.
   *
   * @param sessionId the id of the session to lock this block
   * @param blockId the id of the block to lock
   * @return the lock id if the lock is acquired successfully
   * @throws BlockDoesNotExistException if block id can not be found, for example, evicted already
   */
  long lockBlock(long sessionId, long blockId) throws BlockDoesNotExistException;

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
   * @throws BlockDoesNotExistException if block id can not be found, for example, evicted already
   */
  void unlockBlock(long sessionId, long blockId) throws BlockDoesNotExistException;

  /**
   * Creates the meta data of a new block and assigns a temporary path (e.g., a subdir of the final
   * location named after the the session id) to store its data. This method only creates the meta
   * data but adds NO data to this temporary location. The location can be a location with specific
   * tier and dir, or {@link BlockStoreLocation#anyTier()}, or
   * {@link BlockStoreLocation#anyDirInTier(String)}.
   * <p>
   * Before commit, all the data written to this block will be stored in the temp path and the block
   * is only "visible" to its writer client.
   *
   * @param sessionId the id of the session
   * @param blockId the id of the block to create
   * @param location location to create this block
   * @param initialBlockSize initial size of this block in bytes
   * @return metadata of the temp block created
   * @throws IllegalArgumentException if location does not belong to tiered storage
   * @throws BlockAlreadyExistsException if block id already exists, either temporary or committed,
   *         or block in eviction plan already exists
   * @throws WorkerOutOfSpaceException if this Store has no more space than the initialBlockSize
   * @throws IOException if blocks in eviction plan fail to be moved or deleted
   */
  TempBlockMeta createBlockMeta(long sessionId, long blockId, BlockStoreLocation location,
      long initialBlockSize) throws BlockAlreadyExistsException, WorkerOutOfSpaceException,
      IOException;

  /**
   * Gets the metadata of a block given its block id or throws {@link BlockDoesNotExistException}.
   * This method does not require a lock id so the block is possible to be moved or removed after it
   * returns.
   *
   * @param blockId the block id
   * @return metadata of the block
   * @throws BlockDoesNotExistException if no BlockMeta for this block id is found
   */
  BlockMeta getVolatileBlockMeta(long blockId) throws BlockDoesNotExistException;

  /**
   * Gets the meta data of a specific block from local storage.
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
   * Commits a temporary block to the local store. After commit, the block will be available in this
   * block store for all clients to access. Since a temp block is "private" to the writer, this
   * method requires no previously acquired lock.
   *
   * @param sessionId the id of the session
   * @param blockId the id of a temp block
   * @throws BlockAlreadyExistsException if block id already exists in committed blocks
   * @throws BlockDoesNotExistException if the temporary block can not be found
   * @throws InvalidWorkerStateException if block id does not belong to session id
   * @throws IOException if the block can not be moved from temporary path to committed path
   * @throws WorkerOutOfSpaceException if there is no more space left to hold the block
   */
  void commitBlock(long sessionId, long blockId) throws BlockAlreadyExistsException,
      BlockDoesNotExistException, InvalidWorkerStateException, IOException,
      WorkerOutOfSpaceException;

  /**
   * Aborts a temporary block. The meta data of this block will not be added, its data will be
   * deleted and the space will be reclaimed. Since a temp block is "private" to the writer, this
   * requires no previously acquired lock.
   *
   * @param sessionId the id of the session
   * @param blockId the id of a temp block
   * @throws BlockAlreadyExistsException if block id already exists in committed blocks
   * @throws BlockDoesNotExistException if the temporary block can not be found
   * @throws InvalidWorkerStateException if block id does not belong to session id
   * @throws IOException if temporary block can not be deleted
   */
  void abortBlock(long sessionId, long blockId) throws BlockAlreadyExistsException,
      BlockDoesNotExistException, InvalidWorkerStateException, IOException;

  /**
   * Requests to increase the size of a temp block. Since a temp block is "private" to the writer
   * client, this operation requires no previously acquired lock.
   *
   * @param sessionId the id of the session to request space
   * @param blockId the id of the temp block
   * @param additionalBytes the amount of more space to request in bytes, never be less than 0
   * @throws BlockDoesNotExistException if block id can not be found, or some block in eviction plan
   *         cannot be found
   * @throws WorkerOutOfSpaceException if requested space can not be satisfied
   * @throws IOException if blocks in {@link alluxio.worker.block.evictor.EvictionPlan} fail to be
   *         moved or deleted on file system
   */
  void requestSpace(long sessionId, long blockId, long additionalBytes)
      throws BlockDoesNotExistException, WorkerOutOfSpaceException, IOException;

  /**
   * Creates a writer to write data to a temp block. Since the temp block is "private" to the
   * writer, this operation requires no previously acquired lock.
   *
   * @param sessionId the id of the session to get the writer
   * @param blockId the id of the temp block
   * @return a {@link BlockWriter} instance on this block
   * @throws BlockDoesNotExistException if the block can not be found
   * @throws IOException if block can not be created
   */
  BlockWriter getBlockWriter(long sessionId, long blockId) throws BlockDoesNotExistException,
      IOException;

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
   * @throws IOException if block can not be read
   */
  BlockReader getBlockReader(long sessionId, long blockId, long lockId)
      throws BlockDoesNotExistException, InvalidWorkerStateException, IOException;

  /**
   * Moves an existing block to a new location.
   *
   * @param sessionId the id of the session to move a block
   * @param blockId the id of an existing block
   * @param newLocation the location of the destination
   * @throws IllegalArgumentException if newLocation does not belong to the tiered storage
   * @throws BlockDoesNotExistException if block id can not be found
   * @throws BlockAlreadyExistsException if block id already exists in committed blocks of the
   *         newLocation
   * @throws InvalidWorkerStateException if block id has not been committed
   * @throws WorkerOutOfSpaceException if newLocation does not have enough extra space to hold the
   *         block
   * @throws IOException if block cannot be moved from current location to newLocation
   */
  void moveBlock(long sessionId, long blockId, BlockStoreLocation newLocation)
      throws BlockDoesNotExistException, BlockAlreadyExistsException, InvalidWorkerStateException,
      WorkerOutOfSpaceException, IOException;

  /**
   * Moves an existing block to a new location.
   *
   * @param sessionId the id of the session to remove a block
   * @param blockId the id of an existing block
   * @param oldLocation the location of the source
   * @param newLocation the location of the destination
   * @throws IllegalArgumentException if newLocation does not belong to the tiered storage
   * @throws BlockDoesNotExistException if block id can not be found
   * @throws BlockAlreadyExistsException if block id already exists in committed blocks of the
   *         newLocation
   * @throws InvalidWorkerStateException if block id has not been committed
   * @throws WorkerOutOfSpaceException if newLocation does not have enough extra space to hold the
   *         block
   * @throws IOException if block cannot be moved from current location to newLocation
   */
  void moveBlock(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) throws BlockDoesNotExistException,
      BlockAlreadyExistsException, InvalidWorkerStateException, WorkerOutOfSpaceException,
      IOException;

  /**
   * Removes an existing block. If the block can not be found in this store.
   *
   * @param sessionId the id of the session to remove a block
   * @param blockId the id of an existing block
   * @throws InvalidWorkerStateException if block id has not been committed
   * @throws BlockDoesNotExistException if block can not be found
   * @throws IOException if block cannot be removed from current path
   */
  void removeBlock(long sessionId, long blockId) throws InvalidWorkerStateException,
      BlockDoesNotExistException, IOException;

  /**
   * Removes an existing block. If the block can not be found in this store.
   *
   * @param sessionId the id of the session to move a block
   * @param blockId the id of an existing block
   * @param location the location of the block
   * @throws InvalidWorkerStateException if block id has not been committed
   * @throws BlockDoesNotExistException if block can not be found
   * @throws IOException if block cannot be removed from current path
   */
  void removeBlock(long sessionId, long blockId, BlockStoreLocation location)
      throws InvalidWorkerStateException, BlockDoesNotExistException, IOException;

  /**
   * Notifies the block store that a block was accessed so the block store could update accordingly
   * the registered listeners such as evictor and allocator on block access.
   *
   * @param sessionId the id of the session to access a block
   * @param blockId the id of an accessed block
   * @throws BlockDoesNotExistException if the block id is not found
   */
  void accessBlock(long sessionId, long blockId) throws BlockDoesNotExistException;

  /**
   * Gets the meta data of the entire store in a snapshot. There is no guarantee the state will be
   * consistent with the snapshot after this method is called.
   * This function should be cheap since it is called for every block.
   *
   * @return store meta data
   */
  BlockStoreMeta getBlockStoreMeta();

  /**
   * Similar as {@link BlockStoreMeta#getBlockStoreMeta} except that this includes more information
   * about the block store (e.g. blockId list). This is an expensive operation.
   *
   * @return full store meta data
   */
  BlockStoreMeta getBlockStoreMetaFull();

  /**
   * Checks if the storage has a given block.
   *
   * @param blockId the block id
   * @return true if the block is contained, false otherwise
   */
  boolean hasBlockMeta(long blockId);

  /**
   * Cleans up the data associated with a specific session (typically a dead session). Clean up
   * entails unlocking the block locks of this session, reclaiming space of temp blocks created by
   * this session, and deleting the session temporary folder.
   *
   * @param sessionId the session id
   */
  void cleanupSession(long sessionId);

  /**
   * Frees space to make a specific amount of bytes available in the location.
   *
   * @param sessionId the session id
   * @param availableBytes the amount of free space in bytes
   * @param location the location to free space
   * @throws WorkerOutOfSpaceException if there is not enough space
   * @throws BlockDoesNotExistException if blocks in {@link EvictionPlan} can not be found
   * @throws IOException if blocks in {@link EvictionPlan} fail to be moved or deleted on file
   *         system
   */
  void freeSpace(long sessionId, long availableBytes, BlockStoreLocation location)
      throws WorkerOutOfSpaceException, BlockDoesNotExistException, IOException;

  /**
   * Registers a {@link BlockStoreEventListener} to this block store.
   *
   * @param listener the listener to those events
   */
  void registerBlockStoreEventListener(BlockStoreEventListener listener);

  /**
   * Update the pinned inodes.
   *
   * @param inodes a set of inodes that are currently pinned
   */
  void updatePinnedInodes(Set<Long> inodes);
}
