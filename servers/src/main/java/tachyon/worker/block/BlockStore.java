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

import com.google.common.base.Optional;

import tachyon.worker.block.io.BlockReader;
import tachyon.worker.block.io.BlockWriter;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * A blob store interface to represent the local storage managing and serving all the blocks in the
 * local storage.
 * <p>
 * TODO: define customized Exceptions to better indicate the failure reason, rather than always
 * throwing IOException
 */
interface BlockStore {

  /**
   * Locks an existing block and guards subsequent READs on this block. In case the block doesn't
   * exist (e.g., evicted already), throw IOException.
   *
   * @param userId the ID of the user to lock this block
   * @param blockId the ID of the block to lock
   * @return the lock ID if the lock is acquired successfully
   * @throws IOException if blockId is invalid
   */
  long lockBlock(long userId, long blockId) throws IOException;

  /**
   * Releases an acquired block lock based on a lockId (returned by {@link #lockBlock}).
   *
   * @param lockId the ID of the lock returned by {@link #lockBlock}
   * @throws IOException if lockId is invalid
   */
  void unlockBlock(long lockId) throws IOException;

  /**
   * NOTE: temporary, will be removed after changing client side code.
   */
  void unlockBlock(long userId, long blockId) throws IOException;

  /**
   * Creates the meta data of a new block and assigns a temporary path (e.g., a subdir of the final
   * location named after the the user ID) to store its data. This method only creates the meta data
   * but adds NO data to this temporary location. The location can be a location with specific tier
   * and dir, or {@link BlockStoreLocation#anyTier()}, or
   * {@link BlockStoreLocation#anyDirInTier(int)}.
   * <p>
   * Before commit, all the data written to this block will be stored in the temp path and the block
   * is only "visible" to its writer client.
   *
   * @param userId the ID of the user
   * @param blockId the ID of the block to create
   * @param location location to create this block
   * @param initialBlockSize initial size of this block in bytes
   * @return metadata of the temp block created
   * @throws IOException if blockId or location is invalid, or this Store has no space
   */
  TempBlockMeta createBlockMeta(long userId, long blockId, BlockStoreLocation location,
      long initialBlockSize) throws IOException;

  /**
   * Gets the meta data of a specific block from local storage.
   * <p>
   * This method requires the lock ID returned by a previously acquired {@link #lockBlock}.
   *
   * @param userId the ID of the user to get this file
   * @param blockId the ID of the block
   * @param lockId the ID of the lock
   * @return metadata of the block
   * @throws IOException if the block can not be found
   */
  BlockMeta getBlockMeta(long userId, long blockId, long lockId) throws IOException;

  /**
   * Commits a temporary block to the local store. After commit, the block will be available in this
   * block store for all clients to access. Since a temp block is "private" to the writer, this
   * method requires no previously acquired lock.
   *
   * @param userId the ID of the user
   * @param blockId the ID of a temp block
   * @throws IOException if the block can not be found or moved
   */
  void commitBlock(long userId, long blockId) throws IOException;

  /**
   * Aborts a temporary block. The meta data of this block will not be added, its data will be
   * deleted and the space will be reclaimed. Since a temp block is "private" to the writer, this
   * requires no previously acquired lock.
   *
   * @param userId the ID of the user
   * @param blockId the ID of a temp block
   * @throws IOException if the block is invalid or committed or can not be removed
   */
  void abortBlock(long userId, long blockId) throws IOException;

  /**
   * Requests to increase the size of a temp block. Since a temp block is "private" to the writer
   * client, this operation requires no previously acquired lock.
   *
   * @param userId the ID of the user to request space
   * @param blockId the ID of the temp block
   * @param additionalBytes the amount of more space to request in bytes
   * @throws IOException if there is not enough space or other blocks fail to be moved/evicted
   */
  void requestSpace(long userId, long blockId, long additionalBytes) throws IOException;

  /**
   * Creates a writer to write data to a temp block. Since the temp block is "private" to the
   * writer, this operation requires no previously acquired lock.
   *
   * @param userId the ID of the user to get the writer
   * @param blockId the ID of the temp block
   * @return a {@link BlockWriter} instance on this block
   * @throws IOException if the blockId is invalid, or block can not be created
   */
  BlockWriter getBlockWriter(long userId, long blockId) throws IOException;

  /**
   * Creates a reader of an existing block to read data from this block.
   * <p>
   * This operation requires the lock ID returned by a previously acquired {@link #lockBlock}.
   *
   * @param userId the ID of the user to get the reader
   * @param blockId the ID of an existing block
   * @param lockId the ID of the lock returned by {@link #lockBlock}
   * @return a {@link BlockReader} instance on this block
   * @throws IOException if the blockId or lockId is invalid, or block can not be read
   */
  BlockReader getBlockReader(long userId, long blockId, long lockId) throws IOException;

  /**
   * Moves an existing block to a new location. If the block can not be found, throw IOException.
   *
   * @param userId the ID of the user to move a block
   * @param blockId the ID of an existing block
   * @param newLocation the location of the destination
   * @throws IOException if blockId or newLocation is invalid, or block cannot be moved
   */
  void moveBlock(long userId, long blockId, BlockStoreLocation newLocation) throws IOException;

  /**
   * Removes an existing block. If the block can not be found in this store, throw IOException.
   *
   * @param userId the ID of the user to remove a block
   * @param blockId the ID of an existing block
   * @throws IOException if blockId is invalid, or block cannot be removed
   */
  void removeBlock(long userId, long blockId) throws IOException;

  /**
   * Notifies the block store that a block was accessed so the block store could update accordingly
   * the registered listeners such as evictor and allocator on block access.
   *
   * @param userId the ID of the user to access a block
   * @param blockId the ID of an accessed block
   */
  void accessBlock(long userId, long blockId) throws IOException;

  /**
   * Gets the meta data of the entire store.
   *
   * @return store meta data
   */
  BlockStoreMeta getBlockStoreMeta();

  /**
   * Cleans up the data associated with a specific user (typically a dead user), e.g., unlock the
   * unreleased locks by this user, reclaim space of temp blocks created by this user.
   *
   * @param userId the user ID
   * @throws IOException if block can not be deleted or locks can not be released
   */
  void cleanupUser(long userId) throws IOException;

  /**
   * Frees space to make a specific amount of bytes available in the location.
   *
   * @param userId the user ID
   * @param availableBytes the amount of free space in bytes
   * @param location the location to free space
   * @throws IOException if location is invalid, or there is not enough space, or eviction failed
   */
  void freeSpace(long userId, long availableBytes, BlockStoreLocation location) throws IOException;

  /**
   * Registers a {@link BlockStoreEventListener} to this block store.
   *
   * @param listener the listener to those events
   */
  void registerBlockStoreEventListener(BlockStoreEventListener listener);

}
