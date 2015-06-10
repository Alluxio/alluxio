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

import tachyon.worker.BlockStoreLocation;
import tachyon.worker.block.io.BlockReader;
import tachyon.worker.block.io.BlockWriter;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * This interface represents a blob store that manages and serves all the blobs (i.e., blocks) in
 * the local storage.
 * <p>
 * TODO: rename this store to blob store.
 */
public interface BlockStore {

  /**
   * Locks a block for a specific mode (READ or WRITE) and guards the subsequent operations on this
   * block.
   *
   * @param userId the ID of the user to lock this block
   * @param blockId the ID of the block to lock
   * @param blockLockType the lock type
   * @return the lock ID if the lock is acquired successfully, {@link Optional#absent()} otherwise
   */
  Optional<Long> lockBlock(long userId, long blockId, BlockLock.BlockLockType blockLockType);

  /**
   * Release an acquired lock on a block previously by {@link #lockBlock}.
   *
   * @param lockId the ID of the lock returned by {@link #lockBlock}
   * @return true if the lock has been released, false otherwise
   */
  boolean unlockBlock(long lockId);

  /**
   * Creates the meta data of a new block and assigns a temporary path (e.g., a subdir of the final
   * location named after the the user ID) to store its data. This method only creates the meta data
   * but adds NO data to this temporary location. The location can be a specific location, or
   * {@link BlockStoreLocation#anyTier()} if any location in the store is fine.
   * <p>
   * Before commit, all the data written to this block will be stored in the temp path and the block
   * is only "visible" to its writer client.
   *
   * @param userId the ID of the user
   * @param blockId the ID of the block to create
   * @param location location to create this block
   * @param initialBlockSize initial size of this block in bytes
   * @return block meta if success, absent otherwise
   */
  Optional<TempBlockMeta> createBlockMeta(long userId, long blockId, BlockStoreLocation location,
      long initialBlockSize) throws IOException;

  /**
   * Gets the meta data of a specific block in local storage.
   * <p>
   * This method requires the lock ID returned by a proceeding {@link #lockBlock}.
   *
   * @param userId the ID of the user to get this file
   * @param blockId the ID of the block
   * @param lockId the ID of the lock
   * @return the block meta, or {@link Optional#absent()} if the block can not be found.
   */
  Optional<BlockMeta> getBlockMeta(long userId, long blockId, long lockId);

  /**
   * Commits a temporary block to the local store and returns the updated meta data. After commit,
   * the block will be available in this block store for all clients. Since a temp block is
   * "private" to the writer, this method requires no proceeding lock acquired.
   *
   * @param userId the ID of the user
   * @param blockId the ID of a temp block
   * @return true if success, false otherwise
   * @throws IOException
   */
  boolean commitBlock(long userId, long blockId) throws IOException;

  /**
   * Aborts a temporary block. The meta data of this block will not be added, its data will be
   * deleted and the space will be reclaimed. Since a temp block is "private" to the writer, this
   * requires no proceeding lock acquired.
   *
   * @param userId the ID of the user
   * @param blockId the ID of a temp block
   * @return true if success, false otherwise
   * @throws IOException
   */
  boolean abortBlock(long userId, long blockId) throws IOException;

  /**
   * Requests to increase the size of a temp block. Since a temp block is "private" to the writer,
   * this requires no proceeding lock acquired.
   *
   * @param userId the ID of the user to request space
   * @param blockId the ID of the temp block
   * @param size the amount of more space to request in bytes
   * @return true if success, false otherwise
   * @throws IOException
   */
  boolean requestSpace(long userId, long blockId, long size) throws IOException;

  /**
   * Creates a writer to write data to a temp block. Since the temp block is "private" to the
   * writer, this requires no proceeding lock acquired.
   *
   * @param userId the ID of the user to get the writer
   * @param blockId the ID of the temp block
   * @return a {@link BlockWriter} instance on this block if success, absent otherwise
   * @throws IOException
   */
  Optional<BlockWriter> getBlockWriter(long userId, long blockId) throws IOException;

  /**
   * Creates a reader of an existing block to read data from this block.
   * <p>
   * This method requires the lock ID returned by a proceeding {@link #lockBlock}.
   *
   * @param userId the ID of the user to get the reader
   * @param blockId the ID of an existing block
   * @param lockId the ID of the lock returned by {@link #lockBlock}
   * @return a {@link BlockReader} instance on this block if success, absent otherwise
   * @throws IOException
   */
  Optional<BlockReader> getBlockReader(long userId, long blockId, long lockId) throws IOException;

  /**
   * Copies an existing block to another location in the storage. If the block can not be found or
   * the new location doesn't have enough space, return false.
   * <p>
   * This method requires the lock ID returned by a proceeding {@link #lockBlock}.
   *
   * @param userId the ID of the user to copy a block
   * @param blockId the ID of an existing block
   * @param lockId the ID of the lock returned by {@link #lockBlock}
   * @param newLocation the location of the destination
   * @return true if success, false otherwise
   * @throws IOException
   */
  boolean copyBlock(long userId, long blockId, long lockId, BlockStoreLocation newLocation)
      throws IOException;

  /**
   * Moves an existing block to a new location. If the block can not be found, return false.
   * <p>
   * This method requires the lock ID returned by a proceeding {@link #lockBlock}.
   *
   * @param userId the ID of the user to remove a block
   * @param blockId the ID of an existing block
   * @param lockId the ID of the lock returned by {@link #lockBlock}
   * @return true if successful, false otherwise.
   * @throws IOException
   */
  boolean moveBlock(long userId, long blockId, long lockId) throws IOException;

  /**
   * Removes an existing block from a specific location. If the block can not be found, return
   * false.
   * <p>
   * This method requires the lock ID returned by a proceeding {@link #lockBlock}.
   *
   * @param userId the ID of the user to remove a block
   * @param blockId the ID of an existing block
   * @param lockId the ID of the lock returned by {@link #lockBlock}
   * @return true if successful, false otherwise.
   * @throws IOException
   */
  boolean removeBlock(long userId, long blockId, long lockId) throws IOException;

  /**
   * Notifies the block store that a block was accessed (so the block store could update accordingly
   * the evictor and allocator).
   *
   * @param userId the ID of the user to access a block
   * @param blockId the ID of an accessed block
   */
  void accessBlock(long userId, long blockId);

  /**
   * Gets the meta data of the entire store.
   *
   * @return store meta data
   */
  StoreMeta getStoreMeta();

  /**
   * Cleans up the data associated with a specific user (typically a dead user), e.g., unlock the
   * unreleased locks by this user, reclaim space of temp blocks created by this user.
   *
   * @param userId the user ID
   * @return true if success, false otherwise (e.g., cannot delete file)
   */
  boolean cleanupUser(long userId);

  /**
   * Frees a certain amount of space in the specified location according to the eviction policy.
   *
   * @param userId the user ID
   * @param size the amount of space to free in bytes
   * @param location the location to free space
   * @return true if success, false otherwise
   */
  boolean freeSpace(long userId, long size, BlockStoreLocation location);
}
