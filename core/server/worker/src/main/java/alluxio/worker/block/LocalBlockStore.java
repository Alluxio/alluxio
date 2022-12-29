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

import alluxio.exception.runtime.BlockDoesNotExistRuntimeException;
import alluxio.proto.dataserver.Protocol;
import alluxio.worker.SessionCleanable;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;
import java.util.Set;

/**
 * A blob store interface to represent the local storage managing and serving all the blocks in the
 * local storage.
 */
public interface LocalBlockStore
    extends SessionCleanable, Closeable {
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
   * Creates the metadata of a new block and assigns a temporary path (e.g., a subdir of the final
   * location named after session id) to store its data. The location can be a location with
   * specific tier and dir, or {@link BlockStoreLocation#anyTier()}, or
   * {@link BlockStoreLocation#anyDirInTier(String)}.
   *
   * <p>
   * Before commit, all the data written to this block will be stored in the temp path and the block
   * is only "visible" to its writer client.
   *
   * @param sessionId the id of the session
   * @param blockId the id of the block to create
   * @param options allocation options
   * @return metadata of the temp block created
   */
  TempBlockMeta createBlock(long sessionId, long blockId, AllocateOptions options);

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
   * Gets the temp metadata of a specific block from local storage.
   *
   * @param blockId the id of the block
   * @return metadata of the block if the temp block exists
   */
  Optional<TempBlockMeta> getTempBlockMeta(long blockId);

  /**
   * Commits a temporary block to the local store. After commit, the block will be available in this
   * block store for all clients to access. Since a temp block is "private" to the writer, this
   * method requires no previously acquired lock.
   *
   * @param sessionId the id of the session
   * @param blockId the id of a temp block
   * @param pinOnCreate whether to pin block on create
   */
  void commitBlock(long sessionId, long blockId, boolean pinOnCreate);

  /**
   * Similar to {@link #commitBlock(long, long, boolean)}. It returns the block locked,
   * so the caller is required to explicitly unlock the block.
   * //TODO(Beinan): make this method to be private
   * @param sessionId the id of the session
   * @param blockId the id of a temp block
   * @param pinOnCreate whether to pin block on create
   * @return the lock
   */
  BlockLock commitBlockLocked(long sessionId, long blockId, boolean pinOnCreate);

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
   * Requests to increase the size of a temp block. Since a temp block is "private" to the writer
   * client, this operation requires no previously acquired lock.
   *
   * @param sessionId the id of the session to request space
   * @param blockId the id of the temp block
   * @param additionalBytes the amount of more space to request in bytes, never be less than 0
   */
  void requestSpace(long sessionId, long blockId, long additionalBytes);

  /**
   * Creates a writer to write data to a temp block. Since the temp block is "private" to the
   * writer, this operation requires no previously acquired lock.
   *
   * @param sessionId the id of the session to get the writer
   * @param blockId the id of the temp block
   * @return a {@link BlockWriter} instance on this block
   */
  BlockWriter createBlockWriter(long sessionId, long blockId);

  /**
   * Creates a reader of an existing block to read data from this block.
   * <p>
   * This operation requires the lock id returned by a previously acquired
   * {@link #pinBlock(long, long)}.
   *
   * @param sessionId the id of the session to get the reader
   * @param blockId the id of an existing block
   * @param offset the offset within the block
   * @return a {@link BlockReader} instance on this block
   * @throws BlockDoesNotExistRuntimeException if lockId is not found
   */
  BlockReader createBlockReader(long sessionId, long blockId, long offset)
      throws IOException;

  /**
   * Creates a reader of an existing block to read data from this block.
   * <p>
   * The block reader will fetch the data from UFS when the data is not cached by the worker
   *
   * @param sessionId the id of the session to get the reader
   * @param blockId the id of an existing block
   * @param options the options for UFS fall-back
   * @return a {@link BlockReader} instance on this block
   */
  default BlockReader createBlockReader(long sessionId, long blockId,
      Protocol.OpenUfsBlockOptions options) {
    throw new UnsupportedOperationException();
  }

  /**
   * Moves an existing block to a new location.
   *
   * @param sessionId the id of the session to move a block
   * @param blockId the id of an existing block
   * @param moveOptions the options for move
   * block
   */
  void moveBlock(long sessionId, long blockId, AllocateOptions moveOptions)
      throws IOException;

  /**
   * Removes an existing block. If the block can not be found in this store.
   *
   * @param sessionId the id of the session to remove a block
   * @param blockId the id of an existing block
   */
  void removeBlock(long sessionId, long blockId) throws IOException;

  /**
   * Notifies the block store that a block was accessed so the block store could update accordingly
   * the registered listeners such as evictor and allocator on block access.
   *
   * @param sessionId the id of the session to access a block
   * @param blockId the id of an accessed block
   */
  void accessBlock(long sessionId, long blockId);

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
   * Cleans up the data associated with a specific session (typically a dead session). Clean up
   * entails unlocking the block locks of this session, reclaiming space of temp blocks created by
   * this session, and deleting the session temporary folder.
   *
   * @param sessionId the session id
   */
  @Override
  void cleanupSession(long sessionId);

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

  /**
   * Remove Storage directories that are no longer accessible.
   */
  void removeInaccessibleStorage();
}
