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

package alluxio.master.metastore;

import alluxio.exception.status.AlreadyExistsException;
import alluxio.exception.status.NotFoundException;
import alluxio.proto.master.Block.BlockLocation;
import alluxio.proto.master.Block.BlockMeta;
import alluxio.resource.LockResource;

import java.util.Iterator;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Store for holding block metadata.
 *
 * Implementations must be threadsafe.
 */
@ThreadSafe
public interface BlockStore extends Store {
  /// Read methods

  /**
   * @param id a block id
   * @return whether the block store contains the id
   */
  boolean contains(long id);

  /**
   * @param id a block id
   * @return the metadata for the block id
   * @throws NotFoundException if the block does not exist
   */
  BlockMeta getMeta(long id) throws NotFoundException;

  /**
   * @param id a block id
   * @return the metadata for the block id, or null if the block does not exist
   */
  @Nullable
  BlockMeta getMetaOrNull(long id);

  /**
   * @param id a block id
   * @return the locations of the block
   * @throws NotFoundException if the block does not exist
   */
  Set<BlockLocation> getLocations(long id) throws NotFoundException;

  /**
   * @return a weakly consistent iterator over the block ids in the block store
   */
  Iterator<Long> blockIds();

  /// Write methods

  /**
   * Creates a new block with the given metadata.
   *
   * @param id a block id
   * @param blockMeta block metadata
   * @throws AlreadyExistsException if a block with the given block id already exists
   */
  void newBlock(long id, BlockMeta blockMeta) throws AlreadyExistsException;

  /**
   * Removes a block's metadata.
   *
   * @param id a block id
   * @throws NotFoundException if the block does not exist
   */
  void remove(long id) throws NotFoundException;

  /**
   * Sets the length for a block.
   *
   * @param id a block id
   * @param length the length to set
   * @throws NotFoundException if the block does not exist
   */
  void setBlockLength(long id, long length) throws NotFoundException;

  /**
   * @param id the block id to set a location for
   * @param blockLocation the block location to set
   * @throws NotFoundException if no block with the specified id exists
   */
  void setLocation(long id, BlockLocation blockLocation) throws NotFoundException;

  /**
   * @param blockId the block to remove a location for
   * @param workerId the worker id to remove
   * @throws NotFoundException if the block is not found; no exception is thrown if the location is
   *         not found
   */
  void removeLocation(long blockId, long workerId) throws NotFoundException;

  /**
   * Locks the specified block if it exists. The lock guarantees that no other thread will read or
   * modify the block's metadata. The lock is re-entrant, allowing users to call other BlockStore
   * methods while holding it.
   *
   * Avoid acquiring multiple block locks at once - doing so will likely lead to deadlock.
   *
   * @param id the id of the block to lock
   * @return null if the block doesn't exist, otherwise return a closeable which will unlock the
   *         block
   */
  LockResource lockBlock(long id) throws NotFoundException;

  /**
   * Similar to {@link #lockBlock(long)}, but returns null instead of throwing an exception when the
   * block does not exist. This is useful when the block not existing is a common case.
   *
   * @param id the id of the block to lock
   * @return null if the block doesn't exist, otherwise return a closeable which will unlock the
   *         block
   */
  @Nullable
  LockResource lockBlockOrNull(long id);

  /**
   * Resets the block store, clearing all blocks.
   */
  void reset();
}
