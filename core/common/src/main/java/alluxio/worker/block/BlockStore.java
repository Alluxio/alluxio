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

import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.TempBlockMeta;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

/**
 * An abstraction of block store on worker.
 */
public interface BlockStore {

  /**
   * Checks if the storage has a given temp block.
   *
   * @param blockId the temp block id
   * @return true if the block is contained, false otherwise
   */
  boolean hasTempBlockMeta(long blockId);

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
   * Gets the metadata of a block given its block id or empty if block does not exist.
   * This method does not require a lock id so the block is possible to be moved or removed after it
   * returns.
   *
   * @param blockId the block id
   * @return metadata of the block
   */
  Optional<? extends BlockMeta> getVolatileBlockMeta(long blockId);

  /**
   * Update the pinned inodes.
   *
   * @param inodes a set of inodes that are currently pinned
   */
  void updatePinnedInodes(Set<Long> inodes);

  /**
   * Moves an existing block to a new location.
   *
   * @param sessionId the id of the session to move a block
   * @param blockId the id of an existing block
   * @param medium the target medium
   * @throws WorkerOutOfSpaceException if newLocation does not have enough extra space to hold the
   *         block
   */
  void moveBlock(long sessionId, long blockId, String medium)
      throws IOException, WorkerOutOfSpaceException;
}
