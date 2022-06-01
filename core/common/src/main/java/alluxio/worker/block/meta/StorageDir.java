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

package alluxio.worker.block.meta;

import alluxio.exception.InvalidWorkerStateException;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.worker.block.BlockStoreLocation;

import java.util.List;
import java.util.Optional;

/**
 * Represents a directory in a storage tier. It has a fixed capacity allocated to it on
 * instantiation. It contains the set of blocks currently in the storage directory.
 */
public interface StorageDir {
  /**
   * Gets the total capacity of this {@link StorageDir} in bytes, which is a constant once this
   * {@link StorageDir} has been initialized.
   *
   * @return the total capacity of this {@link StorageDir} in bytes
   */
  long getCapacityBytes();

  /**
   * Gets the total available capacity of this {@link StorageDir} in bytes. This value equals the
   * total capacity of this {@link StorageDir}, minus the used bytes by committed blocks and temp
   * blocks.
   *
   * @return available capacity in bytes
   */
  long getAvailableBytes();

  /**
   * Gets the total size of committed blocks in this StorageDir in bytes.
   *
   * @return number of committed bytes
   */
  long getCommittedBytes();

  /**
   * @return the path of the directory
   */
  String getDirPath();

  /**
   * @return the medium of the storage dir
   */
  String getDirMedium();

  /**
   * Returns the {@link StorageTier} containing this {@link StorageDir}.
   *
   * @return {@link StorageTier}
   */
  StorageTier getParentTier();

  /**
   * Returns the zero-based index of this dir in its parent {@link StorageTier}.
   *
   * @return index
   */
  int getDirIndex();

  /**
   * Returns the list of block ids in this dir.
   *
   * @return a list of block ids
   */
  List<Long> getBlockIds();

  /**
   * Returns the list of blocks stored in this dir.
   *
   * @return a list of blocks
   */
  List<BlockMeta> getBlocks();

  /**
   * Checks if a block is in this storage dir.
   *
   * @param blockId the block id
   * @return true if the block is in this storage dir, false otherwise
   */
  boolean hasBlockMeta(long blockId);

  /**
   * Checks if a temp block is in this storage dir.
   *
   * @param blockId the block id
   * @return true if the block is in this storage dir, false otherwise
   */
  boolean hasTempBlockMeta(long blockId);

  /**
   * Gets the {@link BlockMeta} from this storage dir by its block id.
   *
   * @param blockId the block id
   * @return {@link BlockMeta} of the given block or empty
   */
  Optional<BlockMeta> getBlockMeta(long blockId);

  /**
   * Gets the {@link TempBlockMeta} from this storage dir by its block id.
   *
   * @param blockId the block id
   * @return {@link TempBlockMeta} of the given block or empty
   */
  Optional<TempBlockMeta> getTempBlockMeta(long blockId);

  /**
   * Adds the metadata of a new block into this storage dir.
   *
   * @param blockMeta the metadata of the block
   * @throws WorkerOutOfSpaceException when not enough space to hold block
   */
  void addBlockMeta(BlockMeta blockMeta) throws WorkerOutOfSpaceException;

  /**
   * Adds the metadata of a new block into this storage dir.
   *
   * @param tempBlockMeta the metadata of a temp block to add
   */
  void addTempBlockMeta(TempBlockMeta tempBlockMeta);

  /**
   * Removes a block from this storage dir.
   *
   * @param blockMeta the metadata of the block
   */
  void removeBlockMeta(BlockMeta blockMeta);

  /**
   * Removes a temp block from this storage dir.
   *
   * @param tempBlockMeta the metadata of the temp block to remove
   */
  void removeTempBlockMeta(TempBlockMeta tempBlockMeta);

  /**
   * Changes the size of a temp block.
   *
   * @param tempBlockMeta the metadata of the temp block to resize
   * @param newSize the new size after change in bytes
   * @throws InvalidWorkerStateException when newSize is smaller than oldSize
   */
  void resizeTempBlockMeta(TempBlockMeta tempBlockMeta, long newSize)
      throws InvalidWorkerStateException;

  /**
   * Cleans up the temp block metadata for each block id passed in.
   *
   * @param sessionId the id of the client associated with the temporary blocks
   * @param tempBlockIds the list of temporary blocks to clean up, non temporary blocks or
   *        nonexistent blocks will be ignored
   */
  void cleanupSessionTempBlocks(long sessionId, List<Long> tempBlockIds);

  /**
   * Gets the temporary blocks associated with a session in this {@link StorageDir}, an empty list
   * is returned if the session has no temporary blocks in this {@link StorageDir}.
   *
   * @param sessionId the id of the session
   * @return A list of temporary blocks the session is associated with in this {@link StorageDir}
   */
  List<TempBlockMeta> getSessionTempBlocks(long sessionId);

  /**
   * @return the block store location of this directory
   */
  BlockStoreLocation toBlockStoreLocation();

  /**
   * @return amount of reserved bytes for this dir
   */
  long getReservedBytes();
}
