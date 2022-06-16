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

/**
 * A listener interface for receiving metadata mutation events of {@link BlockStore}. All the
 * callback methods are triggered only after the actual event has been completed successfully.
 * <p>
 * All methods may be called concurrently, thus listener implementation needs to ensure
 * thread-safety.
 */
public interface BlockStoreEventListener {

  /**
   * Actions when accessing a block.
   *
   * @param blockId the id of the block to access
   */
  void onAccessBlock(long blockId);

  /**
   * Actions when accessing a block.
   * @param blockId the id of the block to access
   * @param location the location of the block
   */
  void onAccessBlock(long blockId, BlockStoreLocation location);

  /**
   * Actions when aborting a temporary block.
   *
   * @param blockId the id of the block where the mutation to abort
   */
  void onAbortBlock(long blockId);

  /**
   * Actions when committing a temporary block to a {@link BlockStoreLocation}.
   * @param blockId the id of the block to commit
   * @param location the location of the block to be committed
   */
  void onCommitBlock(long blockId, BlockStoreLocation location);

  /**
   * Actions when moving a block by a client from a {@link BlockStoreLocation} to another.
   * @param blockId the id of the block to be moved
   * @param oldLocation the source location of the block to be moved
   * @param newLocation the destination location where the block is to be moved to
   */
  void onMoveBlockByClient(long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation);

  /**
   * Actions when moving a block by a worker from a {@link BlockStoreLocation} to another.
   * @param blockId the id of the block to be moved
   * @param oldLocation the source location of the block to be moved
   * @param newLocation the destination location where the block is to be moved to
   */
  void onMoveBlockByWorker(long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation);

  /**
   * Actions when removing an existing block.
   *
   * @param blockId the id of the block to be removed
   */
  void onRemoveBlockByClient(long blockId);

  /**
   * Actions when removing an existing block by worker.
   *
   * @param blockId the id of the block to be removed
   */
  void onRemoveBlockByWorker(long blockId);

  /**
   * Actions when removing an existing block.
   * @param blockId the id of the block to be removed
   * @param location the location of the block to be removed
   */
  void onRemoveBlock(long blockId, BlockStoreLocation location);

  /**
   * Actions when a block is lost.
   *
   * @param blockId the id of the lost block
   */
  void onBlockLost(long blockId);

  /**
   * Actions when a storage dir is lost.
   *
   * @param tierAlias the tier alias of this storage
   * @param dirPath the directory path of this storage
   */
  void onStorageLost(String tierAlias, String dirPath);

  /**
   * Actions when a storage dir is lost.
   *
   * @param dirLocation the location of this storage
   */
  void onStorageLost(BlockStoreLocation dirLocation);
}
