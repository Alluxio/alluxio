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

/**
 * A listener interface for receiving meta data mutation events of {@link BlockStore}. All the
 * callback methods are triggered only after the actual event has been completed successfully.
 * <p>
 * All methods may be called concurrently, thus listener implementation needs to ensure
 * thread-safety.
 */
public interface BlockStoreEventListener {

  /**
   * Actions when accessing a block.
   *
   * @param userId the ID of the user to access this block
   * @param blockId the ID of the block to access
   */
  void onAccessBlock(long userId, long blockId);

  /**
   * Actions when aborting a temporary block.
   *
   * @param userId the ID of the user to abort on this block
   * @param blockId the ID of the block where the mutation to abort
   */
  void onAbortBlock(long userId, long blockId);

  /**
   * Actions when committing a temporary block to a {@link BlockStoreLocation}.
   *
   * @param userId the ID of the user to commit to this block
   * @param blockId the ID of the block to commit
   * @param location the location of the block to be committed
   */
  void onCommitBlock(long userId, long blockId, BlockStoreLocation location);

  /**
   * Actions when moving a block by a client from a {@link BlockStoreLocation} to another.
   *
   * @param userId the ID of the user to move this block
   * @param blockId the ID of the block to be moved
   * @param oldLocation the source location of the block to be moved
   * @param newLocation the destination location where the block is to be moved to
   */
  void onMoveBlockByClient(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation);

  /**
   * Actions when moving a block by a worker from a {@link BlockStoreLocation} to another.
   *
   * @param userId the ID of the user to move this block
   * @param blockId the ID of the block to be moved
   * @param oldLocation the source location of the block to be moved
   * @param newLocation the destination location where the block is to be moved to
   */
  void onMoveBlockByWorker(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation);

  /**
   * Actions when removing an existing block.
   *
   * @param userId the ID of the user to remove this block
   * @param blockId the ID of the block to be removed
   */
  void onRemoveBlockByClient(long userId, long blockId);

  /**
   * Actions when removing an existing block by worker.
   *
   * @param userId the ID of the user to remove this block
   * @param blockId the ID of the block to be removed
   */
  void onRemoveBlockByWorker(long userId, long blockId);

}
