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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Lists;

/**
 * Represents the delta of the block store within one heartbeat period. For now, newly committed
 * blocks do not pass through this master communication mechanism, instead it is synchronized
 * through {@link tachyon.worker.block.BlockDataManager#commitBlock(long, long)}.
 */
@ThreadSafe
public final class BlockHeartbeatReporter extends BlockStoreEventListenerBase {

  /** List of blocks that were removed in the last heartbeat period. */
  private final List<Long> mRemovedBlocks;

  /** Map of storage tier alias to a list of blocks that were added in the last heartbeat period. */
  private final Map<String, List<Long>> mAddedBlocks;

  /**
   * Creates a new instance of {@link BlockHeartbeatReporter}.
   */
  public BlockHeartbeatReporter() {
    mRemovedBlocks = new ArrayList<Long>(100);
    mAddedBlocks = new HashMap<String, List<Long>>(20);
  }

  /**
   * Generates the report of the block store delta in the last heartbeat period. Calling this method
   * marks the end of a period and the start of a new heartbeat period.
   *
   * @return the block store delta report for the last heartbeat period
   */
  public synchronized BlockHeartbeatReport generateReport() {
    // Copy added and removed blocks
    Map<String, List<Long>> addedBlocks = new HashMap<String, List<Long>>(mAddedBlocks);
    List<Long> removedBlocks = new ArrayList<Long>(mRemovedBlocks);
    // Clear added and removed blocks
    mAddedBlocks.clear();
    mRemovedBlocks.clear();
    return new BlockHeartbeatReport(addedBlocks, removedBlocks);
  }

  @Override
  public synchronized void onMoveBlockByClient(long sessionId, long blockId,
      BlockStoreLocation oldLocation, BlockStoreLocation newLocation) {
    // Remove the block from our list of added blocks in this heartbeat, if it was added, to
    // prevent adding the block twice.
    removeBlockFromAddedBlocks(blockId);
    // Add the block back with the new tier
    addBlockToAddedBlocks(blockId, newLocation.tierAlias());
  }

  @Override
  public synchronized void onRemoveBlockByClient(long sessionId, long blockId) {
    // Remove the block from list of added blocks, in case it was added in this heartbeat period.
    removeBlockFromAddedBlocks(blockId);
    // Add to the list of removed blocks in this heartbeat period.
    if (!mRemovedBlocks.contains(blockId)) {
      mRemovedBlocks.add(blockId);
    }
  }

  @Override
  public synchronized void onRemoveBlockByWorker(long sessionId, long blockId) {
    // Remove the block from list of added blocks, in case it was added in this heartbeat period.
    removeBlockFromAddedBlocks(blockId);
    // Add to the list of removed blocks in this heartbeat period.
    if (!mRemovedBlocks.contains(blockId)) {
      mRemovedBlocks.add(blockId);
    }
  }

  @Override
  public synchronized void onMoveBlockByWorker(long sessionId, long blockId,
      BlockStoreLocation oldLocation, BlockStoreLocation newLocation) {
    // Remove the block from our list of added blocks in this heartbeat, if it was added, to
    // prevent adding the block twice.
    removeBlockFromAddedBlocks(blockId);
    // Add the block back with the new storagedir.
    addBlockToAddedBlocks(blockId, newLocation.tierAlias());
  }

  /**
   * Adds a block to the list of added blocks in this heartbeat period.
   *
   * @param blockId the id of the block to add
   * @param tierAlias alias of the storage tier containing the block
   */
  private void addBlockToAddedBlocks(long blockId, String tierAlias) {
    if (mAddedBlocks.containsKey(tierAlias)) {
      mAddedBlocks.get(tierAlias).add(blockId);
    } else {
      mAddedBlocks.put(tierAlias, Lists.newArrayList(blockId));
    }
  }

  /**
   * Removes the block from the added blocks map, if it exists.
   *
   * @param blockId the block to remove
   */
  private void removeBlockFromAddedBlocks(long blockId) {
    Iterator<Entry<String, List<Long>>> iterator = mAddedBlocks.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, List<Long>> entry = iterator.next();
      List<Long> blockList = entry.getValue();
      if (blockList.contains(blockId)) {
        blockList.remove(blockId);
        if (blockList.isEmpty()) {
          iterator.remove();
        }
        // exit the loop when already find and remove block id from mAddedBlocks
        break;
      }
    }
  }
}
