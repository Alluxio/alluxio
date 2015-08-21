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

import com.google.common.collect.Lists;

/**
 * Represents the delta of the block store within one heartbeat period. For now, newly committed
 * blocks do not pass through this master communication mechanism, instead it is synchronized
 * through {@link tachyon.worker.block.BlockDataManager#commitBlock(long, long)}. This class is
 * thread safe.
 */
public final class BlockHeartbeatReporter extends BlockStoreEventListenerBase {
  /** Lock for operations on the removed and added block collections */
  private final Object mLock;

  /** List of blocks that were removed in the last heartbeat period */
  private final List<Long> mRemovedBlocks;
  /** Map of storage dirs to a list of blocks that were added in the last heartbeat period */
  private final Map<Long, List<Long>> mAddedBlocks;

  public BlockHeartbeatReporter() {
    mLock = new Object();
    mRemovedBlocks = new ArrayList<Long>(100);
    mAddedBlocks = new HashMap<Long, List<Long>>(20);
  }

  /**
   * Generates the report of the block store delta in the last heartbeat period. Calling this method
   * marks the end of a period and the start of a new heartbeat period.
   *
   * @return the block store delta report for the last heartbeat period
   */
  public BlockHeartbeatReport generateReport() {
    synchronized (mLock) {
      // Copy added and removed blocks
      Map<Long, List<Long>> addedBlocks = new HashMap<Long, List<Long>>(mAddedBlocks);
      List<Long> removedBlocks = new ArrayList<Long>(mRemovedBlocks);
      // Clear added and removed blocks
      mAddedBlocks.clear();
      mRemovedBlocks.clear();
      return new BlockHeartbeatReport(addedBlocks, removedBlocks);
    }
  }

  @Override
  public void onMoveBlockByClient(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    Long storageDirId = newLocation.getStorageDirId();
    synchronized (mLock) {
      // Remove the block from our list of added blocks in this heartbeat, if it was added, to
      // prevent adding the block twice.
      removeBlockFromAddedBlocks(blockId);
      // Add the block back with the new storagedir.
      addBlockToAddedBlocks(blockId, storageDirId);
    }
  }

  @Override
  public void onRemoveBlockByClient(long userId, long blockId) {
    synchronized (mLock) {
      // Remove the block from list of added blocks, in case it was added in this heartbeat period.
      removeBlockFromAddedBlocks(blockId);
      // Add to the list of removed blocks in this heartbeat period.
      if (!mRemovedBlocks.contains(blockId)) {
        mRemovedBlocks.add(blockId);
      }
    }
  }

  @Override
  public void onRemoveBlockByWorker(long userId, long blockId) {
    synchronized (mLock) {
      // Remove the block from list of added blocks, in case it was added in this heartbeat period.
      removeBlockFromAddedBlocks(blockId);
      // Add to the list of removed blocks in this heartbeat period.
      if (!mRemovedBlocks.contains(blockId)) {
        mRemovedBlocks.add(blockId);
      }
    }
  }

  @Override
  public void onMoveBlockByWorker(long userId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    Long storageDirId = newLocation.getStorageDirId();
    synchronized (mLock) {
      // Remove the block from our list of added blocks in this heartbeat, if it was added, to
      // prevent adding the block twice.
      removeBlockFromAddedBlocks(blockId);
      // Add the block back with the new storagedir.
      addBlockToAddedBlocks(blockId, storageDirId);
    }
  }

  /**
   * Adds a block to the list of added blocks in this heartbeat period.
   *
   * @param blockId The id of the block to add
   * @param storageDirId The storage directory id containing the block
   */
  private void addBlockToAddedBlocks(long blockId, long storageDirId) {
    if (mAddedBlocks.containsKey(storageDirId)) {
      mAddedBlocks.get(storageDirId).add(blockId);
    } else {
      mAddedBlocks.put(storageDirId, Lists.newArrayList(blockId));
    }
  }

  /**
   * Removes the block from the added blocks map, if it exists.
   *
   * @param blockId The block to remove
   */
  private void removeBlockFromAddedBlocks(long blockId) {
    Iterator<Entry<Long, List<Long>>> iterator = mAddedBlocks.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<Long, List<Long>> entry = iterator.next();
      List<Long> blockList = entry.getValue();
      if (blockList.contains(blockId)) {
        blockList.remove(blockId);
        if (blockList.isEmpty()) {
          iterator.remove();
        }
        // exit the loop when already find and remove blockId from mAddedBlocks
        break;
      }
    }
  }
}
