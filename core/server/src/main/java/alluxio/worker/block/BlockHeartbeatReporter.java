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

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents the delta of the block store within one heartbeat period. For now, newly committed
 * blocks do not pass through this master communication mechanism, instead it is synchronized
 * through {@link alluxio.worker.block.BlockWorker#commitBlock(long, long)}.
 */
@ThreadSafe
public final class BlockHeartbeatReporter extends AbstractBlockStoreEventListener {
  /** Lock for operations on the removed and added block collections. */
  private final Object mLock;

  /** List of blocks that were removed in the last heartbeat period. */
  private final List<Long> mRemovedBlocks;

  /** Map of storage tier alias to a list of blocks that were added in the last heartbeat period. */
  private final Map<String, List<Long>> mAddedBlocks;

  /**
   * Creates a new instance of {@link BlockHeartbeatReporter}.
   */
  public BlockHeartbeatReporter() {
    mLock = new Object();
    mRemovedBlocks = new ArrayList<>(100);
    mAddedBlocks = new HashMap<>(20);
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
      Map<String, List<Long>> addedBlocks = new HashMap<>(mAddedBlocks);
      List<Long> removedBlocks = new ArrayList<>(mRemovedBlocks);
      // Clear added and removed blocks
      mAddedBlocks.clear();
      mRemovedBlocks.clear();
      return new BlockHeartbeatReport(addedBlocks, removedBlocks);
    }
  }

  @Override
  public void onMoveBlockByClient(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    synchronized (mLock) {
      // Remove the block from our list of added blocks in this heartbeat, if it was added, to
      // prevent adding the block twice.
      removeBlockFromAddedBlocks(blockId);
      // Add the block back with the new tier
      addBlockToAddedBlocks(blockId, newLocation.tierAlias());
    }
  }

  @Override
  public void onRemoveBlockByClient(long sessionId, long blockId) {
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
  public void onRemoveBlockByWorker(long sessionId, long blockId) {
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
  public void onMoveBlockByWorker(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    synchronized (mLock) {
      // Remove the block from our list of added blocks in this heartbeat, if it was added, to
      // prevent adding the block twice.
      removeBlockFromAddedBlocks(blockId);
      // Add the block back with the new storagedir.
      addBlockToAddedBlocks(blockId, newLocation.tierAlias());
    }
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
