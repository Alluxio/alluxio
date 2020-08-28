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

  /**
   * Map of block store locations to a list of blocks that were added in the last
   * heartbeat period.
   */
  private final Map<BlockStoreLocation, List<Long>> mAddedBlocks;

  /**
   * Map of storage tier alias to a list of storage paths
   * that were lost in the last hearbeat period.
   */
  private final Map<String, List<String>> mLostStorage;

  /**
   * Creates a new instance of {@link BlockHeartbeatReporter}.
   */
  public BlockHeartbeatReporter() {
    mLock = new Object();
    mRemovedBlocks = new ArrayList<>(100);
    mAddedBlocks = new HashMap<>(20);
    mLostStorage = new HashMap<>();
  }

  /**
   * Generates the report of the block store delta in the last heartbeat period. Calling this method
   * marks the end of a period and the start of a new heartbeat period.
   *
   * @return the block store delta report for the last heartbeat period
   */
  public BlockHeartbeatReport generateReport() {
    synchronized (mLock) {
      BlockHeartbeatReport report
          = new BlockHeartbeatReport(mAddedBlocks, mRemovedBlocks, mLostStorage);
      // Clear added and removed blocks
      mAddedBlocks.clear();
      mRemovedBlocks.clear();
      mLostStorage.clear();
      return report;
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
      addBlockToAddedBlocks(blockId, newLocation);
    }
  }

  @Override
  public void onRemoveBlockByClient(long sessionId, long blockId) {
    synchronized (mLock) {
      removeBlockInternal(blockId);
    }
  }

  @Override
  public void onRemoveBlockByWorker(long sessionId, long blockId) {
    synchronized (mLock) {
      removeBlockInternal(blockId);
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
      addBlockToAddedBlocks(blockId, newLocation);
    }
  }

  @Override
  public void onBlockLost(long blockId) {
    synchronized (mLock) {
      removeBlockInternal(blockId);
    }
  }

  @Override
  public void onStorageLost(String tierAlias, String dirPath) {
    synchronized (mLock) {
      List<String> storagesList = mLostStorage.getOrDefault(tierAlias, new ArrayList<>());
      storagesList.add(dirPath);
      mLostStorage.put(tierAlias, storagesList);
    }
  }

  private void removeBlockInternal(long blockId) {
    // Remove the block from list of added blocks, in case it was added in this heartbeat period.
    removeBlockFromAddedBlocks(blockId);
    // Add to the list of removed blocks in this heartbeat period.
    if (!mRemovedBlocks.contains(blockId)) {
      mRemovedBlocks.add(blockId);
    }
  }

  /**
   * Adds a block to the list of added blocks in this heartbeat period.
   *
   * @param blockId the id of the block to add
   * @param location BlockStoreLocation containing the blockid
   */
  private void addBlockToAddedBlocks(long blockId, BlockStoreLocation location) {
    if (mAddedBlocks.containsKey(location)) {
      mAddedBlocks.get(location).add(blockId);
    } else {
      mAddedBlocks.put(location, Lists.newArrayList(blockId));
    }
  }

  /**
   * Removes the block from the added blocks map, if it exists.
   *
   * @param blockId the block to remove
   */
  private void removeBlockFromAddedBlocks(long blockId) {
    Iterator<Entry<BlockStoreLocation, List<Long>>> iterator = mAddedBlocks.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<BlockStoreLocation, List<Long>> entry = iterator.next();
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
