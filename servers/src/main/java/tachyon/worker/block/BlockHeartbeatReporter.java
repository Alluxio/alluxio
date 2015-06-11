package tachyon.worker.block;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import tachyon.worker.BlockStoreLocation;

/**
 * Represents the data the BlockWorker will send to the master in its periodic heartbeat. This
 * class is thread safe.
 */
public class BlockHeartbeatReporter implements BlockMetaEventListener {
  private final Object mLock;

  private List<Long> mRemovedBlocks;
  private Map<Long, List<Long>> mAddedBlocks;

  public BlockHeartbeatReporter() {
    mLock = new Object();
    mRemovedBlocks = new ArrayList<Long>(100);
    mAddedBlocks = new HashMap<Long, List<Long>>();
  }

  @Override
  public void preCommitBlock(long userId, long blockId, BlockStoreLocation location) {
    // Do nothing
  }

  @Override
  public void postCommitBlock(long userId, long blockId, BlockStoreLocation location) {
    Long storageDirId = location.getStorageDirId();
    synchronized (mLock) {
      addBlockToAddedBlocks(blockId, storageDirId);
    }
  }

  @Override
  public void preMoveBlock(long userId, long blockId, BlockStoreLocation newLocation) {
    // Do nothing
  }

  @Override
  public void postMoveBlock(long userId, long blockId, BlockStoreLocation newLocation) {
    Long storageDirId = newLocation.getStorageDirId();
    synchronized (mLock) {
      // Remove the block from our list of added blocks in this heartbeat, if it was added to
      // prevent adding the block twice.
      removeBlockFromAddedBlocks(blockId);
      // Add the block back with the new storagedir.
      addBlockToAddedBlocks(blockId, storageDirId);
    }
  }

  @Override
  public void preRemoveBlock(long userId, long blockId) {
    // Do nothing
  }

  @Override
  public void postRemoveBlock(long userId, long blockId) {
    synchronized (mLock) {
      // Remove the block from list of added blocks, in case it was added in this heartbeat period.
      removeBlockFromAddedBlocks(blockId);
      // Add to the list of removed blocks in this heartbeat period.
      mRemovedBlocks.add(blockId);
    }
  }

  /**
   * Adds a block to the list of added blocks in this heartbeat period.
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
   * @param blockId The block to remove
   */
  private void removeBlockFromAddedBlocks(long blockId) {
    for (List<Long> blockList : mAddedBlocks.values()) {
      if (blockList.contains(blockId)) {
        blockList.remove(blockId);
      }
    }
  }
}
