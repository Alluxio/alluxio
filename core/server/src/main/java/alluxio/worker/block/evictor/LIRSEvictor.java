/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.evictor;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.collections.Pair;
import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDirView;
import alluxio.worker.block.meta.StorageTierView;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is used to evict blocks with LIRS policy on each tier. Different from LRU and LRFU,
 * LIRS evicts the block with maximum Inter-Reference Recency (IRR). IRR is the number of distinct
 * blocks between last access and second-to-last access to the block. Block with large IRR is
 * predicted to be accessed again in a long time, so they needs to be evicted first to make room for
 * other blocks which will be accessed soon. LIRS divides the tier into 2 parts: LIR cache and HIR
 * cache. LIR cache holds blocks with small IRR and HIR cache holds blocks with large IRR. So blocks
 * in HIR cache needs to be evicted first. If blocks in HIR cache gets a small IRR, they will be
 * moved to LIR cache. And if the LIR cache is full, some blocks with relatively large IRR will be
 * moved to HIR cache. LIRS can achieve better performance than LRU and LRFU in loop access pattern
 * (like a1,a2,...,ak,a1,a2,...,ak,...). Besides, LIRS can achieve as good performance as LRU and
 * LRFU in most of workloads. If your workloads contain a lot of iterations, LIRS policy is
 * recommended for you.
 */
public final class LIRSEvictor extends AbstractEvictor {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final int LINKED_HASH_MAP_INIT_CAPACITY = 200;
  private static final float LINKED_HASH_MAP_INIT_LOAD_FACTOR = 0.75f;
  private static final boolean LINKED_HASH_MAP_ACCESS_ORDERED = false;
  private static final boolean UNUSED_MAP_VALUE = true;

  // The percent of HIR blocks on each tier
  private final double mHIRPercent;
  // The percent of LIR blocks on each tier
  private final double mLIRPercent;
  private final Configuration mConfiguration;
  // Map from the location of a StorageDir to its condition of used space
  private Map<BlockStoreLocation, SpaceContainer> mSpaceManager =
      new ConcurrentHashMap<BlockStoreLocation, SpaceContainer>();
  /**
   * Map from the the block id to the block size. Maintain the information of block size is because
   * the space of HIR block or LIR block need to be reclaimed when
   * {@link #onRemoveBlockByClient(long, long)} or {@link #onRemoveBlockByWorker(long, long)} is
   * called.
   */
  private Map<Long, Long> mBlockIdToSize = new ConcurrentHashMap<Long, Long>();
  /**
   * Record the information of all LIR blocks. Pair<BlockStoreLocation, Long> identifies a block
   * because a block may contain more than one ghost blocks on all tiers.
   */
  private Map<Pair<BlockStoreLocation, Long>, BlockLIRSInfo> mLIRCache =
      Collections.synchronizedMap(new LinkedHashMap<Pair<BlockStoreLocation, Long>, BlockLIRSInfo>(
          LINKED_HASH_MAP_INIT_CAPACITY, LINKED_HASH_MAP_INIT_LOAD_FACTOR,
          LINKED_HASH_MAP_ACCESS_ORDERED));
  // Record the information of all HIR blocks.
  private Map<Pair<BlockStoreLocation, Long>, BlockLIRSInfo> mHIRCache =
      Collections.synchronizedMap(new LinkedHashMap<Pair<BlockStoreLocation, Long>, BlockLIRSInfo>(
          LINKED_HASH_MAP_INIT_CAPACITY, LINKED_HASH_MAP_INIT_LOAD_FACTOR,
          LINKED_HASH_MAP_ACCESS_ORDERED));
  /**
   * Record the information of all temp blocks. Temp blocks are blocks moved from other tiers and
   * still not accessed by any clients.
   */
  private Map<Pair<BlockStoreLocation, Long>, Boolean> mTmpCache = Collections.synchronizedMap(
      new LinkedHashMap<Pair<BlockStoreLocation, Long>, Boolean>(LINKED_HASH_MAP_INIT_CAPACITY,
          LINKED_HASH_MAP_INIT_LOAD_FACTOR, LINKED_HASH_MAP_ACCESS_ORDERED));

  /**
   * Create a new instance of {@link LIRSEvictor}.
   *
   * @param view a view of block metadata information
   * @param allocator an allocation policy
   */
  public LIRSEvictor(BlockMetadataManagerView view, Allocator allocator) {
    super(view, allocator);
    mConfiguration = WorkerContext.getConf();
    mHIRPercent = mConfiguration.getDouble(Constants.WORKER_EVICTOR_LIRS_HIR_PERCENT);
    Preconditions.checkArgument(mHIRPercent >= 0 && mHIRPercent <= 1,
        "HIR percent should be larger than 0 and less than 1");
    mLIRPercent = 1.0 - mHIRPercent;

    for (StorageTierView tier : view.getTierViews()) {
      for (StorageDirView dir : tier.getDirViews()) {
        long lirBytes = 0;
        long hirBytes = 0;
        long totalBytes = dir.getAvailableBytes() + dir.getEvitableBytes();
        BlockStoreLocation location =
            new BlockStoreLocation(tier.getTierViewAlias(), dir.getDirViewIndex());
        for (BlockMeta blockMeta : dir.getEvictableBlocks()) {
          long blockId = blockMeta.getBlockId();
          long blocksize = blockMeta.getBlockSize();
          Pair<BlockStoreLocation, Long> key =
              new Pair<BlockStoreLocation, Long>(location, blockId);
          // If the size of LIR blocks exceeds the limit, put it into HIR set.
          if (lirBytes + blocksize > mLIRPercent * totalBytes) {
            // put into HIR set
            BlockLIRSInfo blockinfo = new BlockLIRSInfo(false, true);
            mLIRCache.put(key, blockinfo);
            mHIRCache.put(key, blockinfo);
            hirBytes += blocksize;
          } else {
            // put into LIR set
            mLIRCache.put(key, new BlockLIRSInfo(true, true));
            lirBytes += blocksize;
          }
          mBlockIdToSize.put(blockId, blocksize);
        }
        mSpaceManager.put(location, new SpaceContainer(hirBytes, lirBytes, totalBytes));
      }
    }
  }

  /**
   * Generate the iterator of blocks for eviction. HIR blocks will be evicted first, so first
   * generate the iterator of HIR blocks. Second, generate the iterator of temp blocks in case that
   * the temp blocks will be maintained too long. Third, generate the iterator of LIR blocks because
   * LIR blocks need to be evicted at last.
   *
   * @return the merged iterator of HIR blocks, temp blocks and LIR blocks
   */
  @Override
  protected Iterator<Long> getBlockIterator() {
    Iterator<Pair<BlockStoreLocation, Long>> hirIterator = mHIRCache.keySet().iterator();
    Iterator<Long> hirBlockIterator =
        Iterators.transform(hirIterator, new Function<Pair<BlockStoreLocation, Long>, Long>() {
          @Override
          public Long apply(Pair<BlockStoreLocation, Long> input) {
            return input.getSecond();
          }
        });

    Iterator<Pair<BlockStoreLocation, Long>> tmpIterator = mTmpCache.keySet().iterator();
    Iterator<Long> tmpBlockIterator =
        Iterators.transform(tmpIterator, new Function<Pair<BlockStoreLocation, Long>, Long>() {
          @Override
          public Long apply(Pair<BlockStoreLocation, Long> input) {
            return input.getSecond();
          }
        });

    List<Long> lirBlocks = new ArrayList<Long>();
    Iterator<Map.Entry<Pair<BlockStoreLocation, Long>, BlockLIRSInfo>> lirIterator =
        mLIRCache.entrySet().iterator();
    while (lirIterator.hasNext()) {
      Entry<Pair<BlockStoreLocation, Long>, BlockLIRSInfo> entry = lirIterator.next();
      BlockLIRSInfo blockInfo = entry.getValue();
      if (blockInfo.isResident() && blockInfo.isLIR()) {
        lirBlocks.add(entry.getKey().getSecond());
      }
    }
    Collections.reverse(lirBlocks);
    return Iterators.concat(hirBlockIterator, tmpBlockIterator, lirBlocks.iterator());
  }

  @Override
  public void onAccessBlock(long userId, long blockId) {
    updateOnAccess(blockId);
  }

  @Override
  public void onCommitBlock(long userId, long blockId, BlockStoreLocation location) {
    try {
      updateOnCommit(blockId, location);
    } catch (BlockDoesNotExistException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void onMoveBlockByClient(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    updateOnMove(blockId, oldLocation, newLocation);
  }

  @Override
  public void onMoveBlockByWorker(long sessionId, long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    updateOnMove(blockId, oldLocation, newLocation);
  }

  @Override
  public void onRemoveBlockByClient(long userId, long blockId) {
    updateOnRemove(blockId);
  }

  @Override
  public void onRemoveBlockByWorker(long userId, long blockId) {
    updateOnRemove(blockId);
  }

  @Override
  protected void onRemoveBlockFromIterator(long blockId) {
    mBlockIdToSize.remove(blockId);
    for (StorageTierView tierView : mManagerView.getTierViews()) {
      for (StorageDirView dirView : tierView.getDirViews()) {
        BlockStoreLocation location =
            new BlockStoreLocation(tierView.getTierViewAlias(), dirView.getDirViewIndex());
        Pair<BlockStoreLocation, Long> blockEntry =
            new Pair<BlockStoreLocation, Long>(location, blockId);
        mHIRCache.remove(blockEntry);
        mLIRCache.remove(blockEntry);
        mTmpCache.remove(blockEntry);
      }
    }
  }

  private void updateOnAccess(long blockId) {
    for (StorageTierView tier : mManagerView.getTierViews()) {
      for (StorageDirView dir : tier.getDirViews()) {
        BlockStoreLocation location =
            new BlockStoreLocation(tier.getTierViewAlias(), dir.getDirViewIndex());
        Pair<BlockStoreLocation, Long> key = new Pair<BlockStoreLocation, Long>(location, blockId);
        SpaceContainer spaceContainer = mSpaceManager.get(location);
        long blockSize = mBlockIdToSize.get(blockId);
        // 1: Block hits in tmp cache
        if (mTmpCache.containsKey(key)) {
          mTmpCache.remove(key);
          // Move the block to LIR cache if a ghost block exists in LIR cache
          if (mLIRCache.containsKey(key)) {
            BlockLIRSInfo blockinfo = mLIRCache.get(key);
            spaceContainer.incrementLIR(blockSize);
            blockinfo.setIsLIR(true);
            blockinfo.setResident(true);
            mLIRCache.remove(key);
            mLIRCache.put(key, blockinfo);
          } else if (spaceContainer.getLIRBytes() + blockSize > spaceContainer.getCapacity()
              * mLIRPercent) {
            // Move the block to HIR cache if LIR cache is full
            spaceContainer.incrementHIR(blockSize);
            BlockLIRSInfo blockinfo = new BlockLIRSInfo(false, true);
            mHIRCache.put(key, blockinfo);
            mLIRCache.put(key, blockinfo);
          } else {
            // Move the block to LIR cache if LIR cache is not full
            spaceContainer.incrementLIR(blockSize);
            BlockLIRSInfo blockinfo = new BlockLIRSInfo(true, true);
            mLIRCache.put(key, blockinfo);
          }
        } else if (mHIRCache.containsKey(key)) {
          // 2. Block hits in HIR cache
          BlockLIRSInfo blockinfo = mHIRCache.get(key);
          if (mLIRCache.containsKey(key)) {
            // Move the block to LIR cache if a ghost block exists in LIR cache
            blockinfo.setIsLIR(true);
            blockinfo.setResident(true);
            mHIRCache.remove(key);
            mLIRCache.remove(key);
            mLIRCache.put(key, blockinfo);
            spaceContainer.moveBlockFromHIRToLIR(blockSize);
          } else {
            // Move the block to HIR cache if no ghost block exists
            mHIRCache.remove(key);
            mHIRCache.put(key, blockinfo);
            mLIRCache.put(key, blockinfo);
          }
        } else if (mLIRCache.containsKey(key)) {
          // 3. Block hits in LIR cache, just move the block to top of LIR cache
          BlockLIRSInfo blockInfo = mLIRCache.get(key);
          mLIRCache.remove(key);
          mLIRCache.put(key, blockInfo);
        }
        // adjust the space size for LIR blocks
        updateSizeOfLIR(location);
      }
    }
  }

  private void updateOnCommit(long blockId, BlockStoreLocation location)
      throws BlockDoesNotExistException {
    for (StorageTierView tier : mManagerView.getTierViews()) {
      for (StorageDirView dir : tier.getDirViews()) {
        String alias = tier.getTierViewAlias();
        BlockStoreLocation dirLocation = new BlockStoreLocation(alias, dir.getDirViewIndex());
        if (dirLocation.belongsTo(location)) {
          Pair<BlockStoreLocation, Long> key =
              new Pair<BlockStoreLocation, Long>(dirLocation, blockId);
          SpaceContainer spaceContainer = mSpaceManager.get(dirLocation);
          long blockSize = mManagerView.getBlockSizeIfExist(blockId);
          if (blockSize == -1) {
            throw new BlockDoesNotExistException(ExceptionMessage.BLOCK_META_NOT_FOUND, blockId);
          }
          mBlockIdToSize.put(blockId, blockSize);
          if (spaceContainer.getLIRBytes() + blockSize > spaceContainer.getCapacity()
              * mLIRPercent) {
            // Move the block to HIR cache if LIR cache is full
            spaceContainer.incrementHIR(blockSize);
            BlockLIRSInfo blockinfo = new BlockLIRSInfo(false, true);
            mHIRCache.put(key, blockinfo);
            mLIRCache.put(key, blockinfo);
          } else {
            // Move the block to LIR cache if LIR cache is not full
            spaceContainer.incrementLIR(blockSize);
            mLIRCache.put(key, new BlockLIRSInfo(true, true));
          }
          mSpaceManager.put(dirLocation, spaceContainer);
          return;
        }
      }
    }
  }

  private void updateOnMove(long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    // Check if the newLocation belongs to the oldLocation. If so, don't need to move
    if (newLocation.belongsTo(oldLocation)) {
      return;
    }
    for (StorageTierView tier : mManagerView.getTierViews()) {
      for (StorageDirView dir : tier.getDirViews()) {
        BlockStoreLocation location =
            new BlockStoreLocation(tier.getTierViewAlias(), dir.getDirViewIndex());
        Pair<BlockStoreLocation, Long> key = new Pair<BlockStoreLocation, Long>(location, blockId);
        SpaceContainer spaceContainer = mSpaceManager.get(location);
        long blockSize = mBlockIdToSize.get(blockId);
        // 1. Blocks in the StorageDir moved from
        if (location.belongsTo(oldLocation)) {
          if (mHIRCache.containsKey(key)) {
            // Reclaim HIR space if block hits in HIR cache
            spaceContainer.decrementHIR(blockSize);
            if (mLIRCache.containsKey(key)) {
              mLIRCache.get(key).setResident(false);
            }
            mHIRCache.remove(key);
          } else if (mLIRCache.containsKey(key)) {
            // Reclain LIR space if block hits in LIR cache
            BlockLIRSInfo blockinfo = mLIRCache.get(key);
            spaceContainer.decrementLIR(blockSize);
            blockinfo.setResident(false);
            updateSizeOfLIR(location);
          } else if (mTmpCache.containsKey(key)) {
            // Remove from temp cache if block hits in temp cache
            mTmpCache.remove(key);
          }
          mSpaceManager.put(location, spaceContainer);
        } else if (new BlockStoreLocation(tier.getTierViewAlias(), dir.getDirViewIndex())
            .belongsTo(newLocation)) {
          // 2. Blocks in the StorageDir moved to. Just put the blocks into tmp cache
          mTmpCache.put(key, UNUSED_MAP_VALUE);
        }
      }
    }
  }

  private void updateOnRemove(long blockId) {
    for (StorageTierView tier : mManagerView.getTierViews()) {
      for (StorageDirView dir : tier.getDirViews()) {
        BlockStoreLocation location =
            new BlockStoreLocation(tier.getTierViewAlias(), dir.getDirViewIndex());
        SpaceContainer spaceContainer = mSpaceManager.get(location);
        long blockSize = mBlockIdToSize.get(blockId);
        Pair<BlockStoreLocation, Long> key = new Pair<BlockStoreLocation, Long>(location, blockId);
        if (mHIRCache.containsKey(key)) {
          // Reclaim HIR space and remove from HIR cache
          mHIRCache.remove(key);
          spaceContainer.decrementHIR(blockSize);
          // If the block is also in LIR cache, remove it.
          if (mLIRCache.containsKey(key)) {
            mLIRCache.remove(key);
          }
        } else if (mLIRCache.containsKey(key)) {
          // Reclaim LIR space and remove from LIR cache
          if (mLIRCache.get(key).isResident()) {
            spaceContainer.decrementLIR(blockSize);
          }
          mLIRCache.remove(key);
        } else if (mTmpCache.containsKey(key)) {
          // Remove from temp cache
          mTmpCache.remove(key);
        }
        mBlockIdToSize.remove(blockId);
        mSpaceManager.put(location, spaceContainer);
        updateSizeOfLIR(location);
      }
    }
  }

  /**
   * Update the space size for LIR blocks if it exceeds the limit space. Pop the bottom LIR block
   * until LIR size is cut under the limit. The bottom of LIR cache on each StorageDir will never be
   * a ghost block or HIR block. The bottom of LIR cache on each StorageDir is guaranteed to be a
   * LIR block to mark the maximum recency of all IRR blocks so that all blocks in LIR cache has
   * smaller recency than the maximum recency of IRR blocks and are possible to be transformed to a
   * LIR block when they are accessed again.
   *
   * @param location location of the StorageDir
   */
  private void updateSizeOfLIR(BlockStoreLocation location) {
    SpaceContainer spaceContainer = mSpaceManager.get(location);
    Iterator<Map.Entry<Pair<BlockStoreLocation, Long>, BlockLIRSInfo>> it =
        mLIRCache.entrySet().iterator();
    if (!it.hasNext()) {
      return;
    }
    Entry<Pair<BlockStoreLocation, Long>, BlockLIRSInfo> entry = it.next();
    BlockLIRSInfo blockinfo = entry.getValue();
    while (spaceContainer.getLIRBytes() > spaceContainer.getCapacity() * mLIRPercent
        || !entry.getKey().getFirst().equals(location) || blockinfo.isHIR()
        || !blockinfo.isResident()) {
      if (!entry.getKey().getFirst().equals(location)) {
        if (it.hasNext()) {
          entry = it.next();
          blockinfo = entry.getValue();
          continue;
        } else {
          break;
        }
      } else if (blockinfo.isHIR() || !blockinfo.isResident()) {
        it.remove();
      } else if (blockinfo.isLIR()) {
        long blockSize = mBlockIdToSize.get(entry.getKey().getSecond());
        blockinfo.setIsHIR(true);
        blockinfo.setResident(true);
        spaceContainer.moveBlockFromLIRToHIR(blockSize);
        mHIRCache.put(entry.getKey(), blockinfo);
        it.remove();
      }
      if (it.hasNext()) {
        entry = it.next();
        blockinfo = entry.getValue();
      } else {
        break;
      }
    }
    mSpaceManager.put(location, spaceContainer);
  }

  /**
   * Class to record the state of a block. There are 3 kinds of state of a block: HIR block: an HIR
   * block which will be evicted first; LIR block: a LIR block which will be evicted after HIR
   * blocks; Ghost block (not resident on the tier): a block which has been moved to other tiers,
   * but an entry is maintained to mark when it is last accessed.
   */
  class BlockLIRSInfo {
    // If the block is an HIR block
    private boolean mIsHIR;
    // If the block is an LIR block
    private boolean mIsLIR;
    // If the block is a ghost block (resident on the tier)
    private boolean mIsResident;

    public BlockLIRSInfo(boolean isLIR, boolean isResident) {
      mIsLIR = isLIR;
      mIsHIR = !isLIR;
      mIsResident = isResident;
    }

    public boolean isHIR() {
      return mIsHIR;
    }

    public boolean isLIR() {
      return mIsLIR;
    }

    public boolean isResident() {
      return mIsResident;
    }

    public void setResident(boolean isResident) {
      mIsResident = isResident;
    }

    public void setIsHIR(boolean isHIR) {
      mIsLIR = !isHIR;
      mIsHIR = isHIR;
    }

    public void setIsLIR(boolean isLIR) {
      mIsLIR = isLIR;
      mIsHIR = !isLIR;
    }
  }

  /**
   * Class to record the space used condition of a StorageDir.
   */
  class SpaceContainer {
    // Used space of HIR blocks
    private long mHIRBytes;
    // Used space of LIR blocks
    private long mLIRBytes;
    // Capacity space of the StorageDir
    private long mCapacity;

    public SpaceContainer(long hirBytes, long lirBytes, long capacity) {
      mHIRBytes = hirBytes;
      mLIRBytes = lirBytes;
      mCapacity = capacity;
    }

    public long getHIRBytes() {
      return mHIRBytes;
    }

    public long getLIRBytes() {
      return mLIRBytes;
    }

    public long getCapacity() {
      return mCapacity;
    }

    public void decrementHIR(long blockSize) {
      mHIRBytes -= blockSize;
    }

    public void decrementLIR(long blockSize) {
      mLIRBytes -= blockSize;
    }

    public void incrementHIR(long blockSize) {
      mHIRBytes += blockSize;
    }

    public void incrementLIR(long blockSize) {
      mLIRBytes += blockSize;
    }

    /**
     * Resize the space for HIR blocks and LIR blocks when a block moves from HIR to LIR.
     *
     * @param blockSize the size of a block
     */
    public void moveBlockFromHIRToLIR(long blockSize) {
      mHIRBytes -= blockSize;
      mLIRBytes += blockSize;
    }

    /**
     * Resize the space for HIR blocks and LIR blocks when a block moves from LIR to HIR.
     *
     * @param blockSize the size of a block
     */
    public void moveBlockFromLIRToHIR(long blockSize) {
      mHIRBytes += blockSize;
      mLIRBytes -= blockSize;
    }
  }
}
