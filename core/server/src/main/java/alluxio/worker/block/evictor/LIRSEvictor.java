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
 * blocks between last access and second-to-last access to the block. Block with high IRR is
 * predicted to be accessed again in a long time, so they need to be evicted first to make room for
 * other blocks which will be accessed soon.
 *
 * LIRS divides the tier into 2 parts: LIR cache and HIR cache. LIR cache holds blocks with low IRR
 * and HIR cache holds blocks with high IRR. So blocks in HIR cache needs to be evicted first. If
 * blocks in HIR cache gets a low IRR, they will be moved to LIR cache. And if the LIR cache is
 * full, some blocks with relatively high IRR will be moved to HIR cache.
 *
 * LIRS can achieve better performance than LRU and LRFU in loop access pattern. For example, a
 * list of files contain blocks: a1,a2,...,ak. When iterating accessing these files, the blocks
 * will be referenced in the order: a1,a2,...,ak,a1,a2,...,ak,.... some blocks of them will be
 * stored in LIR cache and the others in HIR cache. No HIR blocks will be moved to LIR cache
 * because all blocks have equal IRR. Therefore, all blocks in LIR cache will never be removed which
 * promise the hit rate is at least (number of LIR blocks)/(number of all blocks). while if these
 * blocks cannot be held in the memory in this case, LRU will achieve zero hit rate. It's also
 * verified with Spark kmeans that the hit rate of LIRS policy improves a lot than other policies.
 * So if your workloads contain a lot of iterations like kmeans, LIRS policy is recommended for you.
 * Besides, LIRS can achieve as good performance as LRU and LRFU in most of workloads.
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
  // Maps from the location of a StorageDir to its condition of used space
  private final Map<BlockStoreLocation, SpaceContainer> mSpaceManager =
      new ConcurrentHashMap<BlockStoreLocation, SpaceContainer>();
  /**
   * Maps from the the block id to the block size. Maintain the information of block size is because
   * the space of HIR block or LIR block need to be reclaimed when
   * {@link #onRemoveBlockByClient(long, long)} or {@link #onRemoveBlockByWorker(long, long)} is
   * called.
   */
  private Map<Long, Long> mBlockIdToSize = new ConcurrentHashMap<Long, Long>();
  /**
   * Records the information of all LIR blocks. Pair<BlockStoreLocation, Long> identifies a block
   * because one block may contain more than one ghost copies on all tiers. One ghost block is
   * used to mark the block removed from some tier but it may be referenced again in the future.
   */
  private Map<Pair<BlockStoreLocation, Long>, BlockType> mLIRCache =
      Collections.synchronizedMap(new LinkedHashMap<Pair<BlockStoreLocation, Long>, BlockType>(
          LINKED_HASH_MAP_INIT_CAPACITY, LINKED_HASH_MAP_INIT_LOAD_FACTOR,
          LINKED_HASH_MAP_ACCESS_ORDERED));
  // Record the information of all HIR blocks.
  private Map<Pair<BlockStoreLocation, Long>, BlockType> mHIRCache =
      Collections.synchronizedMap(new LinkedHashMap<Pair<BlockStoreLocation, Long>, BlockType>(
          LINKED_HASH_MAP_INIT_CAPACITY, LINKED_HASH_MAP_INIT_LOAD_FACTOR,
          LINKED_HASH_MAP_ACCESS_ORDERED));

  /**
   * Creates a new instance of {@link LIRSEvictor}.
   *
   * @param view a view of block metadata information
   * @param allocator an allocation policy
   */
  public LIRSEvictor(BlockMetadataManagerView view, Allocator allocator) {
    super(view, allocator);
    mConfiguration = WorkerContext.getConf();
    mHIRPercent = mConfiguration.getDouble(Constants.WORKER_EVICTOR_LIRS_HIR_PERCENT);
    Preconditions.checkArgument(mHIRPercent > 0 && mHIRPercent <= 1,
        "HIR percent should be larger than 0 and no less than 1");
    mLIRPercent = 1.0 - mHIRPercent;

    for (StorageTierView tier : view.getTierViews()) {
      for (StorageDirView dir : tier.getDirViews()) {
        long lirBytes = 0;
        long hirBytes = 0;
        long totalBytes = dir.getAvailableBytes() + dir.getEvitableBytes();
        double lirLimitBytes = mLIRPercent * totalBytes;
        BlockStoreLocation location =
            new BlockStoreLocation(tier.getTierViewAlias(), dir.getDirViewIndex());
        for (BlockMeta blockMeta : dir.getEvictableBlocks()) {
          long blockId = blockMeta.getBlockId();
          long blocksize = blockMeta.getBlockSize();
          Pair<BlockStoreLocation, Long> key =
              new Pair<BlockStoreLocation, Long>(location, blockId);
          // If the size of LIR blocks doesn't exceed the limit, put it into HIR cache.
          if (lirBytes + blocksize <= lirLimitBytes) {
            // put into LIR cache.
            mLIRCache.put(key, BlockType.LIR);
            lirBytes += blocksize;
          } else {
            // put into HIR cache.
            mLIRCache.put(key, BlockType.HIR);
            mHIRCache.put(key, BlockType.HIR);
            hirBytes += blocksize;
          }
          mBlockIdToSize.put(blockId, blocksize);
        }
        mSpaceManager.put(location, new SpaceContainer(hirBytes, lirBytes));
      }
    }
  }

  /**
   * Generates the iterator of blocks for eviction. HIR blocks will be evicted first, so first
   * generate the iterator of HIR blocks. Then generate the iterator of LIR blocks because
   * LIR blocks need to be evicted at last.
   *
   * @return the merged iterator of HIR blocks and LIR blocks
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

    List<Long> lirBlocks = new ArrayList<Long>();
    Iterator<Map.Entry<Pair<BlockStoreLocation, Long>, BlockType>> lirIterator =
        mLIRCache.entrySet().iterator();
    while (lirIterator.hasNext()) {
      Entry<Pair<BlockStoreLocation, Long>, BlockType> entry = lirIterator.next();
      BlockType blockInfo = entry.getValue();
      if (blockInfo.isLIR()) {
        lirBlocks.add(entry.getKey().getSecond());
      }
    }
    Collections.reverse(lirBlocks);
    return Iterators.concat(hirBlockIterator, lirBlocks.iterator());
  }

  @Override
  public void onAccessBlock(long sessionId, long blockId) {
    updateOnAccess(blockId);
  }

  @Override
  public void onCommitBlock(long sessionId, long blockId, BlockStoreLocation location) {
    try {
      updateOnCommit(blockId, location);
    } catch (BlockDoesNotExistException e) {
      LOG.warn("Fail to commit block {} because {}", blockId, e);
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
  public void onRemoveBlockByClient(long sessionId, long blockId) {
    updateOnRemove(blockId);
  }

  @Override
  public void onRemoveBlockByWorker(long sessionId, long blockId) {
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
        if (mHIRCache.containsKey(key)) {
          if (mHIRCache.get(key).isMoved()) {
            if (mLIRCache.containsKey(key) || spaceContainer.getLIRBytes() + blockSize
                <= (dir.getAvailableBytes() + dir.getEvitableBytes()) * mLIRPercent) {
              spaceContainer.moveBlockFromHIRToLIR(blockSize);
              mLIRCache.remove(key);
              mHIRCache.remove(key);
              mLIRCache.put(key, BlockType.LIR);
            } else {
              mHIRCache.remove(key);
              mHIRCache.put(key, BlockType.HIR);
              mLIRCache.put(key, BlockType.HIR);
            }
          } else if (mHIRCache.get(key).isHIR()) {
            if (mLIRCache.containsKey(key)) {
              spaceContainer.moveBlockFromHIRToLIR(blockSize);
              mLIRCache.remove(key);
              mHIRCache.remove(key);
              mLIRCache.put(key, BlockType.LIR);
            } else {
              mHIRCache.remove(key);
              mHIRCache.put(key, BlockType.HIR);
              mLIRCache.put(key, BlockType.HIR);
            }
          }
        } else {
          mLIRCache.remove(key);
          mLIRCache.put(key, BlockType.LIR);
        }
        // Adjusts the space size for LIR blocks
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
          BlockMeta blockMeta = mManagerView.getBlockMeta(blockId);
          if (blockMeta == null) {
            throw new BlockDoesNotExistException(ExceptionMessage.BLOCK_NOT_FOUND_AT_LOCATION,
                blockId, location);
          }
          long blockSize = blockMeta.getBlockSize();
          mBlockIdToSize.put(blockId, blockSize);
          if (spaceContainer.getLIRBytes() + blockSize <= (dir.getAvailableBytes()
              + dir.getEvitableBytes()) * mLIRPercent) {
            // Moves the block to LIR cache if LIR cache is not full
            spaceContainer.incrementLIR(blockSize);
            mLIRCache.put(key, BlockType.LIR);
          } else {
            // Moves the block to HIR cache if LIR cache is full
            spaceContainer.incrementHIR(blockSize);
            mHIRCache.put(key, BlockType.HIR);
            mLIRCache.put(key, BlockType.HIR);
          }
          mSpaceManager.put(dirLocation, spaceContainer);
          return;
        }
      }
    }
  }

  private void updateOnMove(long blockId, BlockStoreLocation oldLocation,
      BlockStoreLocation newLocation) {
    // Checks if the newLocation belongs to the oldLocation. If so, don't need to move
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
            // Reclaims HIR space if block hits in HIR cache
            spaceContainer.decrementHIR(blockSize);
            if (mLIRCache.containsKey(key)) {
              mLIRCache.put(key, BlockType.EVICTED);
            }
            mHIRCache.remove(key);
          } else if (mLIRCache.containsKey(key)) {
            // Reclaims LIR space if block hits in LIR cache
            spaceContainer.decrementLIR(blockSize);
            mLIRCache.put(key, BlockType.EVICTED);
            updateSizeOfLIR(location);
          }
        } else if (location.belongsTo(newLocation)) {
          // 2. Blocks in the StorageDir moved to. Just put the blocks into tmp cache
          mHIRCache.put(key, BlockType.MOVED);
          spaceContainer.incrementHIR(blockSize);
        }
        mSpaceManager.put(location, spaceContainer);
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
          // Reclaims HIR space and remove the block from HIR cache
          mHIRCache.remove(key);
          spaceContainer.decrementHIR(blockSize);
          // If the block is also in LIR cache, remove it.
          if (mLIRCache.containsKey(key)) {
            mLIRCache.remove(key);
          }
        } else if (mLIRCache.containsKey(key)) {
          // Reclaims LIR space and remove the block from LIR cache
          if (mLIRCache.get(key).isLIR()) {
            spaceContainer.decrementLIR(blockSize);
          }
          mLIRCache.remove(key);
        }
        mBlockIdToSize.remove(blockId);
        mSpaceManager.put(location, spaceContainer);
        updateSizeOfLIR(location);
      }
    }
  }

  /**
   * Updates the space size for LIR blocks if it exceeds the limit space. Pop the bottom LIR block
   * until LIR size is cut under the limit. The bottom of LIR cache on each StorageDir will never be
   * a ghost block or HIR block. The bottom of LIR cache on each StorageDir is guaranteed to be a
   * LIR block to mark the maximum recency of all IRR blocks so that all blocks in LIR cache has
   * smaller recency than the maximum recency of IRR blocks and are possible to be transformed to a
   * LIR block when they are accessed again.
   *
   * @param location location of the StorageDir
   * @throws BlockDoesNotExistException if the meta data of the block can not be found
   */
  private void updateSizeOfLIR(BlockStoreLocation location) {
    String tierAlias = location.tierAlias();
    int dirIndex = location.dir();
    StorageDirView dirView = mManagerView.getTierView(tierAlias).getDirView(dirIndex);
    long capacity = dirView.getAvailableBytes() + dirView.getEvitableBytes();
    SpaceContainer spaceContainer = mSpaceManager.get(location);
    Iterator<Map.Entry<Pair<BlockStoreLocation, Long>, BlockType>> it =
        mLIRCache.entrySet().iterator();
    if (!it.hasNext()) {
      return;
    }
    Entry<Pair<BlockStoreLocation, Long>, BlockType> entry = it.next();
    BlockType blockType = entry.getValue();
    while (spaceContainer.getLIRBytes() > capacity * mLIRPercent
        || !entry.getKey().getFirst().equals(location) || blockType.isHIR()
        || !blockType.isResident()) {
      if (!entry.getKey().getFirst().equals(location)) {
        if (it.hasNext()) {
          entry = it.next();
          blockType = entry.getValue();
          continue;
        } else {
          break;
        }
      } else if (blockType.isHIR() || !blockType.isResident()) {
        it.remove();
      } else if (blockType.isLIR()) {
        long blockSize = mBlockIdToSize.get(entry.getKey().getSecond());
        spaceContainer.moveBlockFromLIRToHIR(blockSize);
        mHIRCache.put(entry.getKey(), BlockType.HIR);
        it.remove();
      }
      if (it.hasNext()) {
        entry = it.next();
        blockType = entry.getValue();
      } else {
        break;
      }
    }
    mSpaceManager.put(location, spaceContainer);
  }

  /**
   * Block types stored in LIR LRU stack.
   */
  enum BlockType {
    // Blocks resident in HIR cache.
    HIR(1),
    // Blocks moved from other tiers, but haven't been accessed yet.
    MOVED(2),
    // Blocks resident in LIR cache.
    LIR(3),
    // Blocks not resident on current tier.
    EVICTED(4);

    private final int mValue;

    BlockType(int value) {
      mValue = value;
    }

    public boolean isMoved() {
      return mValue == MOVED.mValue;
    }

    public boolean isHIR() {
      return mValue == HIR.mValue;
    }

    public boolean isLIR() {
      return mValue == LIR.mValue;
    }

    public boolean isResident() {
      return mValue != EVICTED.mValue;
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

    public SpaceContainer(long hirBytes, long lirBytes) {
      mHIRBytes = hirBytes;
      mLIRBytes = lirBytes;
    }

    public long getHIRBytes() {
      return mHIRBytes;
    }

    public long getLIRBytes() {
      return mLIRBytes;
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
     * Resizes the space for HIR blocks and LIR blocks when a block moves from HIR to LIR.
     *
     * @param blockSize the size of a block
     */
    public synchronized void moveBlockFromHIRToLIR(long blockSize) {
      mHIRBytes -= blockSize;
      mLIRBytes += blockSize;
    }

    /**
     * Resizes the space for HIR blocks and LIR blocks when a block moves from LIR to HIR.
     *
     * @param blockSize the size of a block
     */
    public synchronized void moveBlockFromLIRToHIR(long blockSize) {
      mHIRBytes += blockSize;
      mLIRBytes -= blockSize;
    }
  }
}
