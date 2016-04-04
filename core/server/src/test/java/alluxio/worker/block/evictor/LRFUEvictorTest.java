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
import alluxio.worker.WorkerContext;
import alluxio.worker.block.BlockMetadataManager;
import alluxio.worker.block.BlockMetadataManagerView;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.TieredBlockStoreTestUtils;
import alluxio.worker.block.allocator.Allocator;
import alluxio.worker.block.allocator.MaxFreeAllocator;
import alluxio.worker.block.meta.StorageDir;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Unit tests for specific behavior of {@link LRFUEvictor} such as evicting/moving blocks with
 * minimum CRF value and cascading LRFU eviction.
 */
public class LRFUEvictorTest {
  private static final long SESSION_ID = 2;
  private static final long BLOCK_ID = 10;

  private BlockMetadataManager mMetaManager;
  private BlockMetadataManagerView mManagerView;
  private Evictor mEvictor;
  private Allocator mAllocator;

  private double mStepFactor;
  private double mAttenuationFactor;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Sets up all dependencies before a test runs.
   *
   * @throws Exception if setting up the meta manager, the lock manager or the evictor fails
   */
  @Before
  public final void before() throws Exception {
    File tempFolder = mTestFolder.newFolder();
    mMetaManager = TieredBlockStoreTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());
    mManagerView =
        new BlockMetadataManagerView(mMetaManager, Collections.<Long>emptySet(),
            Collections.<Long>emptySet());
    Configuration conf = WorkerContext.getConf();
    conf.set(Constants.WORKER_EVICTOR_CLASS, LRFUEvictor.class.getName());
    conf.set(Constants.WORKER_ALLOCATOR_CLASS, MaxFreeAllocator.class.getName());
    mAllocator = Allocator.Factory.create(conf, mManagerView);
    mStepFactor = conf.getDouble(Constants.WORKER_EVICTOR_LRFU_STEP_FACTOR);
    mAttenuationFactor =
        conf.getDouble(Constants.WORKER_EVICTOR_LRFU_ATTENUATION_FACTOR);
    mEvictor = Evictor.Factory.create(conf, mManagerView, mAllocator);
  }

  private void cache(long sessionId, long blockId, long bytes, int tierLevel, int dirIdx)
      throws Exception {
    StorageDir dir = mMetaManager.getTiers().get(tierLevel).getDir(dirIdx);
    TieredBlockStoreTestUtils.cache(sessionId, blockId, bytes, dir, mMetaManager, mEvictor);
  }

  // access the block to update evictor
  private void access(long blockId) {
    ((BlockStoreEventListener) mEvictor).onAccessBlock(SESSION_ID, blockId);
  }

  private double calculateAccessWeight(long timeInterval) {
    return Math.pow(1.0 / mAttenuationFactor, mStepFactor * timeInterval);
  }

  /**
   * Sort all blocks in ascending order of CRF.
   *
   * @return the sorted CRF of all blocks
   */
  private List<Map.Entry<Long, Double>> getSortedCRF(Map<Long, Double> crfMap) {
    List<Map.Entry<Long, Double>> sortedCRF =
        new ArrayList<Map.Entry<Long, Double>>(crfMap.entrySet());
    Collections.sort(sortedCRF, new Comparator<Map.Entry<Long, Double>>() {
      @Override
      public int compare(Entry<Long, Double> o1, Entry<Long, Double> o2) {
        double res = o1.getValue() - o2.getValue();
        if (res < 0) {
          return -1;
        } else if (res > 0) {
          return 1;
        } else {
          return 0;
        }
      }
    });
    return sortedCRF;
  }

  /**
   * Tests that the eviction in the bottom tier works.
   *
   * @throws Exception if the caching fails
   */
  @Test
  public void evictInBottomTierTest() throws Exception {
    int bottomTierOrdinal = TieredBlockStoreTestUtils
        .TIER_ORDINAL[TieredBlockStoreTestUtils.TIER_ORDINAL.length - 1];
    Map<Long, Double> blockIdToCRF = new HashMap<Long, Double>();
    // capacity increases with index
    long[] bottomTierDirCapacity = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[bottomTierOrdinal];
    int nDir = bottomTierDirCapacity.length;
    // fill in dirs from larger to smaller capacity with blockId equal to BLOCK_ID plus dir index
    for (int i = 0; i < nDir; i++) {
      cache(SESSION_ID, BLOCK_ID + i, bottomTierDirCapacity[i], bottomTierOrdinal, i);
      // update CRF of blocks when blocks are committed
      blockIdToCRF.put(BLOCK_ID + i, calculateAccessWeight(nDir - 1 - i));
    }
    // access blocks in the order: 10, 10, 11, 10, 11, 12. Update CRF of all blocks
    // during each access
    for (int i = 0; i < nDir; i++) {
      for (int j = 0; j <= i; j++) {
        access(BLOCK_ID + j);
        for (int k = 0; k < nDir; k++) {
          if (k == j) {
            blockIdToCRF.put(BLOCK_ID + k,
                blockIdToCRF.get(BLOCK_ID + k) * calculateAccessWeight(1L) + 1.0);
          } else {
            blockIdToCRF.put(BLOCK_ID + k,
                blockIdToCRF.get(BLOCK_ID + k) * calculateAccessWeight(1L));
          }
        }
      }
    }
    // sort blocks in ascending order of CRF
    List<Entry<Long, Double>> blockCRF = getSortedCRF(blockIdToCRF);
    BlockStoreLocation anyDirInBottomTier =
        BlockStoreLocation.anyDirInTier(TieredBlockStoreTestUtils.TIER_ALIAS[bottomTierOrdinal]);
    // request smallest capacity and update access time on the evicted block for nDir times, the dir
    // to evict blocks from should be in the same order as sorted blockCRF
    for (int i = 0; i < nDir; i++) {
      EvictionPlan plan =
          mEvictor.freeSpaceWithView(bottomTierDirCapacity[0], anyDirInBottomTier, mManagerView);
      Assert.assertNotNull(plan);
      Assert.assertTrue(plan.toMove().isEmpty());
      Assert.assertEquals(1, plan.toEvict().size());
      long toEvictBlockId = plan.toEvict().get(0).getFirst();
      long objectBlockId = blockCRF.get(i).getKey();
      Assert.assertEquals(objectBlockId + " " + toEvictBlockId, objectBlockId, toEvictBlockId);
      // update CRF of the chosen block in case that it is chosen again
      for (int j = 0; j < nDir; j++) {
        access(toEvictBlockId);
      }
    }
  }

  /**
   * Tests the cascading eviction with the first tier filled and the second tier empty resulting in
   * no eviction.
   *
   * @throws Exception if the caching fails
   */
  @Test
  public void cascadingEvictionTest1() throws Exception {
    // Two tiers, each dir in the second tier has more space than any dir in the first tier. Fill in
    // the first tier, leave the second tier empty. Request space from the first tier, blocks should
    // be moved from the first to the second tier without eviction.
    int firstTierOrdinal = TieredBlockStoreTestUtils.TIER_ORDINAL[0];
    long[] firstTierDirCapacity = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[0];
    int nDir = firstTierDirCapacity.length;
    Map<Long, Double> blockIdToCRF = new HashMap<Long, Double>();
    for (int i = 0; i < nDir; i++) {
      cache(SESSION_ID, BLOCK_ID + i, firstTierDirCapacity[i], firstTierOrdinal, i);
      // update CRF of blocks when blocks are committed
      blockIdToCRF.put(BLOCK_ID + i, calculateAccessWeight(nDir - 1 - i));
    }
    // access blocks in the order: 10, 10, 11. Update CRF of all blocks
    // during each access
    for (int i = 0; i < nDir; i++) {
      for (int j = 0; j <= i; j++) {
        access(BLOCK_ID + j);
        for (int k = 0; k < nDir; k++) {
          if (k == j) {
            blockIdToCRF.put(BLOCK_ID + k,
                blockIdToCRF.get(BLOCK_ID + k) * calculateAccessWeight(1L) + 1.0);
          } else {
            blockIdToCRF.put(BLOCK_ID + k,
                blockIdToCRF.get(BLOCK_ID + k) * calculateAccessWeight(1L));
          }
        }
      }
    }
    List<Map.Entry<Long, Double>> blockCRF = getSortedCRF(blockIdToCRF);
    BlockStoreLocation anyDirInFirstTier =
        BlockStoreLocation.anyDirInTier(TieredBlockStoreTestUtils.TIER_ALIAS[firstTierOrdinal]);
    long smallestCapacity = firstTierDirCapacity[0];
    // request smallest capacity and update access time on the moved block for nDir times, the dir
    // to move blocks from should be in the same order as sorted blockCRF
    for (int i = 0; i < nDir; i++) {
      EvictionPlan plan =
          mEvictor.freeSpaceWithView(smallestCapacity, anyDirInFirstTier, mManagerView);
      Assert.assertTrue(EvictorTestUtils.validCascadingPlan(smallestCapacity, plan, mMetaManager));
      Assert.assertEquals(0, plan.toEvict().size());
      Assert.assertEquals(1, plan.toMove().size());
      long blockId = plan.toMove().get(0).getBlockId();
      long objectBlockId = blockCRF.get(i).getKey();
      Assert.assertEquals(objectBlockId, blockId);
      // update CRF of the chosen block in case that it is chosen again
      for (int j = 0; j < nDir; j++) {
        access(objectBlockId);
      }
    }
  }

  /**
   * Tests the cascading eviction with the first and second tier filled resulting in blocks in the
   * second tier are evicted.
   *
   * @throws Exception if the caching fails
   */
  @Test
  public void cascadingEvictionTest2() throws Exception {
    // Two tiers, the second tier has more dirs than the first tier and each dir in the second tier
    // has more space than any dir in the first tier. Fill in all dirs and request space from the
    // first tier, blocks should be moved from the first to the second tier, and some blocks in the
    // second tier should be evicted to hold blocks moved from the first tier.
    long blockId = BLOCK_ID;
    long totalBlocks = 0;
    for (int tierOrdinal : TieredBlockStoreTestUtils.TIER_ORDINAL) {
      totalBlocks += TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[tierOrdinal].length;
    }
    Map<Long, Double> blockIdToCRF = new HashMap<Long, Double>();
    for (int tierOrdinal : TieredBlockStoreTestUtils.TIER_ORDINAL) {
      long[] tierCapacity = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[tierOrdinal];
      for (int dirIdx = 0; dirIdx < tierCapacity.length; dirIdx++) {
        cache(SESSION_ID, blockId, tierCapacity[dirIdx], tierOrdinal, dirIdx);
        // update CRF of blocks when blocks are committed
        blockIdToCRF.put(blockId, calculateAccessWeight(totalBlocks - 1 - (blockId - BLOCK_ID)));
        blockId++;
      }
    }

    // access blocks in the order: 10, 10, 11, 10, 11, 12, 10, 11, 12, 13, 10, 11, 12, 13, 14
    // Update CRF of all blocks during each access
    for (int i = 0; i < totalBlocks; i++) {
      for (int j = 0; j <= i; j++) {
        access(BLOCK_ID + j);
        for (int k = 0; k < totalBlocks; k++) {
          if (k == j) {
            blockIdToCRF.put(BLOCK_ID + k,
                blockIdToCRF.get(BLOCK_ID + k) * calculateAccessWeight(1L) + 1.0);
          } else {
            blockIdToCRF.put(BLOCK_ID + k,
                blockIdToCRF.get(BLOCK_ID + k) * calculateAccessWeight(1L));
          }
        }
      }
    }

    List<Map.Entry<Long, Double>> blockCRF = getSortedCRF(blockIdToCRF);
    // sorted blocks in the first tier
    List<Long> blocksInFirstTier = new ArrayList<Long>();
    // sorted blocks in the second tier
    List<Long> blocksInSecondTier = new ArrayList<Long>();
    for (int i = 0; i < blockCRF.size(); i++) {
      long block = blockCRF.get(i).getKey();
      if (block - BLOCK_ID < TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[0].length) {
        blocksInFirstTier.add(block);
      } else if (block - BLOCK_ID < TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[0].length
          + TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[1].length) {
        blocksInSecondTier.add(block);
      }
    }
    BlockStoreLocation anyDirInFirstTier =
        BlockStoreLocation.anyDirInTier(TieredBlockStoreTestUtils.TIER_ALIAS[0]);
    int nDirInFirstTier = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[0].length;
    long smallestCapacity = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[0][0];
    for (int i = 0; i < nDirInFirstTier; i++) {
      EvictionPlan plan =
          mEvictor.freeSpaceWithView(smallestCapacity, anyDirInFirstTier, mManagerView);
      Assert.assertTrue(EvictorTestUtils.validCascadingPlan(smallestCapacity, plan, mMetaManager));
      // block with minimum CRF in the first tier needs to be moved to the second tier
      Assert.assertEquals(1, plan.toMove().size());
      long blockIdMovedInFirstTier = plan.toMove().get(0).getBlockId();
      long objectBlockIdInFirstTier = blocksInFirstTier.get(i);
      Assert.assertEquals(objectBlockIdInFirstTier, blockIdMovedInFirstTier);
      // cached block with minimum CRF in the second tier will be evicted to hold blocks moved
      // from first tier
      Assert.assertEquals(1, plan.toEvict().size());
      long blockIdEvictedInSecondTier = plan.toEvict().get(0).getFirst();
      long objectBlockIdInSecondTier = blocksInSecondTier.get(i);
      Assert.assertEquals(objectBlockIdInSecondTier, blockIdEvictedInSecondTier);
      // update CRF of the chosen blocks in case that they are chosen again
      for (int j = 0; j < totalBlocks; j++) {
        access(blockIdMovedInFirstTier);
      }
      for (int j = 0; j < totalBlocks; j++) {
        access(blockIdEvictedInSecondTier);
      }
    }
  }
}
