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

package tachyon.worker.block.evictor;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.worker.block.BlockStoreEventListener;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.TieredBlockStoreTestUtils;

/**
 * Sanity check on specific behavior of LRUEvictor such as evicting/moving least recently used
 * blocks and cascading LRU eviction.
 */
public class LRUEvictorTestBase extends EvictorTestBase {
  @Before
  public final void before() throws Exception {
    init(LRUEvictor.class.getName());
  }

  // access the block to update evictor
  private void access(long blockId) {
    ((BlockStoreEventListener) mEvictor).onAccessBlock(SESSION_ID, blockId);
  }

  @Test
  public void evictInBottomTierTest() throws Exception {
    int bottomTierLevel =
        TieredBlockStoreTestUtils.TIER_LEVEL[TieredBlockStoreTestUtils.TIER_LEVEL.length - 1];
    // capacity increases with index
    long[] bottomTierDirCapacity = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[bottomTierLevel];
    int nDir = bottomTierDirCapacity.length;
    // fill in dirs from larger to smaller capacity with blockId equal to BLOCK_ID plus dir index
    for (int i = nDir - 1; i >= 0; i --) {
      cache(SESSION_ID, BLOCK_ID + i, bottomTierDirCapacity[i], bottomTierLevel, i);
    }
    BlockStoreLocation anyDirInBottomTier = BlockStoreLocation.anyDirInTier(bottomTierLevel + 1);
    // request smallest capacity and update access time on the evicted block for nDir times, the dir
    // to evict blocks from should be in the same order as caching
    for (int i = nDir - 1; i >= 0; i --) {
      EvictionPlan plan =
          mEvictor.freeSpaceWithView(bottomTierDirCapacity[0], anyDirInBottomTier, mManagerView);
      Assert.assertNotNull(plan);
      Assert.assertTrue(plan.toMove().isEmpty());
      Assert.assertEquals(1, plan.toEvict().size());
      long toEvictBlockId = plan.toEvict().get(0).getFirst();
      Assert.assertEquals(BLOCK_ID + i, toEvictBlockId);

      access(toEvictBlockId);
    }
  }

  @Test
  public void cascadingEvictionTest1() throws Exception {
    // Two tiers, each dir in the second tier has more space than any dir in the first tier. Fill in
    // the first tier, leave the second tier empty. Request space from the first tier, blocks should
    // be moved from the first to the second tier without eviction.
    int firstTierLevel = TieredBlockStoreTestUtils.TIER_LEVEL[0];
    long[] firstTierDirCapacity = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[0];
    int nDir = firstTierDirCapacity.length;
    for (int i = 0; i < nDir; i ++) {
      cache(SESSION_ID, BLOCK_ID + i, firstTierDirCapacity[i], firstTierLevel, i);
    }
    BlockStoreLocation anyDirInFirstTier = BlockStoreLocation.anyDirInTier(firstTierLevel + 1);
    long smallestCapacity = firstTierDirCapacity[0];
    for (int i = 0; i < nDir; i ++) {
      EvictionPlan plan =
          mEvictor.freeSpaceWithView(smallestCapacity, anyDirInFirstTier, mManagerView);
      Assert.assertTrue(EvictorTestUtils.validCascadingPlan(smallestCapacity, plan, mMetaManager));
      Assert.assertEquals(0, plan.toEvict().size());
      Assert.assertEquals(1, plan.toMove().size());
      long blockId = plan.toMove().get(0).getBlockId();
      Assert.assertEquals(BLOCK_ID + i, blockId);

      access(blockId);
    }
  }

  @Test
  public void cascadingEvictionTest2() throws Exception {
    // Two tiers, the second tier has more dirs than the first tier and each dir in the second tier
    // has more space than any dir in the first tier. Fill in all dirs and request space from the
    // first tier, blocks should be moved from the first to the second tier, and some blocks in the
    // second tier should be evicted to hold blocks moved from the first tier.
    long blockId = BLOCK_ID;
    for (int tierLevel : TieredBlockStoreTestUtils.TIER_LEVEL) {
      long[] tierCapacity = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[tierLevel];
      for (int dirIdx = 0; dirIdx < tierCapacity.length; dirIdx ++) {
        cache(SESSION_ID, blockId, tierCapacity[dirIdx], tierLevel, dirIdx);
        blockId ++;
      }
    }

    BlockStoreLocation anyDirInFirstTier =
        BlockStoreLocation.anyDirInTier(TieredBlockStoreTestUtils.TIER_LEVEL[0] + 1);
    int nDirInFirstTier = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[0].length;
    long smallestCapacity = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[0][0];
    for (int i = 0; i < nDirInFirstTier; i ++) {
      EvictionPlan plan =
          mEvictor.freeSpaceWithView(smallestCapacity, anyDirInFirstTier, mManagerView);
      Assert.assertTrue(EvictorTestUtils.validCascadingPlan(smallestCapacity, plan, mMetaManager));
      // least recently used block in the first tier needs to be moved to the second tier
      Assert.assertEquals(1, plan.toMove().size());
      long blockIdMovedInFirstTier = plan.toMove().get(0).getBlockId();
      Assert.assertEquals(BLOCK_ID + i, blockIdMovedInFirstTier);
      // least recently used cached block in the second tier will be evicted to hold blocks moved
      // from first tier
      Assert.assertEquals(1, plan.toEvict().size());
      long blockIdEvictedInSecondTier = plan.toEvict().get(0).getFirst();
      Assert.assertEquals(BLOCK_ID + nDirInFirstTier + i, blockIdEvictedInSecondTier);

      access(blockIdMovedInFirstTier);
      access(blockIdEvictedInSecondTier);
    }
  }

  @Test
  public void cascadingEvictionTest3() throws Exception {
    // First Tier 2000, 3000
    // Second Tier 10000, 20000, 30000
    int blockSize = 1000;
    cache(SESSION_ID, 101, blockSize, 0, 0);
    cache(SESSION_ID, 102, blockSize, 0, 0);
    cache(SESSION_ID, 103, blockSize, 0, 1);
    cache(SESSION_ID, 104, blockSize, 0, 1);
    cache(SESSION_ID, 105, blockSize, 0, 1);
    cache(SESSION_ID, 106, 9500, 1, 2);

    // After caching blocks, the free space looks like
    // First Tier 0, 0
    // Second Tier 10000, 20000, 200500
    BlockStoreLocation anyDirInFirstTier = BlockStoreLocation.anyDirInTier(1);
    BlockStoreLocation firstDirSecondTier = new BlockStoreLocation(2, 1, 0);
    BlockStoreLocation secondDirSecondTier = new BlockStoreLocation(2, 1, 1);
    BlockStoreLocation thirdDirSecondTier = new BlockStoreLocation(2, 1, 2);

    EvictionPlan plan = mEvictor.freeSpaceWithView(blockSize * 2, anyDirInFirstTier, mManagerView);
    Assert.assertNotNull(plan);
    Assert.assertEquals(0, plan.toEvict().size());
    Assert.assertEquals(2, plan.toMove().size());

    // 2 blocks to move. The first one should be moved the 3rd dir as it has max free space.
    long blockId = plan.toMove().get(0).getBlockId();
    Assert.assertEquals(101, blockId);
    BlockStoreLocation dstLocation = plan.toMove().get(0).getDstLocation();
    Assert.assertEquals(thirdDirSecondTier, dstLocation);

    // The second one should be moved the 2nd dir because after the first move the second dir
    // has the max free space.
    blockId = plan.toMove().get(1).getBlockId();
    Assert.assertEquals(102, blockId);
    dstLocation = plan.toMove().get(1).getDstLocation();
    Assert.assertEquals(secondDirSecondTier, dstLocation);

    cache(SESSION_ID, 107, 10000, 1, 0);
    cache(SESSION_ID, 108, 20000, 1, 1);
    cache(SESSION_ID, 109, 19000, 1, 2);
    access(106);

    // After caching more blocks, the free space looks like
    // First Tier 0, 0
    // Second Tier 0, 0, 1500
    plan = mEvictor.freeSpaceWithView(blockSize * 3, anyDirInFirstTier, mManagerView);
    Assert.assertNotNull(plan);
    Assert.assertEquals(1, plan.toEvict().size());
    Assert.assertEquals(3, plan.toMove().size());

    blockId = plan.toEvict().get(0).getFirst();
    Assert.assertEquals(107, blockId);

    // 3 blocks to move. The first one should be moved the 3rd dir as it has max free space.
    blockId = plan.toMove().get(0).getBlockId();
    Assert.assertEquals(103, blockId);
    dstLocation = plan.toMove().get(0).getDstLocation();
    Assert.assertEquals(thirdDirSecondTier, dstLocation);

    // The other two should be moved the 1st dir because the 1st dir has the max free space
    // after evicting block 107.
    blockId = plan.toMove().get(1).getBlockId();
    Assert.assertEquals(104, blockId);
    dstLocation = plan.toMove().get(1).getDstLocation();
    Assert.assertEquals(firstDirSecondTier, dstLocation);

    blockId = plan.toMove().get(2).getBlockId();
    Assert.assertEquals(105, blockId);
    dstLocation = plan.toMove().get(1).getDstLocation();
    Assert.assertEquals(firstDirSecondTier, dstLocation);
  }
}
