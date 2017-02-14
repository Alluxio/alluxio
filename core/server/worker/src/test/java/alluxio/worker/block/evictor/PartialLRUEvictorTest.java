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

package alluxio.worker.block.evictor;

import alluxio.ConfigurationTestUtils;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.TieredBlockStoreTestUtils;
import alluxio.worker.block.meta.StorageDir;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for specific behavior of {@link PartialLRUEvictor} such as evicting/moving least
 * recently used blocks in {@link StorageDir} with max free space and cascading
 * {@link PartialLRUEvictor} eviction.
 */
public class PartialLRUEvictorTest extends EvictorTestBase {

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public final void before() throws Exception {
    init(PartialLRUEvictor.class.getName());
  }

  /**
   * Resets the context of the worker after a test ran.
   */
  @After
  public void after() {
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests that the eviction in the bottom tier works.
   */
  @Test
  public void evictInBottomTier() throws Exception {
    int bottomTierLevel =
        TieredBlockStoreTestUtils.TIER_ORDINAL[TieredBlockStoreTestUtils.TIER_ORDINAL.length - 1];
    // capacity increases with index
    long[] bottomTierDirCapacity = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[bottomTierLevel];
    long smallestCapacity = bottomTierDirCapacity[0];
    long delta = smallestCapacity / 10;
    int nDir = bottomTierDirCapacity.length;
    // free space of StorageDir increases with Dir index
    for (int i = 0; i < nDir; i++) {
      cache(SESSION_ID, BLOCK_ID + i, bottomTierDirCapacity[i] - i * delta, bottomTierLevel, i);
    }

    BlockStoreLocation anyDirInBottomTier =
        BlockStoreLocation.anyDirInTier(TieredBlockStoreTestUtils.TIER_ALIAS[bottomTierLevel]);
    // free the StorageDir with max free space
    EvictionPlan plan =
        mEvictor.freeSpaceWithView(smallestCapacity, anyDirInBottomTier, mManagerView);
    Assert.assertNotNull(plan);
    Assert.assertTrue(plan.toMove().isEmpty());
    Assert.assertEquals(1, plan.toEvict().size());
    long toEvictBlockId = plan.toEvict().get(0).getFirst();
    Assert.assertEquals(BLOCK_ID + nDir - 1, toEvictBlockId);
  }

  /**
   * Tests the cascading eviction with the first tier filled and the second tier empty resulting in
   * no eviction.
   */
  @Test
  public void cascadingEvictionTest1() throws Exception {
    // Two tiers, each dir in the second tier has more space than any dir in the first tier. Fill in
    // the first tier, leave the second tier empty. Request space from the first tier, blocks should
    // be moved from the first to the second tier without eviction.
    int firstTierOrdinal = TieredBlockStoreTestUtils.TIER_ORDINAL[0];
    long[] firstTierDirCapacity = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[0];
    long smallestCapacity = firstTierDirCapacity[0];
    long delta = smallestCapacity / 10;
    int nDir = firstTierDirCapacity.length;
    for (int i = 0; i < nDir; i++) {
      cache(SESSION_ID, BLOCK_ID + i, firstTierDirCapacity[i] - delta * i, firstTierOrdinal, i);
    }
    BlockStoreLocation anyDirInFirstTier =
        BlockStoreLocation.anyDirInTier(TieredBlockStoreTestUtils.TIER_ALIAS[firstTierOrdinal]);
    EvictionPlan plan =
        mEvictor.freeSpaceWithView(smallestCapacity, anyDirInFirstTier, mManagerView);
    Assert.assertTrue(EvictorTestUtils.validCascadingPlan(smallestCapacity, plan, mMetaManager));
    Assert.assertEquals(0, plan.toEvict().size());
    Assert.assertEquals(1, plan.toMove().size());
    long blockId = plan.toMove().get(0).getBlockId();
    Assert.assertEquals(BLOCK_ID + nDir - 1, blockId);
  }

  /**
   * Tests the cascading eviction with the first and second tier filled resulting in blocks in the
   * second tier are evicted.
   */
  @Test
  public void cascadingEvictionTest2() throws Exception {
    // Two tiers, the second tier has more dirs than the first tier and each dir in the second tier
    // has more space than any dir in the first tier. Fill in all dirs and request space from the
    // first tier, blocks should be moved from the first to the second tier, and some blocks in the
    // second tier should be evicted to hold blocks moved from the first tier.
    BlockStoreLocation anyDirInFirstTier =
        BlockStoreLocation.anyDirInTier(TieredBlockStoreTestUtils.TIER_ALIAS[0]);
    int nDirInFirstTier = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[0].length;
    int nDirInSecondTier = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[1].length;
    long smallestCapacity = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[0][0];
    long delta = smallestCapacity / 10;
    long blockId = BLOCK_ID;

    for (int tierLevel : TieredBlockStoreTestUtils.TIER_ORDINAL) {
      long[] tierCapacity = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[tierLevel];
      for (int dirIdx = 0; dirIdx < tierCapacity.length; dirIdx++) {
        cache(SESSION_ID, blockId, tierCapacity[dirIdx] - dirIdx * delta, tierLevel, dirIdx);
        blockId++;
      }
    }

    EvictionPlan plan =
        mEvictor.freeSpaceWithView(smallestCapacity, anyDirInFirstTier, mManagerView);
    Assert.assertTrue(EvictorTestUtils.validCascadingPlan(smallestCapacity, plan, mMetaManager));
    // block in StorageDir with max free space in the first tier needs to be moved to the second
    // tier
    Assert.assertEquals(1, plan.toMove().size());
    long blockIdMovedInFirstTier = plan.toMove().get(0).getBlockId();
    Assert.assertEquals(BLOCK_ID + nDirInFirstTier - 1, blockIdMovedInFirstTier);
    // block in StorageDir with max free space in the second tier will be evicted to hold blocks
    // moved from first tier
    Assert.assertEquals(1, plan.toEvict().size());
    long blockIdEvictedInSecondTier = plan.toEvict().get(0).getFirst();
    Assert.assertEquals(BLOCK_ID + nDirInFirstTier + nDirInSecondTier - 1,
        blockIdEvictedInSecondTier);
  }
}
