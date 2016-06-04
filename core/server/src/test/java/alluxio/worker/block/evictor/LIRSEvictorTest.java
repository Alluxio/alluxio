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

import alluxio.worker.WorkerContext;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.TieredBlockStoreTestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for specific behavior of {@link LIRSEvictor}.
 */
public class LIRSEvictorTest extends EvictorTestBase {

  /**
   * Sets up all dependencies before a test runs.
   *
   * @throws Exception if setting up the meta manager, the lock manager or the evictor fails
   */
  @Before
  public final void before() throws Exception {
    init(LIRSEvictor.class.getName());
  }

  /**
   * Resets the context of the worker after a test ran.
   */
  @After
  public void after() {
    WorkerContext.reset();
  }

  // access the block to update evictor
  private void access(long blockId) {
    ((BlockStoreEventListener) mEvictor).onAccessBlock(SESSION_ID, blockId);
  }

  /**
   * Tests the cascading eviction with the first tier filled and the second tier empty resulting in
   * no eviction.
   *
   * @throws Exception if the caching fails
   */
  @Test
  public void cascadingEvictionTest1() throws Exception {
    // First Tier 2000, 3000
    // Second Tier 10000, 20000, 30000
    int blockSize = 1000;
    cache(SESSION_ID, 101, blockSize, 0, 0);
    // block 102 in HIR cache in tier 0, dir 0
    cache(SESSION_ID, 102, blockSize, 0, 0);
    cache(SESSION_ID, 103, blockSize, 0, 1);
    cache(SESSION_ID, 104, blockSize, 0, 1);
    // block 105 in HIR cache in tier 0, dir 1
    cache(SESSION_ID, 105, blockSize, 0, 1);

    BlockStoreLocation anyDirInFirstTier = BlockStoreLocation.anyDirInTier("MEM");

    // 1. Evict HIR block 102, as it is the least recently referenced HIR block
    EvictionPlan plan = mEvictor.freeSpaceWithView(blockSize, anyDirInFirstTier, mManagerView);
    Assert.assertNotNull(plan);
    Assert.assertEquals(0, plan.toEvict().size());
    Assert.assertEquals(1, plan.toMove().size());
    long blockId = plan.toMove().get(0).getBlockId();
    Assert.assertEquals(102, blockId);
    // Block 102 is moved to LIR cache and block 101 is moved to HIR cache
    access(blockId);

    // 2. Evict HIR block 105, as it is the least recently referenced HIR block
    plan = mEvictor.freeSpaceWithView(blockSize, anyDirInFirstTier, mManagerView);
    Assert.assertNotNull(plan);
    Assert.assertEquals(0, plan.toEvict().size());
    Assert.assertEquals(1, plan.toMove().size());
    blockId = plan.toMove().get(0).getBlockId();
    Assert.assertEquals(105, blockId);
    // Block 105 is moved to LIR cache and block 103 is moved to HIR cache
    access(blockId);

    // 3. Evict HIR block 101, as it is the least recently referenced HIR block
    plan = mEvictor.freeSpaceWithView(blockSize, anyDirInFirstTier, mManagerView);
    Assert.assertNotNull(plan);
    Assert.assertEquals(0, plan.toEvict().size());
    Assert.assertEquals(1, plan.toMove().size());
    blockId = plan.toMove().get(0).getBlockId();
    Assert.assertEquals(101, blockId);
    access(blockId);

    // 4. Evict HIR block 103, as it is the least recently referenced HIR block
    plan = mEvictor.freeSpaceWithView(blockSize, anyDirInFirstTier, mManagerView);
    Assert.assertNotNull(plan);
    Assert.assertEquals(0, plan.toEvict().size());
    Assert.assertEquals(1, plan.toMove().size());
    blockId = plan.toMove().get(0).getBlockId();
    Assert.assertEquals(103, blockId);
    access(blockId);
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
    for (int tierOrdinal : TieredBlockStoreTestUtils.TIER_ORDINAL) {
      long[] tierCapacity = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[tierOrdinal];
      for (int dirIdx = 0; dirIdx < tierCapacity.length; dirIdx++) {
        cache(SESSION_ID, blockId, tierCapacity[dirIdx] / 2, tierOrdinal, dirIdx);
        blockId++;
        cache(SESSION_ID, blockId, tierCapacity[dirIdx] / 2, tierOrdinal, dirIdx);
        blockId++;
      }
    }

    BlockStoreLocation anyDirInFirstTier =
        BlockStoreLocation.anyDirInTier(TieredBlockStoreTestUtils.TIER_ALIAS[0]);
    int nDirInFirstTier = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[0].length;
    long smallestCapacity = TieredBlockStoreTestUtils.TIER_CAPACITY_BYTES[0][0] / 2;
    for (int i = 0; i < nDirInFirstTier; i++) {
      EvictionPlan plan =
          mEvictor.freeSpaceWithView(smallestCapacity, anyDirInFirstTier, mManagerView);
      Assert.assertTrue(EvictorTestUtils.validCascadingPlan(smallestCapacity, plan, mMetaManager));
      // least recently used HIR block in the first tier needs to be moved to the second tier
      Assert.assertEquals(1, plan.toMove().size());
      long blockIdMovedInFirstTier = plan.toMove().get(0).getBlockId();
      Assert.assertEquals(BLOCK_ID + 2 * i + 1, blockIdMovedInFirstTier);
      // least recently used HIR block in the second tier will be evicted to hold blocks moved
      // from first tier
      Assert.assertEquals(1, plan.toEvict().size());
      long blockIdEvictedInSecondTier = plan.toEvict().get(0).getFirst();
      Assert.assertEquals(BLOCK_ID + nDirInFirstTier * 2 + 2 * i + 1, blockIdEvictedInSecondTier);

      access(blockIdMovedInFirstTier);
      access(blockIdEvictedInSecondTier);
    }
  }
}
