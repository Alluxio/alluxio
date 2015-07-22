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

import java.io.File;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.StorageDir;

/**
 * Sanity check on specific behavior of PartialLRUEvictor such as evicting/moving least recently
 * used blocks in StorageDir with max free space and cascading ParitialLRU eviction.
 */
public class PartialLRUEvictorTest {
  private static final long USER_ID = 2;
  private static final long BLOCK_ID = 10;

  private BlockMetadataManager mMetaManager;
  private BlockMetadataManagerView mManagerView;
  private Evictor mEvictor;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public final void before() throws Exception {
    File tempFolder = mTestFolder.newFolder();
    mMetaManager = EvictorTestUtils.defaultMetadataManager(tempFolder.getAbsolutePath());
    mManagerView =
        new BlockMetadataManagerView(mMetaManager, Collections.<Integer>emptySet(),
            Collections.<Long>emptySet());
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.WORKER_EVICT_STRATEGY, PartialLRUEvictor.class.getName());
    mEvictor = Evictor.Factory.createEvictor(conf, mManagerView);
  }

  private void cache(long userId, long blockId, long bytes, int tierLevel, int dirIdx)
      throws Exception {
    StorageDir dir = mMetaManager.getTiers().get(tierLevel).getDir(dirIdx);
    EvictorTestUtils.cache(userId, blockId, bytes, dir, mMetaManager, mEvictor);
  }

  @Test
  public void evictInBottomTierTest() throws Exception {
    int bottomTierLevel = EvictorTestUtils.TIER_LEVEL[EvictorTestUtils.TIER_LEVEL.length - 1];
    // capacity increases with index
    long[] bottomTierDirCapacity = EvictorTestUtils.TIER_CAPACITY[bottomTierLevel];
    long smallestCapacity = bottomTierDirCapacity[0];
    long delta = smallestCapacity / 10;
    int nDir = bottomTierDirCapacity.length;
    // free space of StorageDir increases with Dir index
    for (int i = 0; i < nDir; i ++) {
      cache(USER_ID, BLOCK_ID + i, bottomTierDirCapacity[i] - i * delta, bottomTierLevel, i);
    }

    BlockStoreLocation anyDirInBottomTier = BlockStoreLocation.anyDirInTier(bottomTierLevel + 1);
    // free the StorageDir with max free space
    EvictionPlan plan =
        mEvictor.freeSpaceWithView(smallestCapacity, anyDirInBottomTier, mManagerView);
    Assert.assertNotNull(plan);
    Assert.assertTrue(plan.toMove().isEmpty());
    Assert.assertEquals(1, plan.toEvict().size());
    long toEvictBlockId = plan.toEvict().get(0);
    Assert.assertEquals(BLOCK_ID + nDir - 1, toEvictBlockId);
  }

  @Test
  public void cascadingEvictionTest1() throws Exception {
    // Two tiers, each dir in the second tier has more space than any dir in the first tier. Fill in
    // the first tier, leave the second tier empty. Request space from the first tier, blocks should
    // be moved from the first to the second tier without eviction.
    int firstTierLevel = EvictorTestUtils.TIER_LEVEL[0];
    long[] firstTierDirCapacity = EvictorTestUtils.TIER_CAPACITY[0];
    long smallestCapacity = firstTierDirCapacity[0];
    long delta = smallestCapacity / 10;
    int nDir = firstTierDirCapacity.length;
    for (int i = 0; i < nDir; i ++) {
      cache(USER_ID, BLOCK_ID + i, firstTierDirCapacity[i] - delta * i, firstTierLevel, i);
    }
    BlockStoreLocation anyDirInFirstTier = BlockStoreLocation.anyDirInTier(firstTierLevel + 1);
    EvictionPlan plan =
        mEvictor.freeSpaceWithView(smallestCapacity, anyDirInFirstTier, mManagerView);
    Assert.assertTrue(EvictorTestUtils.validCascadingPlan(smallestCapacity, plan, mMetaManager));
    Assert.assertEquals(0, plan.toEvict().size());
    Assert.assertEquals(1, plan.toMove().size());
    long blockId = plan.toMove().get(0).getFirst();
    Assert.assertEquals(BLOCK_ID + nDir - 1, blockId);
  }

  @Test
  public void cascadingEvictionTest2() throws Exception {
    // Two tiers, the second tier has more dirs than the first tier and each dir in the second tier
    // has more space than any dir in the first tier. Fill in all dirs and request space from the
    // first tier, blocks should be moved from the first to the second tier, and some blocks in the
    // second tier should be evicted to hold blocks moved from the first tier.
    BlockStoreLocation anyDirInFirstTier =
        BlockStoreLocation.anyDirInTier(EvictorTestUtils.TIER_LEVEL[0] + 1);
    int nDirInFirstTier = EvictorTestUtils.TIER_CAPACITY[0].length;
    int nDirInSecondTier = EvictorTestUtils.TIER_CAPACITY[1].length;
    long smallestCapacity = EvictorTestUtils.TIER_CAPACITY[0][0];
    long delta = smallestCapacity / 10;
    long blockId = BLOCK_ID;

    for (int tierLevel : EvictorTestUtils.TIER_LEVEL) {
      long[] tierCapacity = EvictorTestUtils.TIER_CAPACITY[tierLevel];
      for (int dirIdx = 0; dirIdx < tierCapacity.length; dirIdx ++) {
        cache(USER_ID, blockId, tierCapacity[dirIdx] - dirIdx * delta, tierLevel, dirIdx);
        blockId ++;
      }
    }

    EvictionPlan plan =
        mEvictor.freeSpaceWithView(smallestCapacity, anyDirInFirstTier, mManagerView);
    Assert.assertTrue(EvictorTestUtils.validCascadingPlan(smallestCapacity, plan, mMetaManager));
    // block in StorageDir with max free space in the first tier needs to be moved to the second
    // tier
    Assert.assertEquals(1, plan.toMove().size());
    long blockIdMovedInFirstTier = plan.toMove().get(0).getFirst();
    Assert.assertEquals(BLOCK_ID + nDirInFirstTier - 1, blockIdMovedInFirstTier);
    // block in StorageDir with max free space in the second tier will be evicted to hold blocks
    // moved from first tier
    Assert.assertEquals(1, plan.toEvict().size());
    long blockIdEvictedInSecondTier = plan.toEvict().get(0);
    Assert.assertEquals(BLOCK_ID + nDirInFirstTier + nDirInSecondTier - 1,
        blockIdEvictedInSecondTier);
  }
}