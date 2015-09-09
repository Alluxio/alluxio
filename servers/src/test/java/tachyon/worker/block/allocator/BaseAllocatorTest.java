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

package tachyon.worker.block.allocator;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.Before;

import tachyon.StorageLevelAlias;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.TieredBlockStoreTestUtils;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.TempBlockMeta;

public class BaseAllocatorTest {

  protected static final long USER_ID = 1;
  protected int mTestBlockId = 0;

  // Default tier/dir configurations we use for testing
  public static final long DEFAULT_RAM_SIZE = 1000;
  public static final long DEFAULT_SSD_SIZE = 2000;
  public static final long DEFAULT_HDD_SIZE = 3000;

  public static final int[] TIER_LEVEL = {0, 1, 2};
  public static final StorageLevelAlias[] TIER_ALIAS = { StorageLevelAlias.MEM,
      StorageLevelAlias.SSD, StorageLevelAlias.HDD };
  public static final String[][] TIER_PATH = { {"/ramdisk"}, {"/ssd1", "/ssd2"},
      {"/disk1", "/disk2", "/disk3"}};
  public static final long[][] TIER_CAPACITY_BYTES = { {DEFAULT_RAM_SIZE},
      {DEFAULT_SSD_SIZE, DEFAULT_SSD_SIZE},
      {DEFAULT_HDD_SIZE, DEFAULT_HDD_SIZE, DEFAULT_HDD_SIZE}};

  public static final int DEFAULT_RAM_NUM  = TIER_PATH[0].length;
  public static final int DEFAULT_SSD_NUM  = TIER_PATH[1].length;
  public static final int DEFAULT_HDD_NUM  = TIER_PATH[2].length;

  protected BlockMetadataManagerView mManagerView = null;
  protected Allocator mAllocator = null;

  protected BlockStoreLocation mAnyTierLoc = BlockStoreLocation.anyTier();
  protected BlockStoreLocation mAnyDirInTierLoc1 = BlockStoreLocation.anyDirInTier(1);
  protected BlockStoreLocation mAnyDirInTierLoc2 = BlockStoreLocation.anyDirInTier(2);
  protected BlockStoreLocation mAnyDirInTierLoc3 = BlockStoreLocation.anyDirInTier(3);

  @Before
  public void before() throws Exception {
    resetManagerView();
  }

  protected void resetManagerView() throws Exception {
    String tachyonHome =
        File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.currentTimeMillis();
    TieredBlockStoreTestUtils
     .setTachyonConf(tachyonHome, TIER_LEVEL, TIER_ALIAS, TIER_PATH, TIER_CAPACITY_BYTES, null);
    BlockMetadataManager metaManager = BlockMetadataManager.newBlockMetadataManager();
    mManagerView = new BlockMetadataManagerView(metaManager, new HashSet<Long>(),
        new HashSet<Long>());
  }

  /**
   * Given an allocator with the location and blockSize,
   * we assert whether the block can be allocated
   */
  protected void assertTempBlockMeta(Allocator allocator, BlockStoreLocation location,
      long blockSize, boolean avail) throws IOException {

    mTestBlockId ++;

    StorageDirView dirView =
        allocator.allocateBlockWithView(USER_ID, blockSize, location, mManagerView);
    TempBlockMeta tempBlockMeta =
        dirView == null ? null : dirView.createTempBlockMeta(USER_ID, mTestBlockId, blockSize);

    if (avail == false) {
      Assert.assertTrue(tempBlockMeta == null);
    } else {
      Assert.assertTrue(tempBlockMeta != null);
    }
  }

  /**
   * Given an allocator with the location and blockSize,
   * we assert the allocator should be able to
   * 1. @param avail: the block should be successfully allocated or not
   * 2. @param tierAlias: the block should be allocated at this tier
   * 3. @param dirIndex : the block should be allocated at this dir
   */
  protected void assertTempBlockMeta(Allocator allocator,
      BlockStoreLocation location, int blockSize,
      boolean avail, int tierAlias, int dirIndex)
      throws Exception {

    mTestBlockId ++;

    StorageDirView dirView =
        allocator.allocateBlockWithView(USER_ID, blockSize, location, mManagerView);
    TempBlockMeta tempBlockMeta =
        dirView == null ? null : dirView.createTempBlockMeta(USER_ID, mTestBlockId, blockSize);

    if (avail == false) {
      Assert.assertTrue(tempBlockMeta == null);
    } else {
      Assert.assertTrue(tempBlockMeta != null);

      StorageDir pDir = tempBlockMeta.getParentDir();
      StorageTier pTier = pDir.getParentTier();

      Assert.assertTrue(pDir.getDirIndex() == dirIndex);
      Assert.assertTrue(pTier.getTierAlias() == tierAlias);

      //update the dir meta info
      pDir.addBlockMeta(new BlockMeta(mTestBlockId, blockSize, pDir));
    }
  }
}
