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

import java.io.IOException;
import java.util.HashSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import tachyon.worker.WorkerContext;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.TieredBlockStoreTestUtils;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * Base class for allocator tests.
 */
public class BaseAllocatorTest {

  protected static final long SESSION_ID = 1;
  protected int mTestBlockId = 0;

  // Default tier/dir configurations we use for testing
  public static final long DEFAULT_RAM_SIZE = 1000;
  public static final long DEFAULT_SSD_SIZE = 2000;
  public static final long DEFAULT_HDD_SIZE = 3000;

  public static final int[] TIER_LEVEL = {0, 1, 2};
  public static final String[] TIER_ALIAS = {"MEM", "SSD", "HDD"};
  public static final String[][] TIER_PATH = {{"/ramdisk"}, {"/ssd1", "/ssd2"},
      {"/disk1", "/disk2", "/disk3"}};
  public static final long[][] TIER_CAPACITY_BYTES = {{DEFAULT_RAM_SIZE},
      {DEFAULT_SSD_SIZE, DEFAULT_SSD_SIZE},
      {DEFAULT_HDD_SIZE, DEFAULT_HDD_SIZE, DEFAULT_HDD_SIZE}};

  public static final int DEFAULT_RAM_NUM  = TIER_PATH[0].length;
  public static final int DEFAULT_SSD_NUM  = TIER_PATH[1].length;
  public static final int DEFAULT_HDD_NUM  = TIER_PATH[2].length;

  protected BlockMetadataManager mManager = null;
  protected Allocator mAllocator = null;

  protected BlockStoreLocation mAnyTierLoc = BlockStoreLocation.anyTier();
  protected BlockStoreLocation mAnyDirInTierLoc1 = BlockStoreLocation.anyDirInTier("MEM");
  protected BlockStoreLocation mAnyDirInTierLoc2 = BlockStoreLocation.anyDirInTier("SSD");
  protected BlockStoreLocation mAnyDirInTierLoc3 = BlockStoreLocation.anyDirInTier("HDD");

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Sets up all dependencies before a test runs.
   *
   * @throws Exception if setting up the dependencies fails
   */
  @Before
  public void before() throws Exception {
    resetManagerView();
  }

  /**
   * Resets the context of the worker after a test ran.
   */
  @After
  public void after() {
    WorkerContext.reset();
  }

  protected void resetManagerView() throws Exception {
    String tachyonHome = mTestFolder.newFolder().getAbsolutePath();
    TieredBlockStoreTestUtils.setupTachyonConfWithMultiTier(tachyonHome, TIER_LEVEL, TIER_ALIAS,
        TIER_PATH, TIER_CAPACITY_BYTES, null);
    mManager = BlockMetadataManager.newBlockMetadataManager();
  }

  /**
   * Given an allocator with the location and blockSize, we assert whether the block can be
   * allocated
   */
  protected void assertTempBlockMeta(Allocator allocator, BlockStoreLocation location,
      long blockSize, boolean avail) throws IOException {

    mTestBlockId ++;
    StorageDirView dirView =
        allocator.allocateBlockWithView(SESSION_ID, blockSize, location, getManagerView());
    TempBlockMeta tempBlockMeta =
        dirView == null ? null : dirView.createTempBlockMeta(SESSION_ID, mTestBlockId, blockSize);

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
  protected void assertTempBlockMeta(Allocator allocator, BlockStoreLocation location,
      int blockSize, boolean avail, String tierAlias, int dirIndex) throws Exception {

    mTestBlockId ++;

    StorageDirView dirView =
        allocator.allocateBlockWithView(SESSION_ID, blockSize, location, getManagerView());
    TempBlockMeta tempBlockMeta =
        dirView == null ? null : dirView.createTempBlockMeta(SESSION_ID, mTestBlockId, blockSize);

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

  protected BlockMetadataManagerView getManagerView() {
    return new BlockMetadataManagerView(mManager, new HashSet<Long>(), new HashSet<Long>());
  }
}
