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

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockMetadataManagerView;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageDirView;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.TempBlockMeta;

public class BaseAllocatorTest {

  protected static final long SESSION_ID = 1;
  protected int mTestBlockId = 0;

  // Default tier/dir configurations we use for testing
  protected static final int DEFAULT_WORKER_MAX_TIERED_STORAGE_LEVEL = 3;

  protected static final int DEFAULT_RAM_SIZE = 1000;
  protected static final int DEFAULT_RAM_NUM  = 1;

  protected static final int DEFAULT_SSD_SIZE = 2000;
  protected static final int DEFAULT_SSD_NUM  = 2;

  protected static final int DEFAULT_HDD_SIZE = 3000;
  protected static final int DEFAULT_HDD_NUM  = 3;

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
    //TODO: we will probably want to set WorkerContext.tachyonConf
    TachyonConf tachyonConf = WorkerContext.getConf();
    tachyonConf.merge(createTestTachyonConf());
    mManagerView = new BlockMetadataManagerView(
        BlockMetadataManager.newBlockMetadataManager(),
        new HashSet<Long>(), new HashSet<Long>());
  }

  protected TachyonConf createTestTachyonConf(
      int nram, int ramsize,
      int nssd, int ssdsize,
      int nhdd, int hddsize) throws IOException {

    String tachyonHome =
        File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.currentTimeMillis();

    TachyonConf tachyonConf = new TachyonConf();
    tachyonConf.set(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL,
        DEFAULT_WORKER_MAX_TIERED_STORAGE_LEVEL + "");

    tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT, 0), "MEM");
    tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, 0),
        generateDirsStr(nram, tachyonHome + "/ramdisk"));
    tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, 0),
        generateSizeStr(nram, ramsize));

    tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT, 1), "SSD");
    tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, 1),
        generateDirsStr(nssd, tachyonHome + "/ssd"));
    tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, 1),
        generateSizeStr(nssd, ssdsize));

    tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT, 2), "HDD");
    tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, 2),
        generateDirsStr(nhdd, tachyonHome + "/hdd"));
    tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, 2),
        generateSizeStr(nhdd, hddsize));
    return tachyonConf;
  }


  /**
   * Use the default configuration
   * @throws IOException
   */
  protected TachyonConf createTestTachyonConf() throws IOException {
    return createTestTachyonConf(
        DEFAULT_RAM_NUM, DEFAULT_RAM_SIZE,
        DEFAULT_SSD_NUM, DEFAULT_SSD_SIZE,
        DEFAULT_HDD_NUM, DEFAULT_HDD_SIZE
        );
  }

  /**
   * Generate a string consisting of multiple dirs used as the configuration value
   */
  protected String generateDirsStr(int num, String dirBase) {
    String res = "";
    for (int i = 1; i < num + 1; i ++) {
      res += (dirBase + i + ",");
    }
    return res.substring(0, res.length() - 1); //remove the last comma
  }

  protected String generateSizeStr(int num, int size) {
    String res = "";
    for (int i = 0; i < num; i ++) {
      res += (size + ",");
    }
    return res.substring(0, res.length() - 1); //remove the last comma
  }

  /**
   * Given an allocator with the location and blockSize,
   * we assert whether the block can be allocated
   */
  protected void assertTempBlockMeta(Allocator allocator, BlockStoreLocation location,
      int blockSize, boolean avail) throws IOException {

    mTestBlockId ++;

    StorageDirView dirView =
        allocator.allocateBlockWithView(SESSION_ID, blockSize, location, mManagerView);
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
  protected void assertTempBlockMeta(Allocator allocator,
      BlockStoreLocation location, int blockSize,
      boolean avail, int tierAlias, int dirIndex)
      throws Exception {

    mTestBlockId ++;

    StorageDirView dirView =
        allocator.allocateBlockWithView(SESSION_ID, blockSize, location, mManagerView);
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
}
