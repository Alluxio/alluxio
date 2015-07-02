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

import org.junit.Assert;
import org.junit.Before;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.block.BlockMetadataManager;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.TempBlockMeta;

public class BaseAllocatorTest {
  
  protected BlockMetadataManager mMetaManager = null;
  protected Allocator mAllocator = null;
  
  protected BlockStoreLocation mAnyTierLoc = BlockStoreLocation.anyTier();
  protected BlockStoreLocation mAnyDirInTierLoc1 = BlockStoreLocation.anyDirInTier(1);
  protected BlockStoreLocation mAnyDirInTierLoc2 = BlockStoreLocation.anyDirInTier(2);
  protected BlockStoreLocation mAnyDirInTierLoc3 = BlockStoreLocation.anyDirInTier(3);
  
  protected static final long USER_ID = 1;
  protected int mTestBlockId = 0;
  
  @Before
  public final void before() throws IOException {
    mMetaManager = BlockMetadataManager.newBlockMetadataManager(createTestTachyonConf());
  }
  
  /**
   * The test configuration we generate is:
   * 1 DRAM: 1000
   * 2 SSD:  2000 * 2
   * 3 HDD:  3000 * 3   
   * @return
   * @throws IOException
   */
  protected TachyonConf createTestTachyonConf() throws IOException {
    String tachyonHome =
        File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.currentTimeMillis();
    
    TachyonConf tachyonConf = new TachyonConf();
    tachyonConf.set(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, "3");
      
    tachyonConf.set("tachyon.worker.tieredstore.level0.alias", "MEM");
    tachyonConf.set("tachyon.worker.tieredstore.level0.dirs.path", 
        tachyonHome + "/ramdisk");
    tachyonConf.set("tachyon.worker.tieredstore.level0.dirs.quota", 
        1000 + "");
      
    tachyonConf.set("tachyon.worker.tieredstore.level1.alias", "SSD");
    tachyonConf.set("tachyon.worker.tieredstore.level1.dirs.path", 
        tachyonHome + "/ssd1," 
        + tachyonHome + "/ssd2");
    tachyonConf.set("tachyon.worker.tieredstore.level1.dirs.quota", 
        2000 + "," 
        + 2000);
      
    tachyonConf.set("tachyon.worker.tieredstore.level2.alias", "HDD");
    tachyonConf.set("tachyon.worker.tieredstore.level2.dirs.path", 
        tachyonHome + "/disk1," 
        + tachyonHome + "/disk2," 
        + tachyonHome + "/disk3");
    tachyonConf.set("tachyon.worker.tieredstore.level2.dirs.quota", 
        3000 + "," 
        + 3000 + "," 
        + 3000);
    return tachyonConf;
  }
  
  protected void assertTempBlockMeta(Allocator allocator, 
      BlockStoreLocation location, int blockSize,
      boolean avail, int tierIndex, int dirIndex) 
      throws IOException {
    
    mTestBlockId ++;
    
    TempBlockMeta tempBlockMeta = 
        allocator.allocateBlock(USER_ID, mTestBlockId, blockSize, location);
    
    if (avail == false) { 
      Assert.assertTrue(tempBlockMeta == null);
    } else {
      Assert.assertTrue(tempBlockMeta != null);

      StorageDir pDir = tempBlockMeta.getParentDir();
      StorageTier pTier = pDir.getParentTier();
      
      System.out.println("tier: " + pTier.getTierAlias() + " | dir:  " + pDir.getDirIndex());
      
      Assert.assertTrue(pDir.getDirIndex() == dirIndex);
      Assert.assertTrue(pTier.getTierAlias() == tierIndex);
      
      //update the dir meta info
      pDir.addBlockMeta(new BlockMeta(mTestBlockId, blockSize, pDir));
    }
  }
}
