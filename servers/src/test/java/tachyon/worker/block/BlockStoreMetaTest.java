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

package tachyon.worker.block;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.io.Files;
import com.google.common.primitives.Ints;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.util.io.BufferUtils;
import tachyon.worker.block.io.BlockWriter;
import tachyon.worker.block.io.LocalFileBlockWriter;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.TempBlockMeta;

/**
 * Unit tests for tachyon.worker.block.BlockStoreMeta
 */
public class BlockStoreMetaTest {
  private static final long TEST_USER_ID = 33L;
  /** block size in Bytes for test */
  private static final long TEST_BLOCK_SIZE = 200L;
  /** num of dirs created for test in all different tiers */
  private static final long TEST_DIR_NUM = 3L;
  /** num of total committed blocks */
  private static final long COMMITTED_BLOCKS_NUM = 8L;
  
  /** quota setting for layers {MEM, SSD, HDD} respectively */
  private static final long MEM_QUOTA = 2000L;
  private static final long SSD_QUOTA = 0L;
  private static final long HDD_DISK1_QUOTA = 3000L;
  private static final long HDD_DISK2_QUOTA = 5000L;
  
  private BlockMetadataManager mMetadataManager;
  private BlockStoreMeta mBlockStoreMeta;
  
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();
  
  @Before
  public void before() throws Exception {
    TachyonConf tachyonConf = new TachyonConf();
    // Setup a two-tier storage: MEM:{1000}, HDD{3000, 5000}, 
    // where missed SSD tier's quota is zero
    String tachyonHome = mTestFolder.newFolder().getAbsolutePath();
    tachyonConf.set(Constants.TACHYON_HOME, tachyonHome);
    tachyonConf.set(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, "2");
    tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT, 0), "MEM");
    tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, 0),
        tachyonHome + "/ramdisk");
    tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, 0),
        MEM_QUOTA + "b");
    tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT, 1), "HDD");
    tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, 1),
        tachyonHome + "/disk1," + tachyonHome + "/disk2");
    tachyonConf.set(String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, 1),
        HDD_DISK1_QUOTA + "," + HDD_DISK2_QUOTA);
    
    mMetadataManager = BlockMetadataManager.newBlockMetadataManager(tachyonConf);
    
    // Add and commit COMMITTED_BLOCKS_NUM temp blocks repeatedly
    StorageDir dir = mMetadataManager.getTier(1).getDir(0);
    for (long blockId = 0L; blockId < COMMITTED_BLOCKS_NUM; blockId ++) {
      // prepare temp block
      TempBlockMeta tempBlockMeta = new TempBlockMeta(TEST_USER_ID, blockId, TEST_BLOCK_SIZE, dir);
      mMetadataManager.addTempBlockMeta(tempBlockMeta);

      // write data
      File tempFile = new File(tempBlockMeta.getPath());
      
      // create storage directory for user TEST_USER_ID at first loop
      if (blockId == 0L) {
        if (!tempFile.getParentFile().mkdir()) {
          throw new IOException(String.format(
              "Parent directory of %s can not be created for temp block", tempBlockMeta.getPath()));
        }
      }
      
      BlockWriter writer = new LocalFileBlockWriter(tempBlockMeta);
      writer.append(BufferUtils.getIncreasingByteBuffer(Ints.checkedCast(TEST_BLOCK_SIZE)));
      writer.close();

      // commit block
      Files.move(tempFile, new File(tempBlockMeta.getCommitPath()));
      mMetadataManager.commitTempBlockMeta(tempBlockMeta);
      
    }
    mBlockStoreMeta = new BlockStoreMeta(mMetadataManager);
  }
  
  @Test
  public void getBlockListTest() {
    Map<Long, List<Long>> dirIdToBlockIds = new HashMap<Long, List<Long>>();
    for (StorageTier tier : mMetadataManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        dirIdToBlockIds.put(dir.getStorageDirId(), dir.getBlockIds());
      }
    }
    Map<Long, List<Long>> actual = mBlockStoreMeta.getBlockList();
    Assert.assertEquals(TEST_DIR_NUM, actual.keySet().size());
    Assert.assertEquals(dirIdToBlockIds, actual);
  }
  
  @Test
  public void getCapacityBytesTest() {
    long expectedCapacityBytes = MEM_QUOTA + HDD_DISK1_QUOTA + HDD_DISK2_QUOTA;
    Assert.assertEquals(expectedCapacityBytes, mBlockStoreMeta.getCapacityBytes());
  }

  @Test
  public void getCapacityBytesOnDirsTest() {
    Map<Long, Long> dirsToCapacityBytes = new HashMap<Long, Long>();
    for (StorageTier tier : mMetadataManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        dirsToCapacityBytes.put(dir.getStorageDirId(), dir.getCapacityBytes());
      }
    }
    Assert.assertEquals(dirsToCapacityBytes, mBlockStoreMeta.getCapacityBytesOnDirs());
    Assert.assertEquals(TEST_DIR_NUM, mBlockStoreMeta.getCapacityBytesOnDirs().values().size());
    Assert.assertTrue(mBlockStoreMeta.getCapacityBytesOnDirs().values()
        .containsAll(Arrays.asList(MEM_QUOTA, HDD_DISK1_QUOTA, HDD_DISK2_QUOTA)));
  }
  
  @Test
  public void getCapacityBytesOnTiersTest() {
    List<Long> expectedCapacityBytesOnTiers = new ArrayList<Long>();
    expectedCapacityBytesOnTiers.add(MEM_QUOTA); // MEM
    expectedCapacityBytesOnTiers.add(SSD_QUOTA); // SSD(0 Bytes here)
    expectedCapacityBytesOnTiers.add(HDD_DISK1_QUOTA + HDD_DISK2_QUOTA); // HDD
    Assert.assertEquals(expectedCapacityBytesOnTiers, mBlockStoreMeta.getCapacityBytesOnTiers());
  }
  
  @Test
  public void getDirPathsTest() {
    Map<Long, String> dirToPaths = new HashMap<Long, String>();
    for (StorageTier tier : mMetadataManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        dirToPaths.put(dir.getStorageDirId(), dir.getDirPath());
      }
    }
    Assert.assertEquals(dirToPaths, mBlockStoreMeta.getDirPaths());
  }
  
  @Test
  public void getNumberOfBlocksTest() {
    Assert.assertEquals(COMMITTED_BLOCKS_NUM, mBlockStoreMeta.getNumberOfBlocks());
  }
  
  @Test
  public void getUsedBytesTest() {
    long usedBytes = TEST_BLOCK_SIZE * COMMITTED_BLOCKS_NUM;
    Assert.assertEquals(usedBytes, mBlockStoreMeta.getUsedBytes());
  }
  
  @Test
  public void getUsedBytesOnDirsTest() {
    Map<Long, Long> dirsToUsedBytes = new HashMap<Long, Long>();
    for (StorageTier tier : mMetadataManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        dirsToUsedBytes.put(dir.getStorageDirId(), 
            dir.getCapacityBytes() - dir.getAvailableBytes());
      }
    }
    Assert.assertEquals(dirsToUsedBytes, mBlockStoreMeta.getUsedBytesOnDirs());
  }
  
  @Test
  public void getUsedBytesOnTiersTest() {
    long usedBytes = TEST_BLOCK_SIZE * COMMITTED_BLOCKS_NUM;
    List<Long> usedBytesOnTiers = new ArrayList<Long>(Arrays.asList(usedBytes, 0L, 0L));
    Assert.assertEquals(usedBytesOnTiers, mBlockStoreMeta.getUsedBytesOnTiers());
  }
}
