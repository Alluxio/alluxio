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

import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;

/**
 * Unit tests for tachyon.worker.block.BlockStoreMeta
 */
public class BlockStoreMetaTest {
  private static final long TEST_USER_ID = 33L;
  /** block size in Bytes for test */
  private static final long TEST_BLOCK_SIZE = 200L;
  /** num of total committed blocks */
  private static final long COMMITTED_BLOCKS_NUM = 10L;

  private BlockMetadataManager mMetadataManager;
  private BlockStoreMeta mBlockStoreMeta;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    String tachyonHome = mTestFolder.newFolder().getAbsolutePath();
    mMetadataManager = TieredBlockStoreTestUtils.defaultMetadataManager(tachyonHome);

    // Add and commit COMMITTED_BLOCKS_NUM temp blocks repeatedly
    StorageDir dir = mMetadataManager.getTier(1).getDir(0);
    for (long blockId = 0L; blockId < COMMITTED_BLOCKS_NUM; blockId ++) {
      TieredBlockStoreTestUtils.cache(TEST_USER_ID, blockId, TEST_BLOCK_SIZE, dir,
          mMetadataManager, null);
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
    Assert.assertEquals(TieredBlockStoreTestUtils.getDefaultDirNum(), actual.keySet().size());
    Assert.assertEquals(dirIdToBlockIds, actual);
  }

  @Test
  public void getCapacityBytesTest() {
    Assert.assertEquals(TieredBlockStoreTestUtils.getDefaultTotalCapacityBytes(),
        mBlockStoreMeta.getCapacityBytes());
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
    Assert.assertEquals(TieredBlockStoreTestUtils.getDefaultDirNum(), mBlockStoreMeta
        .getCapacityBytesOnDirs().values().size());
  }

  @Test
  public void getCapacityBytesOnTiersTest() {
    List<Long> expectedCapacityBytesOnTiers = new ArrayList<Long>();

    expectedCapacityBytesOnTiers.add(5000L);  // MEM
    expectedCapacityBytesOnTiers.add(60000L); // SSD
    expectedCapacityBytesOnTiers.add(0L);     // HDD
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
