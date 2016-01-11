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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableMap;

import tachyon.collections.Pair;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;

/**
 * Unit tests for {@link BlockStoreMeta}.
 */
public class BlockStoreMetaTest {
  private static final long TEST_SESSION_ID = 33L;
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
    StorageDir dir = mMetadataManager.getTier("MEM").getDir(0);
    for (long blockId = 0L; blockId < COMMITTED_BLOCKS_NUM; blockId ++) {
      TieredBlockStoreTestUtils.cache(TEST_SESSION_ID, blockId, TEST_BLOCK_SIZE, dir,
          mMetadataManager, null);
    }
    mBlockStoreMeta = new BlockStoreMeta(mMetadataManager);
  }

  @After
  public void after() {
    WorkerContext.reset();
  }

  @Test
  public void getBlockListTest() {
    Map<String, List<Long>> tierAliasToBlockIds = new HashMap<String, List<Long>>();
    for (StorageTier tier : mMetadataManager.getTiers()) {
      List<Long> blockIdsOnTier = new ArrayList<Long>();
      for (StorageDir dir : tier.getStorageDirs()) {
        blockIdsOnTier.addAll(dir.getBlockIds());
      }
      tierAliasToBlockIds.put(tier.getTierAlias(), blockIdsOnTier);
    }
    Map<String, List<Long>> actual = mBlockStoreMeta.getBlockList();
    Assert.assertEquals(TieredBlockStoreTestUtils.TIER_ALIAS.length, actual.keySet().size());
    Assert.assertEquals(tierAliasToBlockIds, actual);
  }

  @Test
  public void getCapacityBytesTest() {
    Assert.assertEquals(TieredBlockStoreTestUtils.getDefaultTotalCapacityBytes(),
        mBlockStoreMeta.getCapacityBytes());
  }

  @Test
  public void getCapacityBytesOnDirsTest() {
    Map<Pair<String, String>, Long> dirsToCapacityBytes = new HashMap<Pair<String, String>, Long>();
    for (StorageTier tier : mMetadataManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        dirsToCapacityBytes.put(new Pair<String, String>(tier.getTierAlias(), dir.getDirPath()),
            dir.getCapacityBytes());
      }
    }
    Assert.assertEquals(dirsToCapacityBytes, mBlockStoreMeta.getCapacityBytesOnDirs());
    Assert.assertEquals(TieredBlockStoreTestUtils.getDefaultDirNum(), mBlockStoreMeta
        .getCapacityBytesOnDirs().values().size());
  }

  @Test
  public void getCapacityBytesOnTiersTest() {
    Map<String, Long> expectedCapacityBytesOnTiers = ImmutableMap.of("MEM", 5000L, "SSD", 60000L);
    Assert.assertEquals(expectedCapacityBytesOnTiers, mBlockStoreMeta.getCapacityBytesOnTiers());
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
    Map<Pair<String, String>, Long> dirsToUsedBytes = new HashMap<Pair<String, String>, Long>();
    for (StorageTier tier : mMetadataManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        dirsToUsedBytes.put(new Pair<String, String>(tier.getTierAlias(), dir.getDirPath()),
            dir.getCapacityBytes() - dir.getAvailableBytes());
      }
    }
    Assert.assertEquals(dirsToUsedBytes, mBlockStoreMeta.getUsedBytesOnDirs());
  }

  @Test
  public void getUsedBytesOnTiersTest() {
    long usedBytes = TEST_BLOCK_SIZE * COMMITTED_BLOCKS_NUM;
    Map<String, Long> usedBytesOnTiers = ImmutableMap.of("MEM", usedBytes, "SSD", 0L);
    Assert.assertEquals(usedBytesOnTiers, mBlockStoreMeta.getUsedBytesOnTiers());
  }
}
