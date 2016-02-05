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

package alluxio.worker.block;

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

import alluxio.collections.Pair;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageTier;

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

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Sets up all dependencies before a test runs.
   *
   * @throws Exception if setting up the meta manager, the lock manager or the evictor fails
   */
  @Before
  public void before() throws Exception {
    String alluxioHome = mTestFolder.newFolder().getAbsolutePath();
    mMetadataManager = TieredBlockStoreTestUtils.defaultMetadataManager(alluxioHome);

    // Add and commit COMMITTED_BLOCKS_NUM temp blocks repeatedly
    StorageDir dir = mMetadataManager.getTier("MEM").getDir(0);
    for (long blockId = 0L; blockId < COMMITTED_BLOCKS_NUM; blockId ++) {
      TieredBlockStoreTestUtils.cache(TEST_SESSION_ID, blockId, TEST_BLOCK_SIZE, dir,
          mMetadataManager, null);
    }
    mBlockStoreMeta = new BlockStoreMeta(mMetadataManager);
  }

  /**
   * Resets the context of the worker after a test ran.
   */
  @After
  public void after() {
    WorkerContext.reset();
  }

  /**
   * Tests the {@link BlockStoreMeta#getBlockList()} method.
   */
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

  /**
   * Tests the {@link BlockStoreMeta#getCapacityBytes()} method.
   */
  @Test
  public void getCapacityBytesTest() {
    Assert.assertEquals(TieredBlockStoreTestUtils.getDefaultTotalCapacityBytes(),
        mBlockStoreMeta.getCapacityBytes());
  }

  /**
   * Tests the {@link BlockStoreMeta#getCapacityBytes()} method.
   */
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

  /**
   * Tests the {@link BlockStoreMeta#getCapacityBytesOnTiers()} method.
   */
  @Test
  public void getCapacityBytesOnTiersTest() {
    Map<String, Long> expectedCapacityBytesOnTiers = ImmutableMap.of("MEM", 5000L, "SSD", 60000L);
    Assert.assertEquals(expectedCapacityBytesOnTiers, mBlockStoreMeta.getCapacityBytesOnTiers());
  }

  /**
   * Tests the {@link BlockStoreMeta#getNumberOfBlocks()} method.
   */
  @Test
  public void getNumberOfBlocksTest() {
    Assert.assertEquals(COMMITTED_BLOCKS_NUM, mBlockStoreMeta.getNumberOfBlocks());
  }

  /**
   * Tests the {@link BlockStoreMeta#getUsedBytes()} method.
   */
  @Test
  public void getUsedBytesTest() {
    long usedBytes = TEST_BLOCK_SIZE * COMMITTED_BLOCKS_NUM;
    Assert.assertEquals(usedBytes, mBlockStoreMeta.getUsedBytes());
  }

  /**
   * Tests the {@link BlockStoreMeta#getUsedBytesOnDirs()} method.
   */
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

  /**
   * Tests the {@link BlockStoreMeta#getUsedBytesOnTiers()} method.
   */
  @Test
  public void getUsedBytesOnTiersTest() {
    long usedBytes = TEST_BLOCK_SIZE * COMMITTED_BLOCKS_NUM;
    Map<String, Long> usedBytesOnTiers = ImmutableMap.of("MEM", usedBytes, "SSD", 0L);
    Assert.assertEquals(usedBytesOnTiers, mBlockStoreMeta.getUsedBytesOnTiers());
  }
}
