/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block;

import alluxio.Constants;
import alluxio.collections.Pair;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageTier;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link BlockStoreMeta}.
 */
public final class BlockStoreMetaTest {
  private static final long TEST_SESSION_ID = 33L;
  /** block size in Bytes for test. */
  private static final long TEST_BLOCK_SIZE = 200L;
  /** num of total committed blocks. */
  private static final long COMMITTED_BLOCKS_NUM = 10L;

  private BlockMetadataManager mMetadataManager;
  private BlockStoreMeta mBlockStoreMeta;
  private BlockStoreMeta mBlockStoreMetaFull;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    String alluxioHome = mTestFolder.newFolder().getAbsolutePath();
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_ENABLED, "false");
    ServerConfiguration.set(PropertyKey.WORKER_MANAGEMENT_TIER_PROMOTE_ENABLED, "false");
    mMetadataManager = TieredBlockStoreTestUtils.defaultMetadataManager(alluxioHome);

    // Add and commit COMMITTED_BLOCKS_NUM temp blocks repeatedly
    StorageDir dir = mMetadataManager.getTier(Constants.MEDIUM_MEM).getDir(0);
    for (long blockId = 0L; blockId < COMMITTED_BLOCKS_NUM; blockId++) {
      TieredBlockStoreTestUtils.cache(TEST_SESSION_ID, blockId, TEST_BLOCK_SIZE, dir,
          mMetadataManager, null);
    }
    mBlockStoreMeta = new DefaultBlockStoreMeta(mMetadataManager, false);
    mBlockStoreMetaFull = new DefaultBlockStoreMeta(mMetadataManager, true);
  }

  /**
   * Tests the {@link BlockStoreMeta#getBlockList()} method.
   */
  @Test
  public void getBlockList() {
    Map<String, List<Long>> tierAliasToBlockIds = new HashMap<>();
    for (StorageTier tier : mMetadataManager.getTiers()) {
      List<Long> blockIdsOnTier = new ArrayList<>();
      for (StorageDir dir : tier.getStorageDirs()) {
        blockIdsOnTier.addAll(dir.getBlockIds());
      }
      tierAliasToBlockIds.put(tier.getTierAlias(), blockIdsOnTier);
    }
    Map<String, List<Long>> actual = mBlockStoreMetaFull.getBlockList();
    Assert.assertEquals(TieredBlockStoreTestUtils.TIER_ALIAS.length, actual.keySet().size());
    Assert.assertEquals(tierAliasToBlockIds, actual);
  }

  /**
   * Tests the {@link BlockStoreMeta#getCapacityBytes()} method.
   */
  @Test
  public void getCapacityBytes() {
    Assert.assertEquals(TieredBlockStoreTestUtils.getDefaultTotalCapacityBytes(),
        mBlockStoreMeta.getCapacityBytes());
  }

  /**
   * Tests the {@link BlockStoreMeta#getCapacityBytes()} method.
   */
  @Test
  public void getCapacityBytesOnDirs() {
    Map<Pair<String, String>, Long> dirsToCapacityBytes = new HashMap<>();
    for (StorageTier tier : mMetadataManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        dirsToCapacityBytes.put(new Pair<>(tier.getTierAlias(), dir.getDirPath()),
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
  public void getCapacityBytesOnTiers() {
    Map<String, Long> expectedCapacityBytesOnTiers =
        ImmutableMap.of(Constants.MEDIUM_MEM, 5000L, Constants.MEDIUM_SSD, 60000L);
    Assert.assertEquals(expectedCapacityBytesOnTiers, mBlockStoreMeta.getCapacityBytesOnTiers());
  }

  /**
   * Tests the {@link BlockStoreMeta#getNumberOfBlocks()} method.
   */
  @Test
  public void getNumberOfBlocks() {
    Assert.assertEquals(COMMITTED_BLOCKS_NUM, mBlockStoreMetaFull.getNumberOfBlocks());
  }

  /**
   * Tests the {@link BlockStoreMeta#getUsedBytes()} method.
   */
  @Test
  public void getUsedBytes() {
    long usedBytes = TEST_BLOCK_SIZE * COMMITTED_BLOCKS_NUM;
    Assert.assertEquals(usedBytes, mBlockStoreMeta.getUsedBytes());
  }

  /**
   * Tests the {@link BlockStoreMeta#getUsedBytesOnDirs()} method.
   */
  @Test
  public void getUsedBytesOnDirs() {
    Map<Pair<String, String>, Long> dirsToUsedBytes = new HashMap<>();
    for (StorageTier tier : mMetadataManager.getTiers()) {
      for (StorageDir dir : tier.getStorageDirs()) {
        dirsToUsedBytes.put(new Pair<>(tier.getTierAlias(), dir.getDirPath()),
            dir.getCapacityBytes() - dir.getAvailableBytes());
      }
    }
    Assert.assertEquals(dirsToUsedBytes, mBlockStoreMeta.getUsedBytesOnDirs());
  }

  /**
   * Tests the {@link BlockStoreMeta#getUsedBytesOnTiers()} method.
   */
  @Test
  public void getUsedBytesOnTiers() {
    long usedBytes = TEST_BLOCK_SIZE * COMMITTED_BLOCKS_NUM;
    Map<String, Long> usedBytesOnTiers =
        ImmutableMap.of(Constants.MEDIUM_MEM, usedBytes, Constants.MEDIUM_SSD, 0L);
    Assert.assertEquals(usedBytesOnTiers, mBlockStoreMeta.getUsedBytesOnTiers());
  }
}
