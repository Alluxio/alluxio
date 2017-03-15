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

import alluxio.exception.BlockDoesNotExistException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.WorkerOutOfSpaceException;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link BlockMetadataManager}.
 */
public final class BlockMetadataManagerTest {
  private static final long TEST_SESSION_ID = 2;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_TEMP_BLOCK_ID = 10;
  private static final long TEST_TEMP_BLOCK_ID2 = TEST_TEMP_BLOCK_ID + 1;
  private static final long TEST_BLOCK_SIZE = 20;

  private static final int[] TIER_ORDINAL = {0, 1};
  private static final String[] TIER_ALIAS = {"MEM", "HDD"};
  private static final String[][] TIER_PATH = {{"/ramdisk"}, {"/disk1", "/disk2"}};
  private static final long[][] TIER_CAPACITY_BYTES = {{1000}, {3000, 5000}};

  private BlockMetadataManager mMetaManager;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    String baseDir = mFolder.newFolder().getAbsolutePath();
    TieredBlockStoreTestUtils.setupConfWithMultiTier(baseDir, TIER_ORDINAL, TIER_ALIAS,
        TIER_PATH, TIER_CAPACITY_BYTES, null);

    mMetaManager = BlockMetadataManager.createBlockMetadataManager();
  }

  /**
   * Tests the {@link BlockMetadataManager#getTier(String)} method.
   */
  @Test
  public void getTier() {
    StorageTier tier;
    tier = mMetaManager.getTier("MEM"); // MEM
    Assert.assertEquals("MEM", tier.getTierAlias());
    Assert.assertEquals(0, tier.getTierOrdinal());
    tier = mMetaManager.getTier("HDD"); // HDD
    Assert.assertEquals("HDD", tier.getTierAlias());
    Assert.assertEquals(1, tier.getTierOrdinal());
  }

  /**
   * Tests the {@link BlockMetadataManager#getDir(BlockStoreLocation)} method.
   */
  @Test
  public void getDir() {
    BlockStoreLocation loc;
    StorageDir dir;

    loc = new BlockStoreLocation("MEM", 0);
    dir = mMetaManager.getDir(loc);
    Assert.assertEquals(loc.tierAlias(), dir.getParentTier().getTierAlias());
    Assert.assertEquals(loc.dir(), dir.getDirIndex());

    loc = new BlockStoreLocation("HDD", 1);
    dir = mMetaManager.getDir(loc);
    Assert.assertEquals(loc.tierAlias(), dir.getParentTier().getTierAlias());
    Assert.assertEquals(loc.dir(), dir.getDirIndex());
  }

  /**
   * Tests that an exception is thrown in the {@link BlockMetadataManager#getTier(String)} method
   * when trying to retrieve a tier which does not exist.
   */
  @Test
  public void getTierNotExisting() {
    String badTierAlias = "SSD";
    mThrown.expect(IllegalArgumentException.class);
    mThrown.expectMessage(ExceptionMessage.TIER_ALIAS_NOT_FOUND.getMessage(badTierAlias));
    mMetaManager.getTier(badTierAlias);
  }

  /**
   * Tests the {@link BlockMetadataManager#getTiers()} method.
   */
  @Test
  public void getTiers() {
    List<StorageTier> tiers = mMetaManager.getTiers();
    Assert.assertEquals(2, tiers.size());
    Assert.assertEquals("MEM", tiers.get(0).getTierAlias());
    Assert.assertEquals(0, tiers.get(0).getTierOrdinal());
    Assert.assertEquals("HDD", tiers.get(1).getTierAlias());
    Assert.assertEquals(1, tiers.get(1).getTierOrdinal());
  }

  /**
   * Tests the {@link BlockMetadataManager#getTiersBelow(String)} method.
   */
  @Test
  public void getTiersBelow() {
    List<StorageTier> tiersBelow = mMetaManager.getTiersBelow("MEM");
    Assert.assertEquals(1, tiersBelow.size());
    Assert.assertEquals("HDD", tiersBelow.get(0).getTierAlias());
    Assert.assertEquals(1, tiersBelow.get(0).getTierOrdinal());

    tiersBelow = mMetaManager.getTiersBelow("HDD");
    Assert.assertEquals(0, tiersBelow.size());
  }

  /**
   * Tests the {@link BlockMetadataManager#getAvailableBytes(BlockStoreLocation)} method.
   */
  @Test
  public void getAvailableBytes() {
    Assert.assertEquals(9000, mMetaManager.getAvailableBytes(BlockStoreLocation.anyTier()));
    Assert.assertEquals(1000,
        mMetaManager.getAvailableBytes(BlockStoreLocation.anyDirInTier("MEM")));
    Assert.assertEquals(8000,
        mMetaManager.getAvailableBytes(BlockStoreLocation.anyDirInTier("HDD")));
    Assert.assertEquals(1000, mMetaManager.getAvailableBytes(new BlockStoreLocation("MEM", 0)));
    Assert.assertEquals(3000, mMetaManager.getAvailableBytes(new BlockStoreLocation("HDD", 0)));
    Assert.assertEquals(5000, mMetaManager.getAvailableBytes(new BlockStoreLocation("HDD", 1)));
  }

  /**
   * Tests the different operations for metadata of a block, such as adding a temporary block or
   * committing a block.
   */
  @Test
  public void blockMeta() throws Exception {
    StorageDir dir = mMetaManager.getTier("HDD").getDir(0);
    TempBlockMeta tempBlockMeta =
        new TempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, dir);

    // Empty storage
    Assert.assertFalse(mMetaManager.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    Assert.assertFalse(mMetaManager.hasBlockMeta(TEST_TEMP_BLOCK_ID));
    // Add temp block
    mMetaManager.addTempBlockMeta(tempBlockMeta);
    Assert.assertTrue(mMetaManager.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    Assert.assertFalse(mMetaManager.hasBlockMeta(TEST_TEMP_BLOCK_ID));
    // Get temp block
    Assert.assertEquals(tempBlockMeta, mMetaManager.getTempBlockMeta(TEST_TEMP_BLOCK_ID));
    // Abort temp block
    mMetaManager.abortTempBlockMeta(tempBlockMeta);
    Assert.assertFalse(mMetaManager.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    Assert.assertFalse(mMetaManager.hasBlockMeta(TEST_TEMP_BLOCK_ID));
    // Add temp block with previous block id
    mMetaManager.addTempBlockMeta(tempBlockMeta);
    Assert.assertTrue(mMetaManager.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    Assert.assertFalse(mMetaManager.hasBlockMeta(TEST_TEMP_BLOCK_ID));
    // Commit temp block
    mMetaManager.commitTempBlockMeta(tempBlockMeta);
    Assert.assertFalse(mMetaManager.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    Assert.assertTrue(mMetaManager.hasBlockMeta(TEST_TEMP_BLOCK_ID));
    // Get block
    BlockMeta blockMeta = mMetaManager.getBlockMeta(TEST_TEMP_BLOCK_ID);
    Assert.assertEquals(TEST_TEMP_BLOCK_ID, blockMeta.getBlockId());
    // Remove block
    mMetaManager.removeBlockMeta(blockMeta);
    Assert.assertFalse(mMetaManager.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    Assert.assertFalse(mMetaManager.hasBlockMeta(TEST_TEMP_BLOCK_ID));
  }

  /**
   * Tests that an exception is thrown in the {@link BlockMetadataManager#getBlockMeta(long)} method
   * when trying to retrieve metadata of a block which does not exist.
   */
  @Test
  public void getBlockMetaNotExisting() throws Exception {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(TEST_BLOCK_ID));
    mMetaManager.getBlockMeta(TEST_BLOCK_ID);
  }

  /**
   * Tests that an exception is thrown in the {@link BlockMetadataManager#getTempBlockMeta(long)}
   * method when trying to retrieve metadata of a temporary block which does not exist.
   */
  @Test
  public void getTempBlockMetaNotExisting() throws Exception {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown
        .expectMessage(ExceptionMessage.TEMP_BLOCK_META_NOT_FOUND.getMessage(TEST_TEMP_BLOCK_ID));
    mMetaManager.getTempBlockMeta(TEST_TEMP_BLOCK_ID);
  }

  /**
   * Dummy unit test, actually the case of move block meta to same dir should never happen.
   */
  @Test
  public void moveBlockMetaSameDir() throws Exception {
    // create and add two temp block metas with same tier and dir to the meta manager
    StorageDir dir = mMetaManager.getTier("MEM").getDir(0);
    TempBlockMeta tempBlockMeta1 =
        new TempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    TempBlockMeta tempBlockMeta2 =
        new TempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID2, TEST_BLOCK_SIZE, dir);
    mMetaManager.addTempBlockMeta(tempBlockMeta1);
    mMetaManager.addTempBlockMeta(tempBlockMeta2);

    // commit the first temp block meta
    mMetaManager.commitTempBlockMeta(tempBlockMeta1);
    BlockMeta blockMeta = mMetaManager.getBlockMeta(TEST_TEMP_BLOCK_ID);

    mMetaManager.moveBlockMeta(blockMeta, tempBlockMeta2);

    // test to make sure that the dst tempBlockMeta has been removed from the dir
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown
        .expectMessage(ExceptionMessage.TEMP_BLOCK_META_NOT_FOUND.getMessage(TEST_TEMP_BLOCK_ID2));
    mMetaManager.getTempBlockMeta(TEST_TEMP_BLOCK_ID2);
  }

  /**
   * Tests that an exception is thrown in the
   * {@link BlockMetadataManager#moveBlockMeta(BlockMeta, TempBlockMeta)} method when trying to move
   * a block to a not committed block meta.
   */
  @Test
  public void moveBlockMetaDiffDir() throws Exception {
    // create and add two temp block metas with different dirs in the same HDD tier
    StorageDir dir1 = mMetaManager.getTier("HDD").getDir(0);
    StorageDir dir2 = mMetaManager.getTier("HDD").getDir(1);
    TempBlockMeta tempBlockMeta1 =
        new TempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, dir1);
    TempBlockMeta tempBlockMeta2 =
        new TempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID2, TEST_BLOCK_SIZE, dir2);
    mMetaManager.addTempBlockMeta(tempBlockMeta1);
    mMetaManager.addTempBlockMeta(tempBlockMeta2);

    // commit the first temp block meta
    mMetaManager.commitTempBlockMeta(tempBlockMeta1);
    BlockMeta blockMeta = mMetaManager.getBlockMeta(TEST_TEMP_BLOCK_ID);

    mMetaManager.moveBlockMeta(blockMeta, tempBlockMeta2);

    // make sure that the dst tempBlockMeta has been removed from the dir2
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown
        .expectMessage(ExceptionMessage.TEMP_BLOCK_META_NOT_FOUND.getMessage(TEST_TEMP_BLOCK_ID2));
    mMetaManager.getTempBlockMeta(TEST_TEMP_BLOCK_ID2);
  }

  /**
   * Tests that an exception is thrown in the
   * {@link BlockMetadataManager#moveBlockMeta(BlockMeta, TempBlockMeta)} method when the worker is
   * out of space.
   */
  @Test
  public void moveBlockMetaOutOfSpaceException() throws Exception {
    // Create a committed block under dir2 with larger size than the capacity of dir1,
    // so that WorkerOutOfSpaceException should be thrown when move this block to dir1.

    StorageDir dir1 = mMetaManager.getTier("HDD").getDir(0);
    StorageDir dir2 = mMetaManager.getTier("HDD").getDir(1);
    long maxHddDir1Capacity = TIER_CAPACITY_BYTES[1][0];
    long blockMetaSize = maxHddDir1Capacity + 1;
    BlockMeta blockMeta = new BlockMeta(TEST_BLOCK_ID, blockMetaSize, dir2);
    TempBlockMeta tempBlockMeta2 =
        new TempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID2, TEST_BLOCK_SIZE, dir1);
    mMetaManager.addTempBlockMeta(tempBlockMeta2);
    dir2.addBlockMeta(blockMeta);

    mThrown.expect(WorkerOutOfSpaceException.class);
    mThrown.expectMessage(ExceptionMessage.NO_SPACE_FOR_BLOCK_META.getMessage(TEST_BLOCK_ID,
        blockMetaSize, maxHddDir1Capacity, TIER_ALIAS[1]));
    mMetaManager.moveBlockMeta(blockMeta, tempBlockMeta2);
  }

  /**
   * Tests the {@link BlockMetadataManager#moveBlockMeta(BlockMeta, BlockStoreLocation)} method.
   */
  @Test
  public void moveBlockMetaDeprecated() throws Exception {
    StorageDir dir = mMetaManager.getTier("MEM").getDir(0);
    TempBlockMeta tempBlockMeta =
        new TempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    mMetaManager.addTempBlockMeta(tempBlockMeta);
    mMetaManager.commitTempBlockMeta(tempBlockMeta);
    BlockMeta blockMeta = mMetaManager.getBlockMeta(TEST_TEMP_BLOCK_ID);

    // Move to anywhere
    mMetaManager.moveBlockMeta(blockMeta, BlockStoreLocation.anyTier());

    // Move to tier HDD tier
    blockMeta = mMetaManager.moveBlockMeta(blockMeta, BlockStoreLocation.anyDirInTier("HDD"));
    Assert.assertEquals("HDD", blockMeta.getBlockLocation().tierAlias());

    // Move to tier MEM and dir 0
    blockMeta = mMetaManager.moveBlockMeta(blockMeta, new BlockStoreLocation("MEM", 0));
    Assert.assertEquals("MEM", blockMeta.getBlockLocation().tierAlias());
    Assert.assertEquals(0, blockMeta.getBlockLocation().dir());
  }

  /**
   * Tests that an exception is thrown in the
   * {@link BlockMetadataManager#moveBlockMeta(BlockMeta, BlockStoreLocation)} method when the
   * capacity is exceeded.
   */
  @Test
  public void moveBlockMetaDeprecatedExceedCapacity() throws Exception {
    StorageDir dir = mMetaManager.getTier("HDD").getDir(0);
    BlockMeta blockMeta = new BlockMeta(TEST_BLOCK_ID, 2000, dir);
    dir.addBlockMeta(blockMeta);

    mThrown.expect(WorkerOutOfSpaceException.class);
    mThrown.expectMessage("does not have enough space");
    mMetaManager.moveBlockMeta(blockMeta, new BlockStoreLocation("MEM", 0));
  }

  /**
   * Tests the {@link BlockMetadataManager#resizeTempBlockMeta(TempBlockMeta, long)} method.
   */
  @Test
  public void resizeTempBlockMeta() throws Exception {
    StorageDir dir = mMetaManager.getTier("MEM").getDir(0);
    TempBlockMeta tempBlockMeta =
        new TempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    mMetaManager.resizeTempBlockMeta(tempBlockMeta, TEST_BLOCK_SIZE + 1);
    Assert.assertEquals(TEST_BLOCK_SIZE + 1, tempBlockMeta.getBlockSize());
  }

  /**
   * Tests the {@link BlockMetadataManager#cleanupSessionTempBlocks(long, List)} method.
   */
  @Test
  public void cleanupSession() throws Exception {
    StorageDir dir = mMetaManager.getTier("MEM").getDir(0);
    final long tempBlockId1 = 1;
    final long tempBlockId2 = 2;
    final long tempBlockId3 = 3;
    final long sessionId1 = 100;
    final long sessionId2 = 200;
    TempBlockMeta tempBlockMeta1 =
        new TempBlockMeta(sessionId1, tempBlockId1, TEST_BLOCK_SIZE, dir);
    TempBlockMeta tempBlockMeta2 =
        new TempBlockMeta(sessionId1, tempBlockId2, TEST_BLOCK_SIZE, dir);
    TempBlockMeta tempBlockMeta3 =
        new TempBlockMeta(sessionId2, tempBlockId3, TEST_BLOCK_SIZE, dir);
    BlockMeta blockMeta = new BlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    dir.addTempBlockMeta(tempBlockMeta1);
    dir.addTempBlockMeta(tempBlockMeta2);
    dir.addTempBlockMeta(tempBlockMeta3);
    dir.addBlockMeta(blockMeta);

    // Get temp blocks for sessionId1, expect to get tempBlock1 and tempBlock2
    List<TempBlockMeta> toRemove = mMetaManager.getSessionTempBlocks(sessionId1);
    List<Long> toRemoveBlockIds = new ArrayList<>(toRemove.size());
    for (TempBlockMeta tempBlockMeta : toRemove) {
      toRemoveBlockIds.add(tempBlockMeta.getBlockId());
    }
    Assert.assertEquals(Sets.newHashSet(tempBlockMeta1, tempBlockMeta2),
        new HashSet<>(toRemove));
    Assert.assertTrue(dir.hasTempBlockMeta(tempBlockId1));
    Assert.assertTrue(dir.hasTempBlockMeta(tempBlockId2));

    // Clean up sessionId1, expect tempBlock1 and tempBlock2 to be removed.
    mMetaManager.cleanupSessionTempBlocks(sessionId1, toRemoveBlockIds);
    Assert.assertFalse(dir.hasTempBlockMeta(tempBlockId1));
    Assert.assertFalse(dir.hasTempBlockMeta(tempBlockId2));
    Assert.assertTrue(dir.hasTempBlockMeta(tempBlockId3));
    Assert.assertTrue(dir.hasBlockMeta(TEST_BLOCK_ID));

    // Get temp blocks for sessionId1 again, expect to get nothing
    toRemove = mMetaManager.getSessionTempBlocks(sessionId1);
    toRemoveBlockIds = new ArrayList<>(toRemove.size());
    for (TempBlockMeta tempBlockMeta : toRemove) {
      toRemoveBlockIds.add(tempBlockMeta.getBlockId());
    }
    Assert.assertTrue(toRemove.isEmpty());

    // Clean up sessionId1 again, expect nothing to happen
    mMetaManager.cleanupSessionTempBlocks(sessionId1, toRemoveBlockIds);
    Assert.assertFalse(dir.hasTempBlockMeta(tempBlockId1));
    Assert.assertFalse(dir.hasTempBlockMeta(tempBlockId2));
    Assert.assertTrue(dir.hasTempBlockMeta(tempBlockId3));
    Assert.assertTrue(dir.hasBlockMeta(TEST_BLOCK_ID));

    // Get temp blocks for sessionId2, expect to get tempBlock3
    toRemove = mMetaManager.getSessionTempBlocks(sessionId2);
    toRemoveBlockIds = new ArrayList<>(toRemove.size());
    for (TempBlockMeta tempBlockMeta : toRemove) {
      toRemoveBlockIds.add(tempBlockMeta.getBlockId());
    }
    Assert.assertEquals(Sets.newHashSet(tempBlockMeta3), new HashSet<>(toRemove));
    Assert.assertTrue(dir.hasTempBlockMeta(tempBlockId3));

    // Clean up sessionId2, expect tempBlock3 to be removed
    mMetaManager.cleanupSessionTempBlocks(sessionId2, toRemoveBlockIds);
    Assert.assertFalse(dir.hasTempBlockMeta(tempBlockId1));
    Assert.assertFalse(dir.hasTempBlockMeta(tempBlockId2));
    Assert.assertFalse(dir.hasTempBlockMeta(tempBlockId3));
    Assert.assertTrue(dir.hasBlockMeta(TEST_BLOCK_ID));
  }

  /**
   * Tests the {@link BlockMetadataManager#getBlockMeta(long)} method.
   */
  @Test
  public void getBlockStoreMeta() {
    BlockStoreMeta meta = mMetaManager.getBlockStoreMeta();
    Assert.assertNotNull(meta);

    // Assert the capacities are at alias level [MEM: 1000][SSD: 0][HDD: 8000]
    Map<String, Long> exceptedCapacityBytesOnTiers = ImmutableMap.of("MEM", 1000L, "HDD", 8000L);
    Map<String, Long> exceptedUsedBytesOnTiers = ImmutableMap.of("MEM", 0L, "HDD", 0L);
    Assert.assertEquals(exceptedCapacityBytesOnTiers, meta.getCapacityBytesOnTiers());
    Assert.assertEquals(exceptedUsedBytesOnTiers, meta.getUsedBytesOnTiers());
  }
}
