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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.runtime.ResourceExhaustedRuntimeException;
import alluxio.worker.block.meta.BlockMeta;
import alluxio.worker.block.meta.DefaultBlockMeta;
import alluxio.worker.block.meta.DefaultTempBlockMeta;
import alluxio.worker.block.meta.StorageDir;
import alluxio.worker.block.meta.StorageTier;
import alluxio.worker.block.meta.TempBlockMeta;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

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
  private static final String[] TIER_ALIAS = {Constants.MEDIUM_MEM, Constants.MEDIUM_HDD};
  private static final String[][] TIER_PATH = {{"/ramdisk"}, {"/disk1", "/disk2"}};
  public static final String[][] TIER_MEDIA_TYPE =
      {{Constants.MEDIUM_MEM}, {Constants.MEDIUM_HDD, Constants.MEDIUM_HDD}};
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
    Configuration.set(PropertyKey.WORKER_MANAGEMENT_TIER_ALIGN_ENABLED, false);
    Configuration.set(PropertyKey.WORKER_MANAGEMENT_TIER_PROMOTE_ENABLED, false);
    TieredBlockStoreTestUtils.setupConfWithMultiTier(baseDir, TIER_ORDINAL, TIER_ALIAS,
        TIER_PATH, TIER_CAPACITY_BYTES, TIER_MEDIA_TYPE, null);

    mMetaManager = BlockMetadataManager.createBlockMetadataManager();
  }

  /**
   * Tests the {@link BlockMetadataManager#getTier(String)} method.
   */
  @Test
  public void getTier() {
    StorageTier tier;
    tier = mMetaManager.getTier(Constants.MEDIUM_MEM); // MEM
    assertEquals(Constants.MEDIUM_MEM, tier.getTierAlias());
    assertEquals(0, tier.getTierOrdinal());
    tier = mMetaManager.getTier(Constants.MEDIUM_HDD); // HDD
    assertEquals(Constants.MEDIUM_HDD, tier.getTierAlias());
    assertEquals(1, tier.getTierOrdinal());
  }

  /**
   * Tests the {@link BlockMetadataManager#getDir(BlockStoreLocation)} method.
   */
  @Test
  public void getDir() {
    BlockStoreLocation loc;
    StorageDir dir;

    loc = new BlockStoreLocation(Constants.MEDIUM_MEM, 0);
    dir = mMetaManager.getDir(loc);
    assertEquals(loc.tierAlias(), dir.getParentTier().getTierAlias());
    assertEquals(loc.dir(), dir.getDirIndex());

    loc = new BlockStoreLocation(Constants.MEDIUM_HDD, 1);
    dir = mMetaManager.getDir(loc);
    assertEquals(loc.tierAlias(), dir.getParentTier().getTierAlias());
    assertEquals(loc.dir(), dir.getDirIndex());
  }

  /**
   * Tests that an exception is thrown in the {@link BlockMetadataManager#getTier(String)} method
   * when trying to retrieve a tier which does not exist.
   */
  @Test
  public void getTierNotExisting() {
    String badTierAlias = Constants.MEDIUM_SSD;
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
    assertEquals(2, tiers.size());
    assertEquals(Constants.MEDIUM_MEM, tiers.get(0).getTierAlias());
    assertEquals(0, tiers.get(0).getTierOrdinal());
    assertEquals(Constants.MEDIUM_HDD, tiers.get(1).getTierAlias());
    assertEquals(1, tiers.get(1).getTierOrdinal());
  }

  /**
   * Tests the {@link BlockMetadataManager#getTiersBelow(String)} method.
   */
  @Test
  public void getTiersBelow() {
    List<StorageTier> tiersBelow = mMetaManager.getTiersBelow(Constants.MEDIUM_MEM);
    assertEquals(1, tiersBelow.size());
    assertEquals(Constants.MEDIUM_HDD, tiersBelow.get(0).getTierAlias());
    assertEquals(1, tiersBelow.get(0).getTierOrdinal());

    tiersBelow = mMetaManager.getTiersBelow(Constants.MEDIUM_HDD);
    assertEquals(0, tiersBelow.size());
  }

  /**
   * Tests the {@link BlockMetadataManager#getAvailableBytes(BlockStoreLocation)} method.
   */
  @Test
  public void getAvailableBytes() {
    assertEquals(9000, mMetaManager.getAvailableBytes(BlockStoreLocation.anyTier()));
    assertEquals(1000,
        mMetaManager.getAvailableBytes(BlockStoreLocation.anyDirInTier(Constants.MEDIUM_MEM)));
    assertEquals(8000,
        mMetaManager.getAvailableBytes(BlockStoreLocation.anyDirInTier(Constants.MEDIUM_HDD)));
    assertEquals(1000,
        mMetaManager.getAvailableBytes(new BlockStoreLocation(Constants.MEDIUM_MEM, 0)));
    assertEquals(3000,
        mMetaManager.getAvailableBytes(new BlockStoreLocation(Constants.MEDIUM_HDD, 0)));
    assertEquals(5000,
        mMetaManager.getAvailableBytes(new BlockStoreLocation(Constants.MEDIUM_HDD, 1)));
  }

  /**
   * Tests the different operations for metadata of a block, such as adding a temporary block or
   * committing a block.
   */
  @Test
  public void blockMeta() throws Exception {
    StorageDir dir = mMetaManager.getTier(Constants.MEDIUM_HDD).getDir(0);
    TempBlockMeta tempBlockMeta =
        new DefaultTempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, dir);

    // Empty storage
    assertFalse(mMetaManager.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    assertFalse(mMetaManager.hasBlockMeta(TEST_TEMP_BLOCK_ID));
    // Add temp block
    mMetaManager.addTempBlockMeta(tempBlockMeta);
    assertTrue(mMetaManager.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    assertFalse(mMetaManager.hasBlockMeta(TEST_TEMP_BLOCK_ID));
    // Get temp block
    assertEquals(tempBlockMeta, mMetaManager.getTempBlockMeta(TEST_TEMP_BLOCK_ID).get());
    // Abort temp block
    mMetaManager.abortTempBlockMeta(tempBlockMeta);
    assertFalse(mMetaManager.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    assertFalse(mMetaManager.hasBlockMeta(TEST_TEMP_BLOCK_ID));
    // Add temp block with previous block id
    mMetaManager.addTempBlockMeta(tempBlockMeta);
    assertTrue(mMetaManager.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    assertFalse(mMetaManager.hasBlockMeta(TEST_TEMP_BLOCK_ID));
    // Commit temp block
    mMetaManager.commitTempBlockMeta(tempBlockMeta);
    assertFalse(mMetaManager.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    assertTrue(mMetaManager.hasBlockMeta(TEST_TEMP_BLOCK_ID));
    // Get block
    BlockMeta blockMeta = mMetaManager.getBlockMeta(TEST_TEMP_BLOCK_ID).get();
    assertEquals(TEST_TEMP_BLOCK_ID, blockMeta.getBlockId());
    // Remove block
    mMetaManager.removeBlockMeta(blockMeta);
    assertFalse(mMetaManager.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    assertFalse(mMetaManager.hasBlockMeta(TEST_TEMP_BLOCK_ID));
  }

  /**
   * Tests that the {@link BlockMetadataManager#getBlockMeta(long)} method returns
   * Optional.empty() when trying to retrieve metadata of a block which does not exist.
   */
  @Test
  public void getBlockMetaNotExisting() {
    assertFalse(mMetaManager.getBlockMeta(TEST_BLOCK_ID).isPresent());
  }

  /**
   * Tests that an exception is thrown in the {@link BlockMetadataManager#getTempBlockMeta(long)}
   * method when trying to retrieve metadata of a temporary block which does not exist.
   */
  @Test
  public void getTempBlockMetaNotExisting() {
    assertFalse(mMetaManager.getTempBlockMeta(TEST_TEMP_BLOCK_ID).isPresent());
  }

  /**
   * Dummy unit test, actually the case of move block meta to same dir should never happen.
   */
  @Test
  public void moveBlockMetaSameDir() throws Exception {
    // create and add two temp block metas with same tier and dir to the meta manager
    StorageDir dir = mMetaManager.getTier(Constants.MEDIUM_MEM).getDir(0);
    TempBlockMeta tempBlockMeta1 =
        new DefaultTempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    TempBlockMeta tempBlockMeta2 =
        new DefaultTempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID2, TEST_BLOCK_SIZE, dir);
    mMetaManager.addTempBlockMeta(tempBlockMeta1);
    mMetaManager.addTempBlockMeta(tempBlockMeta2);

    // commit the first temp block meta
    mMetaManager.commitTempBlockMeta(tempBlockMeta1);
    BlockMeta blockMeta = mMetaManager.getBlockMeta(TEST_TEMP_BLOCK_ID).get();

    mMetaManager.moveBlockMeta(blockMeta, tempBlockMeta2);

    // test to make sure that the dst tempBlockMeta has been removed from the dir
    assertFalse(mMetaManager.getTempBlockMeta(TEST_TEMP_BLOCK_ID2).isPresent());
  }

  /**
   * Tests that an exception is thrown in the
   * {@link BlockMetadataManager#moveBlockMeta(BlockMeta, TempBlockMeta)} method when trying to move
   * a block to a not committed block meta.
   */
  @Test
  public void moveBlockMetaDiffDir() throws Exception {
    // create and add two temp block metas with different dirs in the same HDD tier
    StorageDir dir1 = mMetaManager.getTier(Constants.MEDIUM_HDD).getDir(0);
    StorageDir dir2 = mMetaManager.getTier(Constants.MEDIUM_HDD).getDir(1);
    TempBlockMeta tempBlockMeta1 =
        new DefaultTempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, dir1);
    TempBlockMeta tempBlockMeta2 =
        new DefaultTempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID2, TEST_BLOCK_SIZE, dir2);
    mMetaManager.addTempBlockMeta(tempBlockMeta1);
    mMetaManager.addTempBlockMeta(tempBlockMeta2);

    // commit the first temp block meta
    mMetaManager.commitTempBlockMeta(tempBlockMeta1);
    BlockMeta blockMeta = mMetaManager.getBlockMeta(TEST_TEMP_BLOCK_ID).get();

    mMetaManager.moveBlockMeta(blockMeta, tempBlockMeta2);

    // make sure that the dst tempBlockMeta has been removed from the dir2
    assertFalse(mMetaManager.getTempBlockMeta(TEST_TEMP_BLOCK_ID2).isPresent());
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

    StorageDir dir1 = mMetaManager.getTier(Constants.MEDIUM_HDD).getDir(0);
    StorageDir dir2 = mMetaManager.getTier(Constants.MEDIUM_HDD).getDir(1);
    long maxHddDir1Capacity = TIER_CAPACITY_BYTES[1][0];
    long blockMetaSize = maxHddDir1Capacity + 1;
    BlockMeta blockMeta = new DefaultBlockMeta(TEST_BLOCK_ID, blockMetaSize, dir2);
    TempBlockMeta tempBlockMeta2 =
        new DefaultTempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID2, TEST_BLOCK_SIZE, dir1);
    mMetaManager.addTempBlockMeta(tempBlockMeta2);
    dir2.addBlockMeta(blockMeta);

    mThrown.expect(ResourceExhaustedRuntimeException.class);
    mThrown.expectMessage(ExceptionMessage.NO_SPACE_FOR_BLOCK_META.getMessage(TEST_BLOCK_ID,
        blockMetaSize, maxHddDir1Capacity, TIER_ALIAS[1]));
    mMetaManager.moveBlockMeta(blockMeta, tempBlockMeta2);
  }

  /**
   * Tests the {@link BlockMetadataManager#resizeTempBlockMeta(TempBlockMeta, long)} method.
   */
  @Test
  public void resizeTempBlockMeta() {
    StorageDir dir = mMetaManager.getTier(Constants.MEDIUM_MEM).getDir(0);
    TempBlockMeta tempBlockMeta =
        new DefaultTempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    mMetaManager.resizeTempBlockMeta(tempBlockMeta, TEST_BLOCK_SIZE + 1);
    assertEquals(TEST_BLOCK_SIZE + 1, tempBlockMeta.getBlockSize());
  }

  /**
   * Tests the {@link BlockMetadataManager#getBlockMeta(long)} method.
   */
  @Test
  public void getBlockStoreMeta() {
    BlockStoreMeta meta = mMetaManager.getBlockStoreMeta();
    Assert.assertNotNull(meta);

    // Assert the capacities are at alias level [MEM: 1000][SSD: 0][HDD: 8000]
    Map<String, Long> exceptedCapacityBytesOnTiers =
        ImmutableMap.of(Constants.MEDIUM_MEM, 1000L, Constants.MEDIUM_HDD, 8000L);
    Map<String, Long> exceptedUsedBytesOnTiers =
        ImmutableMap.of(Constants.MEDIUM_MEM, 0L, Constants.MEDIUM_HDD, 0L);
    assertEquals(exceptedCapacityBytesOnTiers, meta.getCapacityBytesOnTiers());
    assertEquals(exceptedUsedBytesOnTiers, meta.getUsedBytesOnTiers());
  }
}
