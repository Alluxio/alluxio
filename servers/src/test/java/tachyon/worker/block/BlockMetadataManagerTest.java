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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.block.meta.BlockMeta;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.meta.StorageTier;
import tachyon.worker.block.meta.TempBlockMeta;

// TODO: improve code health of this unittest.
public class BlockMetadataManagerTest {
  private static final long TEST_USER_ID = 2;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_TEMP_BLOCK_ID = 10;
  private static final long TEST_BLOCK_SIZE = 20;
  private BlockMetadataManager mMetaManager;
  private String mTachyonHome;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    TachyonConf tachyonConf = new TachyonConf();
    // Setup a two-tier storage
    mTachyonHome = mFolder.newFolder().getAbsolutePath();;
    tachyonConf.set(Constants.TACHYON_HOME, mTachyonHome);
    tachyonConf.set(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, "2");
    // TODO improve the code using String.format
    tachyonConf.set("tachyon.worker.tieredstore.level0.alias", "MEM");
    tachyonConf.set("tachyon.worker.tieredstore.level0.dirs.path", mTachyonHome + "/ramdisk");
    tachyonConf.set("tachyon.worker.tieredstore.level0.dirs.quota", 1000 + "");
    tachyonConf.set("tachyon.worker.tieredstore.level1.alias", "HDD");
    tachyonConf.set("tachyon.worker.tieredstore.level1.dirs.path", mTachyonHome + "/disk1,"
        + mTachyonHome + "/disk2");
    tachyonConf.set("tachyon.worker.tieredstore.level1.dirs.quota", 3000 + "," + 5000);

    mMetaManager = BlockMetadataManager.newBlockMetadataManager(tachyonConf);
  }

  @Test
  public void getTierTest() throws Exception {
    StorageTier tier;
    tier = mMetaManager.getTier(1); // MEM
    Assert.assertEquals(1, tier.getTierAlias());
    Assert.assertEquals(0, tier.getTierLevel());
    tier = mMetaManager.getTier(3); // HDD
    Assert.assertEquals(3, tier.getTierAlias());
    Assert.assertEquals(1, tier.getTierLevel());
  }

  @Test
  public void getDirTest() throws Exception {
    BlockStoreLocation loc;
    StorageDir dir;

    loc = new BlockStoreLocation(1, 0, 0);
    dir = mMetaManager.getDir(loc);
    Assert.assertEquals(loc.tierAlias(), dir.getParentTier().getTierAlias());
    Assert.assertEquals(loc.dir(), dir.getDirIndex());

    loc = new BlockStoreLocation(3, 0, 1);
    dir = mMetaManager.getDir(loc);
    Assert.assertEquals(loc.tierAlias(), dir.getParentTier().getTierAlias());
    Assert.assertEquals(loc.dir(), dir.getDirIndex());
  }

  @Test
  public void getTierNotExistingTest() throws Exception {
    int badTierAlias = 2;
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Cannot find tier with alias " + badTierAlias);
    mMetaManager.getTier(badTierAlias);
  }

  @Test
  public void getTiersTest() throws Exception {
    List<StorageTier> tiers = mMetaManager.getTiers();
    Assert.assertEquals(2, tiers.size());
    Assert.assertEquals(1, tiers.get(0).getTierAlias());
    Assert.assertEquals(0, tiers.get(0).getTierLevel());
    Assert.assertEquals(3, tiers.get(1).getTierAlias());
    Assert.assertEquals(1, tiers.get(1).getTierLevel());
  }

  @Test
  public void getTiersBelowTest() throws Exception {
    List<StorageTier> tiersBelow = mMetaManager.getTiersBelow(1);
    Assert.assertEquals(1, tiersBelow.size());
    Assert.assertEquals(3, tiersBelow.get(0).getTierAlias());
    Assert.assertEquals(1, tiersBelow.get(0).getTierLevel());

    tiersBelow = mMetaManager.getTiersBelow(3);
    Assert.assertEquals(0, tiersBelow.size());
  }

  @Test
  public void getAvailableBytesTest() throws Exception {
    Assert.assertEquals(9000, mMetaManager.getAvailableBytes(BlockStoreLocation.anyTier()));
    Assert.assertEquals(1000, mMetaManager.getAvailableBytes(BlockStoreLocation.anyDirInTier(1)));
    Assert.assertEquals(8000, mMetaManager.getAvailableBytes(BlockStoreLocation.anyDirInTier(3)));
    Assert.assertEquals(1000, mMetaManager.getAvailableBytes(new BlockStoreLocation(1, 0, 0)));
    Assert.assertEquals(3000, mMetaManager.getAvailableBytes(new BlockStoreLocation(3, 1, 0)));
    Assert.assertEquals(5000, mMetaManager.getAvailableBytes(new BlockStoreLocation(3, 1, 1)));
  }

  @Test
  public void blockMetaTest() throws Exception {
    StorageDir dir = mMetaManager.getTier(3).getDir(0);
    TempBlockMeta tempBlockMeta =
        new TempBlockMeta(TEST_USER_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, dir);

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
    // Add temp block with previous blockId
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

  @Test
  public void getBlockMetaNotExistingTest() throws Exception {
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to get BlockMeta: blockId " + TEST_BLOCK_ID + " not found");
    mMetaManager.getBlockMeta(TEST_BLOCK_ID);
  }

  @Test
  public void getTempBlockMetaNotExistingTest() throws Exception {
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to get TempBlockMeta: temp blockId " + TEST_TEMP_BLOCK_ID
        + " not found");
    mMetaManager.getTempBlockMeta(TEST_TEMP_BLOCK_ID);
  }

  @Test
  public void moveBlockMetaTest() throws Exception {
    StorageDir dir = mMetaManager.getTier(1).getDir(0);
    TempBlockMeta tempBlockMeta =
        new TempBlockMeta(TEST_USER_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    mMetaManager.addTempBlockMeta(tempBlockMeta);
    mMetaManager.commitTempBlockMeta(tempBlockMeta);
    BlockMeta blockMeta = mMetaManager.getBlockMeta(TEST_TEMP_BLOCK_ID);

    // Move to anywhere
    mMetaManager.moveBlockMeta(blockMeta, BlockStoreLocation.anyTier());

    // Move to tier HDD tier
    blockMeta = mMetaManager.moveBlockMeta(blockMeta, BlockStoreLocation.anyDirInTier(3));
    Assert.assertEquals(3, blockMeta.getBlockLocation().tierAlias());

    // Move to tier MEM and dir 0
    blockMeta = mMetaManager.moveBlockMeta(blockMeta, new BlockStoreLocation(1, 0, 0));
    Assert.assertEquals(1, blockMeta.getBlockLocation().tierAlias());
    Assert.assertEquals(0, blockMeta.getBlockLocation().dir());
  }

  @Test
  public void moveBlockMetaExceedCapacity() throws Exception {
    StorageDir dir = mMetaManager.getTier(3).getDir(0);
    BlockMeta blockMeta = new BlockMeta(TEST_BLOCK_ID, 2000, dir);
    dir.addBlockMeta(blockMeta);

    mThrown.expect(IOException.class);
    mThrown.expectMessage("does not have enough space");
    mMetaManager.moveBlockMeta(blockMeta, new BlockStoreLocation(1, 0, 0));
  }

  @Test
  public void resizeTempBlockMetaTest() throws Exception {
    StorageDir dir = mMetaManager.getTier(1).getDir(0);
    TempBlockMeta tempBlockMeta =
        new TempBlockMeta(TEST_USER_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    mMetaManager.resizeTempBlockMeta(tempBlockMeta, TEST_BLOCK_SIZE + 1);
    Assert.assertEquals(TEST_BLOCK_SIZE + 1, tempBlockMeta.getBlockSize());
  }

  @Test
  public void cleanupUserTest() throws Exception {
    StorageDir dir = mMetaManager.getTier(1).getDir(0);
    final long tempBlockId1 = 1;
    final long tempBlockId2 = 2;
    final long tempBlockId3 = 3;
    final long userId1 = 100;
    final long userId2 = 200;
    TempBlockMeta tempBlockMeta1 = new TempBlockMeta(userId1, tempBlockId1, TEST_BLOCK_SIZE, dir);
    TempBlockMeta tempBlockMeta2 = new TempBlockMeta(userId1, tempBlockId2, TEST_BLOCK_SIZE, dir);
    TempBlockMeta tempBlockMeta3 = new TempBlockMeta(userId2, tempBlockId3, TEST_BLOCK_SIZE, dir);
    BlockMeta blockMeta = new BlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, dir);
    dir.addTempBlockMeta(tempBlockMeta1);
    dir.addTempBlockMeta(tempBlockMeta2);
    dir.addTempBlockMeta(tempBlockMeta3);
    dir.addBlockMeta(blockMeta);

    // Get temp blocks for userId1, expect to get tempBlock1 and tempBlock2
    List<TempBlockMeta> toRemove = mMetaManager.getUserTempBlocks(userId1);
    List<Long> toRemoveBlockIds = new ArrayList<Long>(toRemove.size());
    for (TempBlockMeta tempBlockMeta : toRemove) {
      toRemoveBlockIds.add(tempBlockMeta.getBlockId());
    }
    Assert.assertEquals(Sets.newHashSet(tempBlockMeta1, tempBlockMeta2),
        new HashSet<TempBlockMeta>(toRemove));
    Assert.assertTrue(dir.hasTempBlockMeta(tempBlockId1));
    Assert.assertTrue(dir.hasTempBlockMeta(tempBlockId2));

    // Clean up userId1, expect tempBlock1 and tempBlock2 to be removed.
    mMetaManager.cleanupUserTempBlocks(userId1, toRemoveBlockIds);
    Assert.assertFalse(dir.hasTempBlockMeta(tempBlockId1));
    Assert.assertFalse(dir.hasTempBlockMeta(tempBlockId2));
    Assert.assertTrue(dir.hasTempBlockMeta(tempBlockId3));
    Assert.assertTrue(dir.hasBlockMeta(TEST_BLOCK_ID));

    // Get temp blocks for userId1 again, expect to get nothing
    toRemove = mMetaManager.getUserTempBlocks(userId1);
    toRemoveBlockIds = new ArrayList<Long>(toRemove.size());
    for (TempBlockMeta tempBlockMeta : toRemove) {
      toRemoveBlockIds.add(tempBlockMeta.getBlockId());
    }
    Assert.assertTrue(toRemove.isEmpty());

    // Clean up userId1 again, expect nothing to happen
    mMetaManager.cleanupUserTempBlocks(userId1, toRemoveBlockIds);
    Assert.assertFalse(dir.hasTempBlockMeta(tempBlockId1));
    Assert.assertFalse(dir.hasTempBlockMeta(tempBlockId2));
    Assert.assertTrue(dir.hasTempBlockMeta(tempBlockId3));
    Assert.assertTrue(dir.hasBlockMeta(TEST_BLOCK_ID));

    // Get temp blocks for userId2, expect to get tempBlock3
    toRemove = mMetaManager.getUserTempBlocks(userId2);
    toRemoveBlockIds = new ArrayList<Long>(toRemove.size());
    for (TempBlockMeta tempBlockMeta : toRemove) {
      toRemoveBlockIds.add(tempBlockMeta.getBlockId());
    }
    Assert.assertEquals(Sets.newHashSet(tempBlockMeta3), new HashSet<TempBlockMeta>(toRemove));
    Assert.assertTrue(dir.hasTempBlockMeta(tempBlockId3));

    // Clean up userId2, expect tempBlock3 to be removed
    mMetaManager.cleanupUserTempBlocks(userId2, toRemoveBlockIds);
    Assert.assertFalse(dir.hasTempBlockMeta(tempBlockId1));
    Assert.assertFalse(dir.hasTempBlockMeta(tempBlockId2));
    Assert.assertFalse(dir.hasTempBlockMeta(tempBlockId3));
    Assert.assertTrue(dir.hasBlockMeta(TEST_BLOCK_ID));
  }

  @Test
  public void getBlockStoreMetaTest() throws Exception {
    BlockStoreMeta meta = mMetaManager.getBlockStoreMeta();
    Assert.assertNotNull(meta);

    // Assert the capacities are at alias level [MEM: 1000][SSD: 0][HDD: 8000]
    List<Long> exceptedCapacityBytesOnTiers = new ArrayList<Long>(Arrays.asList(1000L, 0L, 8000L));
    List<Long> exceptedUsedBytesOnTiers = new ArrayList<Long>(Arrays.asList(0L, 0L, 0L));
    Assert.assertEquals(exceptedCapacityBytesOnTiers, meta.getCapacityBytesOnTiers());
    Assert.assertEquals(exceptedUsedBytesOnTiers, meta.getUsedBytesOnTiers());
  }
}
