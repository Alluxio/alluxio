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

package tachyon.worker.block.meta;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Sets;

import tachyon.conf.TachyonConf;

public class StorageDirTest {
  private static final long TEST_USER_ID = 2;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_TEMP_BLOCK_ID = 10;
  private static final long TEST_BLOCK_SIZE = 20;
  private static final String TEST_DIR_PATH = "/mnt/ramdisk/0/";
  private static final int TEST_DIR_INDEX = 1;
  private static final long TEST_DIR_CAPACITY = 1000;
  private StorageTier mTier;
  private StorageDir mDir;
  private BlockMeta mBlockMeta;
  private TempBlockMeta mTempBlockMeta;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() {
    TachyonConf tachyonConf = new TachyonConf();
    mTier = new StorageTier(tachyonConf, 0 /* level */);
    mDir = new StorageDir(mTier, TEST_DIR_INDEX, TEST_DIR_CAPACITY, TEST_DIR_PATH);
    mBlockMeta = new BlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, mDir);
    mTempBlockMeta = new TempBlockMeta(TEST_USER_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, mDir);
  }

  @Test
  public void getCapacityBytesTest() {
    Assert.assertEquals(TEST_DIR_CAPACITY, mDir.getCapacityBytes());
  }

  @Test
  public void getAvailableBytesTest() {
    Assert.assertEquals(TEST_DIR_CAPACITY, mDir.getAvailableBytes());
  }

  @Test
  public void getDirPathTest() {
    Assert.assertEquals(TEST_DIR_PATH, mDir.getDirPath());
  }

  @Test
  public void getParentTierTest() {
    Assert.assertEquals(mTier, mDir.getParentTier());
  }

  @Test
  public void getDirIndexTest() {
    Assert.assertEquals(TEST_DIR_INDEX, mDir.getDirIndex());
  }

  @Test
  public void getBlockIdsTest() throws IOException {
    long blockId1 = TEST_BLOCK_ID + 1;
    long blockId2 = TEST_BLOCK_ID + 2;

    BlockMeta blockMeta1 = new BlockMeta(blockId1, TEST_BLOCK_SIZE, mDir);
    BlockMeta blockMeta2 = new BlockMeta(blockId2, TEST_BLOCK_SIZE, mDir);
    mDir.addBlockMeta(blockMeta1);
    mDir.addBlockMeta(blockMeta2);

    List<Long> actual = mDir.getBlockIds();
    Assert.assertEquals(Sets.newHashSet(blockId1, blockId2), new HashSet<Long>(actual));
  }

  @Test
  public void getBlocksTest() throws IOException {
    long blockId1 = TEST_BLOCK_ID + 1;
    long blockId2 = TEST_BLOCK_ID + 2;

    BlockMeta blockMeta1 = new BlockMeta(blockId1, TEST_BLOCK_SIZE, mDir);
    BlockMeta blockMeta2 = new BlockMeta(blockId2, TEST_BLOCK_SIZE, mDir);
    mDir.addBlockMeta(blockMeta1);
    mDir.addBlockMeta(blockMeta2);

    List<BlockMeta> actual = mDir.getBlocks();
    Assert.assertEquals(Sets.newHashSet(blockMeta1, blockMeta2), new HashSet<BlockMeta>(actual));
  }

  @Test
  public void addBlockMetaTooBigTest() throws IOException {
    final long bigBlockSize = TEST_DIR_CAPACITY + 1;
    BlockMeta bigBlockMeta = new BlockMeta(TEST_BLOCK_ID, bigBlockSize, mDir);
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to add BlockMeta: blockId " + TEST_BLOCK_ID + " is "
        + bigBlockSize + " bytes, but only " + TEST_DIR_CAPACITY + " bytes available");
    mDir.addBlockMeta(bigBlockMeta);
  }

  @Test
  public void addBlockMetaExistingTest() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to add BlockMeta: blockId " + TEST_BLOCK_ID + " exists");
    mDir.addBlockMeta(mBlockMeta);
    BlockMeta dupBlockMeta = new BlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, mDir);
    mDir.addBlockMeta(dupBlockMeta);
  }

  @Test
  public void removeBlockMetaNotExistingTest() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to remove BlockMeta: blockId " + TEST_BLOCK_ID + " not found");
    mDir.removeBlockMeta(mBlockMeta);
  }

  @Test
  public void getBlockMetaNotExistingTest() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to get BlockMeta: blockId " + TEST_BLOCK_ID + " not found in ");
    mDir.getBlockMeta(TEST_BLOCK_ID);
  }

  @Test
  public void addTempBlockMetaTooBigTest() throws IOException {
    final long bigBlockSize = TEST_DIR_CAPACITY + 1;
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to add TempBlockMeta: blockId " + TEST_TEMP_BLOCK_ID + " is "
        + bigBlockSize + " bytes, but only " + TEST_DIR_CAPACITY + " bytes available");
    TempBlockMeta bigTempBlockMeta =
        new TempBlockMeta(TEST_USER_ID, TEST_TEMP_BLOCK_ID, bigBlockSize, mDir);
    mDir.addTempBlockMeta(bigTempBlockMeta);
  }

  @Test
  public void addTempBlockMetaExistingTest() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to add TempBlockMeta: blockId " + TEST_TEMP_BLOCK_ID + " exists");
    mDir.addTempBlockMeta(mTempBlockMeta);
    TempBlockMeta dupTempBlockMeta =
        new TempBlockMeta(TEST_USER_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, mDir);
    mDir.addTempBlockMeta(dupTempBlockMeta);
  }

  @Test
  public void removeTempBlockMetaNotExistingTest() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to remove TempBlockMeta: blockId " + TEST_TEMP_BLOCK_ID + " not "
        + "found");
    mDir.removeTempBlockMeta(mTempBlockMeta);
  }

  @Test
  public void removeTempBlockMetaNotOwnerTest() throws IOException {
    final long wrongUserId = TEST_USER_ID + 1;
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to remove TempBlockMeta: blockId " + TEST_TEMP_BLOCK_ID
        + " has userId " + wrongUserId + " not found");
    mDir.addTempBlockMeta(mTempBlockMeta);
    TempBlockMeta wrongTempBlockMeta =
        new TempBlockMeta(wrongUserId, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, mDir);
    mDir.removeTempBlockMeta(wrongTempBlockMeta);
  }

  @Test
  public void getTempBlockMetaNotExistingTest() throws IOException {
    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to get BlockMeta: blockId " + TEST_TEMP_BLOCK_ID
        + " not found in ");
    mDir.getBlockMeta(TEST_TEMP_BLOCK_ID);
  }

  @Test
  public void blockMetaTest() throws IOException {
    Assert.assertFalse(mDir.hasBlockMeta(TEST_BLOCK_ID));
    Assert.assertEquals(TEST_DIR_CAPACITY, mDir.getAvailableBytes());

    mDir.addBlockMeta(mBlockMeta);
    Assert.assertTrue(mDir.hasBlockMeta(TEST_BLOCK_ID));
    Assert.assertEquals(mBlockMeta, mDir.getBlockMeta(TEST_BLOCK_ID));
    Assert.assertEquals(TEST_DIR_CAPACITY - TEST_BLOCK_SIZE, mDir.getAvailableBytes());

    mDir.removeBlockMeta(mBlockMeta);
    Assert.assertFalse(mDir.hasBlockMeta(TEST_BLOCK_ID));
    Assert.assertEquals(TEST_DIR_CAPACITY, mDir.getAvailableBytes());
  }

  @Test
  public void tempBlockMetaTest() throws IOException {
    Assert.assertFalse(mDir.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    Assert.assertEquals(TEST_DIR_CAPACITY, mDir.getAvailableBytes());

    mDir.addTempBlockMeta(mTempBlockMeta);
    Assert.assertTrue(mDir.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    Assert.assertEquals(mTempBlockMeta, mDir.getTempBlockMeta(TEST_TEMP_BLOCK_ID));
    Assert.assertEquals(TEST_DIR_CAPACITY - TEST_BLOCK_SIZE, mDir.getAvailableBytes());

    mDir.removeTempBlockMeta(mTempBlockMeta);
    Assert.assertFalse(mDir.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    Assert.assertEquals(TEST_DIR_CAPACITY, mDir.getAvailableBytes());
  }


  @Test
  public void resizeTempBlockMetaTest() throws Exception {
    mDir.addTempBlockMeta(mTempBlockMeta);
    Assert.assertEquals(TEST_DIR_CAPACITY - TEST_BLOCK_SIZE, mDir.getAvailableBytes());
    final long newSize = TEST_BLOCK_SIZE + 10;
    mDir.resizeTempBlockMeta(mTempBlockMeta, newSize);
    Assert.assertEquals(TEST_DIR_CAPACITY - newSize, mDir.getAvailableBytes());
  }

  @Test
  public void cleanupUserTest() throws Exception {
    // Create blocks under TEST_USER_ID
    mDir.addBlockMeta(mBlockMeta);

    // Create temp blocks under TEST_USER_ID
    long tempBlockId1 = TEST_TEMP_BLOCK_ID + 1;
    long tempBlockId2 = TEST_TEMP_BLOCK_ID + 2;
    long tempBlockId3 = TEST_TEMP_BLOCK_ID + 3;
    long otherUserId = TEST_USER_ID + 1;

    TempBlockMeta tempBlockMeta1 =
        new TempBlockMeta(TEST_USER_ID, tempBlockId1, TEST_BLOCK_SIZE, mDir);
    TempBlockMeta tempBlockMeta2 =
        new TempBlockMeta(TEST_USER_ID, tempBlockId2, TEST_BLOCK_SIZE, mDir);
    TempBlockMeta tempBlockMeta3 =
        new TempBlockMeta(otherUserId, tempBlockId3, TEST_BLOCK_SIZE, mDir);
    mDir.addTempBlockMeta(tempBlockMeta1);
    mDir.addTempBlockMeta(tempBlockMeta2);
    mDir.addTempBlockMeta(tempBlockMeta3);

    List<TempBlockMeta> actual = mDir.cleanupUser(TEST_USER_ID);
    Assert.assertEquals(Sets.newHashSet(tempBlockMeta1, tempBlockMeta2),
        new HashSet<TempBlockMeta>(actual));
    // Two temp blocks created by TEST_USER_ID are expected to be removed
    Assert.assertFalse(mDir.hasTempBlockMeta(tempBlockId1));
    Assert.assertFalse(mDir.hasTempBlockMeta(tempBlockId2));
    // Temp block created by otherUserId is expected to stay
    Assert.assertTrue(mDir.hasTempBlockMeta(tempBlockId3));
    // Block created by TEST_USER_ID is expected to stay
    Assert.assertTrue(mDir.hasBlockMeta(TEST_BLOCK_ID));
  }
}
