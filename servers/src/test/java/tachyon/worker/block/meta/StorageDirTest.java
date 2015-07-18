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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import tachyon.Constants;
import tachyon.TestUtils;
import tachyon.conf.TachyonConf;
import tachyon.worker.block.BlockStoreLocation;

public class StorageDirTest {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final long TEST_USER_ID = 2;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_BLOCK_SIZE = 20;
  private static final long TEST_TEMP_BLOCK_ID = 10;
  private static final long TEST_TEMP_BLOCK_SIZE = 30;
  private static final int TEST_DIR_INDEX = 1;
  private static final long TEST_DIR_CAPACITY = 1000;
  private String mTestDirPath;
  private StorageTier mTier;
  private StorageDir mDir;
  private BlockMeta mBlockMeta;
  private TempBlockMeta mTempBlockMeta;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    // Creates a dummy test dir under mTestDirPath with 1 byte space so initialization can occur
    mTestDirPath = mFolder.newFolder().getAbsolutePath();
    TachyonConf tachyonConf = new TachyonConf();
    tachyonConf.set("tachyon.worker.tieredstore.level0.dirs.path", mFolder.newFolder()
        .getAbsolutePath());
    tachyonConf.set("tachyon.worker.tieredstore.level0.dirs.quota", "1b");
    tachyonConf.set(Constants.WORKER_DATA_FOLDER, "");

    mTier = StorageTier.newStorageTier(tachyonConf, 0 /* level */);
    mDir = StorageDir.newStorageDir(mTier, TEST_DIR_INDEX, TEST_DIR_CAPACITY, mTestDirPath);
    mBlockMeta = new BlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, mDir);
    mTempBlockMeta =
        new TempBlockMeta(TEST_USER_ID, TEST_TEMP_BLOCK_ID, TEST_TEMP_BLOCK_SIZE, mDir);
  }

  private StorageDir newStorageDir(File testDir) throws IOException {
    return StorageDir.newStorageDir(mTier, TEST_DIR_INDEX, TEST_DIR_CAPACITY,
        testDir.getAbsolutePath());
  }

  private void newBlockFile(File dir, String name, int lenBytes) throws IOException {
    File block = new File(dir, name);
    block.createNewFile();
    byte[] data = TestUtils.getIncreasingByteArray(lenBytes);
    TestUtils.writeBufferToFile(block.getAbsolutePath(), data);
  }

  @Test
  public void initializeMetaNoExceptionTest() throws IOException {
    File testDir = mFolder.newFolder();

    int nBlock = 10;
    long availableBytes = TEST_DIR_CAPACITY;
    for (int blockId = 0; blockId < nBlock; blockId ++) {
      int blockSizeBytes = blockId + 1;
      newBlockFile(testDir, String.valueOf(blockId), blockSizeBytes);
      availableBytes -= blockSizeBytes;
    }

    mDir = newStorageDir(testDir);
    Assert.assertEquals(TEST_DIR_CAPACITY, mDir.getCapacityBytes());
    Assert.assertEquals(availableBytes, mDir.getAvailableBytes());
    for (int blockId = 0; blockId < nBlock; blockId ++) {
      Assert.assertTrue(mDir.hasBlockMeta(blockId));
    }
  }

  private void assertMetadataEmpty(StorageDir dir, long capacity) {
    Assert.assertEquals(capacity, dir.getCapacityBytes());
    Assert.assertEquals(capacity, dir.getAvailableBytes());
    Assert.assertTrue(dir.getBlockIds().isEmpty());
  }

  private void assertStorageDirEmpty(File dirPath, StorageDir dir, long capacity) {
    assertMetadataEmpty(dir, capacity);
    File[] files = dirPath.listFiles();
    Assert.assertNotNull(files);
    Assert.assertEquals(0, files.length);
  }

  @Test
  public void initializeMetaDeleteInappropriateFileTest() throws IOException {
    File testDir = mFolder.newFolder();

    newBlockFile(testDir, "block", 1);

    mDir = newStorageDir(testDir);
    assertStorageDirEmpty(testDir, mDir, TEST_DIR_CAPACITY);
  }

  @Test
  public void initializeMetaDeleteInappropriateDirTest() throws IOException {
    File testDir = mFolder.newFolder();

    File newDir = new File(testDir, "dir");
    boolean created = newDir.mkdir();
    Assert.assertTrue(created);

    mDir = newStorageDir(testDir);
    assertStorageDirEmpty(testDir, mDir, TEST_DIR_CAPACITY);
  }

  @Test
  public void initializeMetaBlockLargerThanCapacityTest() throws IOException {
    File testDir = mFolder.newFolder();

    newBlockFile(testDir, String.valueOf(TEST_BLOCK_ID), Ints.checkedCast(TEST_DIR_CAPACITY + 1));

    mThrown.expect(IOException.class);
    mThrown.expectMessage("Failed to add BlockMeta");
    mDir = newStorageDir(testDir);
    assertMetadataEmpty(mDir, TEST_DIR_CAPACITY);
    // assert file not deleted
    File[] files = testDir.listFiles();
    Assert.assertNotNull(files);
    Assert.assertEquals(1, files.length);
  }

  @Test
  public void getBytesTest() throws IOException {
    // Initial state
    Assert.assertEquals(TEST_DIR_CAPACITY, mDir.getCapacityBytes());
    Assert.assertEquals(TEST_DIR_CAPACITY, mDir.getAvailableBytes());
    Assert.assertEquals(0, mDir.getCommittedBytes());

    // Add a temp block
    mDir.addTempBlockMeta(mTempBlockMeta);
    Assert.assertEquals(TEST_DIR_CAPACITY, mDir.getCapacityBytes());
    Assert.assertEquals(TEST_DIR_CAPACITY - TEST_TEMP_BLOCK_SIZE, mDir.getAvailableBytes());
    Assert.assertEquals(0, mDir.getCommittedBytes());

    // Add a committed block
    mDir.addBlockMeta(mBlockMeta);
    Assert.assertEquals(TEST_DIR_CAPACITY, mDir.getCapacityBytes());
    Assert.assertEquals(TEST_DIR_CAPACITY - TEST_BLOCK_SIZE - TEST_TEMP_BLOCK_SIZE,
        mDir.getAvailableBytes());
    Assert.assertEquals(TEST_BLOCK_SIZE, mDir.getCommittedBytes());

    // Remove the temp block added
    mDir.removeTempBlockMeta(mTempBlockMeta);
    Assert.assertEquals(TEST_DIR_CAPACITY, mDir.getCapacityBytes());
    Assert.assertEquals(TEST_DIR_CAPACITY - TEST_BLOCK_SIZE, mDir.getAvailableBytes());
    Assert.assertEquals(TEST_BLOCK_SIZE, mDir.getCommittedBytes());

    // Remove the committed block added
    mDir.removeBlockMeta(mBlockMeta);
    Assert.assertEquals(TEST_DIR_CAPACITY, mDir.getCapacityBytes());
    Assert.assertEquals(TEST_DIR_CAPACITY, mDir.getAvailableBytes());
    Assert.assertEquals(0, mDir.getCommittedBytes());
  }

  @Test
  public void getDirPathTest() {
    Assert.assertEquals(mTestDirPath, mDir.getDirPath());
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
        new TempBlockMeta(TEST_USER_ID, TEST_TEMP_BLOCK_ID, TEST_TEMP_BLOCK_SIZE, mDir);
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
        new TempBlockMeta(wrongUserId, TEST_TEMP_BLOCK_ID, TEST_TEMP_BLOCK_SIZE, mDir);
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
    Assert.assertEquals(TEST_DIR_CAPACITY - TEST_TEMP_BLOCK_SIZE, mDir.getAvailableBytes());

    mDir.removeTempBlockMeta(mTempBlockMeta);
    Assert.assertFalse(mDir.hasTempBlockMeta(TEST_TEMP_BLOCK_ID));
    Assert.assertEquals(TEST_DIR_CAPACITY, mDir.getAvailableBytes());
  }


  @Test
  public void resizeTempBlockMetaTest() throws Exception {
    mDir.addTempBlockMeta(mTempBlockMeta);
    Assert.assertEquals(TEST_DIR_CAPACITY - TEST_TEMP_BLOCK_SIZE, mDir.getAvailableBytes());
    final long newSize = TEST_TEMP_BLOCK_SIZE + 10;
    mDir.resizeTempBlockMeta(mTempBlockMeta, newSize);
    Assert.assertEquals(TEST_DIR_CAPACITY - newSize, mDir.getAvailableBytes());
  }

  // TODO: also test claimed space
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
        new TempBlockMeta(TEST_USER_ID, tempBlockId1, TEST_TEMP_BLOCK_SIZE, mDir);
    TempBlockMeta tempBlockMeta2 =
        new TempBlockMeta(TEST_USER_ID, tempBlockId2, TEST_TEMP_BLOCK_SIZE, mDir);
    TempBlockMeta tempBlockMeta3 =
        new TempBlockMeta(otherUserId, tempBlockId3, TEST_TEMP_BLOCK_SIZE, mDir);
    mDir.addTempBlockMeta(tempBlockMeta1);
    mDir.addTempBlockMeta(tempBlockMeta2);
    mDir.addTempBlockMeta(tempBlockMeta3);

    // Check the temporary blocks belonging to TEST_USER_ID
    List<TempBlockMeta> actual = mDir.getUserTempBlocks(TEST_USER_ID);
    List<Long> actualBlockIds = new ArrayList<Long>(actual.size());
    for (TempBlockMeta tempBlockMeta : actual) {
      actualBlockIds.add(tempBlockMeta.getBlockId());
    }
    Assert.assertEquals(Sets.newHashSet(tempBlockMeta1, tempBlockMeta2),
        new HashSet<TempBlockMeta>(actual));
    Assert.assertTrue(mDir.hasTempBlockMeta(tempBlockId1));
    Assert.assertTrue(mDir.hasTempBlockMeta(tempBlockId2));

    // Two temp blocks created by TEST_USER_ID are expected to be removed
    mDir.cleanupUserTempBlocks(TEST_USER_ID, actualBlockIds);
    Assert.assertFalse(mDir.hasTempBlockMeta(tempBlockId1));
    Assert.assertFalse(mDir.hasTempBlockMeta(tempBlockId2));
    // Temp block created by otherUserId is expected to stay
    Assert.assertTrue(mDir.hasTempBlockMeta(tempBlockId3));
    // Block created by TEST_USER_ID is expected to stay
    Assert.assertTrue(mDir.hasBlockMeta(TEST_BLOCK_ID));
  }

  @Test
  public void toBlockStoreLocationTest() {
    StorageTier tier = mDir.getParentTier();
    Assert.assertEquals(
        new BlockStoreLocation(tier.getTierAlias(), tier.getTierLevel(), mDir.getDirIndex()),
        mDir.toBlockStoreLocation());
  }
}
