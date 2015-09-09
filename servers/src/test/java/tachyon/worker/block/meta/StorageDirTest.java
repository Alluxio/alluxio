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

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import tachyon.StorageLevelAlias;
import tachyon.exception.AlreadyExistsException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.InvalidStateException;
import tachyon.exception.NotFoundException;
import tachyon.exception.OutOfSpaceException;
import tachyon.util.io.BufferUtils;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.TieredBlockStoreTestUtils;

public final class StorageDirTest {
  private static final long TEST_USER_ID = 2;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_BLOCK_SIZE = 20;
  private static final long TEST_TEMP_BLOCK_ID = 10;
  private static final long TEST_TEMP_BLOCK_SIZE = 30;
  private static final int TEST_TIER_LEVEL = 0;
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
  public void before() throws Exception {
    // Creates a dummy test dir under mTestDirPath with 1 byte space so initialization can occur
    mTestDirPath = mFolder.newFolder().getAbsolutePath();
    String[] testDirPaths = {mTestDirPath };
    long[] testDirCapacity = {1};

    TieredBlockStoreTestUtils.setTachyonConfWithSingleTier(null, TEST_TIER_LEVEL,
        StorageLevelAlias.MEM, testDirPaths, testDirCapacity, "");

    mTier = StorageTier.newStorageTier(TEST_TIER_LEVEL);
    mDir = StorageDir.newStorageDir(mTier, TEST_DIR_INDEX, TEST_DIR_CAPACITY, mTestDirPath);
    mBlockMeta = new BlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, mDir);
    mTempBlockMeta =
        new TempBlockMeta(TEST_USER_ID, TEST_TEMP_BLOCK_ID, TEST_TEMP_BLOCK_SIZE, mDir);
  }

  private StorageDir newStorageDir(File testDir) throws Exception {
    return StorageDir.newStorageDir(mTier, TEST_DIR_INDEX, TEST_DIR_CAPACITY,
        testDir.getAbsolutePath());
  }

  private void newBlockFile(File dir, String name, int lenBytes) throws IOException {
    File block = new File(dir, name);
    block.createNewFile();
    byte[] data = BufferUtils.getIncreasingByteArray(lenBytes);
    BufferUtils.writeBufferToFile(block.getAbsolutePath(), data);
  }

  @Test
  public void initializeMetaNoExceptionTest() throws Exception {
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
  public void initializeMetaDeleteInappropriateFileTest() throws Exception {
    File testDir = mFolder.newFolder();

    newBlockFile(testDir, "block", 1);

    mDir = newStorageDir(testDir);
    assertStorageDirEmpty(testDir, mDir, TEST_DIR_CAPACITY);
  }

  @Test
  public void initializeMetaDeleteInappropriateDirTest() throws Exception {
    File testDir = mFolder.newFolder();

    File newDir = new File(testDir, "dir");
    boolean created = newDir.mkdir();
    Assert.assertTrue(created);

    mDir = newStorageDir(testDir);
    assertStorageDirEmpty(testDir, mDir, TEST_DIR_CAPACITY);
  }

  @Test
  public void initializeMetaBlockLargerThanCapacityTest() throws Exception {
    File testDir = mFolder.newFolder();

    newBlockFile(testDir, String.valueOf(TEST_BLOCK_ID), Ints.checkedCast(TEST_DIR_CAPACITY + 1));
    StorageLevelAlias alias = StorageLevelAlias.getAlias(TEST_DIR_INDEX);
    mThrown.expect(OutOfSpaceException.class);
    mThrown.expectMessage(ExceptionMessage.NO_SPACE_FOR_BLOCK_META.getMessage(TEST_BLOCK_ID,
        TEST_DIR_CAPACITY + 1, TEST_DIR_CAPACITY, alias));
    mDir = newStorageDir(testDir);
    assertMetadataEmpty(mDir, TEST_DIR_CAPACITY);
    // assert file not deleted
    File[] files = testDir.listFiles();
    Assert.assertNotNull(files);
    Assert.assertEquals(1, files.length);
  }

  @Test
  public void getBytesTest() throws Exception {
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
  public void getBlockIdsTest() throws Exception {
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
  public void getBlocksTest() throws Exception {
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
  public void addBlockMetaTooBigTest() throws Exception {
    final long bigBlockSize = TEST_DIR_CAPACITY + 1;
    BlockMeta bigBlockMeta = new BlockMeta(TEST_BLOCK_ID, bigBlockSize, mDir);
    StorageLevelAlias alias =
        StorageLevelAlias.getAlias(bigBlockMeta.getBlockLocation().tierAlias());
    mThrown.expect(OutOfSpaceException.class);
    mThrown.expectMessage(ExceptionMessage.NO_SPACE_FOR_BLOCK_META.getMessage(TEST_BLOCK_ID,
        bigBlockSize, TEST_DIR_CAPACITY, alias));
    mDir.addBlockMeta(bigBlockMeta);
  }

  @Test
  public void addBlockMetaExistingTest() throws Exception {
    mThrown.expect(AlreadyExistsException.class);
    mThrown.expectMessage(ExceptionMessage.ADD_EXISTING_BLOCK.getMessage(TEST_BLOCK_ID,
        StorageLevelAlias.getAlias(TEST_DIR_INDEX)));
    mDir.addBlockMeta(mBlockMeta);
    BlockMeta dupBlockMeta = new BlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, mDir);
    mDir.addBlockMeta(dupBlockMeta);
  }

  @Test
  public void removeBlockMetaNotExistingTest() throws Exception {
    mThrown.expect(NotFoundException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(TEST_BLOCK_ID));
    mDir.removeBlockMeta(mBlockMeta);
  }

  @Test
  public void getBlockMetaNotExistingTest() throws Exception {
    mThrown.expect(NotFoundException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(TEST_BLOCK_ID));
    mDir.getBlockMeta(TEST_BLOCK_ID);
  }

  @Test
  public void addTempBlockMetaTooBigTest() throws Exception {
    final long bigBlockSize = TEST_DIR_CAPACITY + 1;
    TempBlockMeta bigTempBlockMeta =
        new TempBlockMeta(TEST_USER_ID, TEST_TEMP_BLOCK_ID, bigBlockSize, mDir);
    StorageLevelAlias alias =
        StorageLevelAlias.getAlias(bigTempBlockMeta.getBlockLocation().tierAlias());
    mThrown.expect(OutOfSpaceException.class);
    mThrown.expectMessage(ExceptionMessage.NO_SPACE_FOR_BLOCK_META.getMessage(TEST_TEMP_BLOCK_ID,
        bigBlockSize, TEST_DIR_CAPACITY, alias));
    mDir.addTempBlockMeta(bigTempBlockMeta);
  }

  @Test
  public void addTempBlockMetaExistingTest() throws Exception {
    mThrown.expect(AlreadyExistsException.class);
    mThrown.expectMessage(ExceptionMessage.ADD_EXISTING_BLOCK.getMessage(TEST_TEMP_BLOCK_ID,
        StorageLevelAlias.getAlias(TEST_DIR_INDEX)));
    mDir.addTempBlockMeta(mTempBlockMeta);
    TempBlockMeta dupTempBlockMeta =
        new TempBlockMeta(TEST_USER_ID, TEST_TEMP_BLOCK_ID, TEST_TEMP_BLOCK_SIZE, mDir);
    mDir.addTempBlockMeta(dupTempBlockMeta);
  }

  @Test
  public void removeTempBlockMetaNotExistingTest() throws Exception {
    mThrown.expect(NotFoundException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(TEST_TEMP_BLOCK_ID));
    mDir.removeTempBlockMeta(mTempBlockMeta);
  }

  @Test
  public void removeTempBlockMetaNotOwnerTest() throws Exception {
    final long wrongUserId = TEST_USER_ID + 1;
    StorageLevelAlias alias = StorageLevelAlias.getAlias(TEST_DIR_INDEX);
    mThrown.expect(NotFoundException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_NOT_FOUND_FOR_USER.getMessage(TEST_TEMP_BLOCK_ID,
        alias, wrongUserId));
    mDir.addTempBlockMeta(mTempBlockMeta);
    TempBlockMeta wrongTempBlockMeta =
        new TempBlockMeta(wrongUserId, TEST_TEMP_BLOCK_ID, TEST_TEMP_BLOCK_SIZE, mDir);
    mDir.removeTempBlockMeta(wrongTempBlockMeta);
  }

  @Test
  public void getTempBlockMetaNotExistingTest() throws Exception {
    mThrown.expect(NotFoundException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(TEST_TEMP_BLOCK_ID));
    mDir.getBlockMeta(TEST_TEMP_BLOCK_ID);
  }

  @Test
  public void blockMetaTest() throws Exception {
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
  public void tempBlockMetaTest() throws Exception {
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

  @Test
  public void resizeTempBlockMetaInvalidStateExceptionTest() throws Exception {
    mDir.addTempBlockMeta(mTempBlockMeta);
    final long newSize = TEST_TEMP_BLOCK_SIZE - 10;
    try {
      mDir.resizeTempBlockMeta(mTempBlockMeta, newSize);
      Assert.fail("Should throw an Exception when newSize is smaller than oldSize");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof InvalidStateException);
      Assert.assertTrue(e.getMessage().equals("Shrinking block, not supported!"));
      Assert.assertEquals(TEST_TEMP_BLOCK_SIZE, mTempBlockMeta.getBlockSize());
    }
  }

  @Test
  public void resizeTempBlockMetaNoAvailableBytesTest() throws Exception {
    mDir.addTempBlockMeta(mTempBlockMeta);
    // resize the temp block size to the dir capacity, which is the limit
    mDir.resizeTempBlockMeta(mTempBlockMeta, TEST_DIR_CAPACITY);
    Assert.assertEquals(TEST_DIR_CAPACITY, mTempBlockMeta.getBlockSize());
    mThrown.expect(IllegalStateException.class);
    mThrown.expectMessage("Available bytes should always be non-negative");
    // resize again, now the newSize is more than available bytes, exception thrown
    mDir.resizeTempBlockMeta(mTempBlockMeta, TEST_DIR_CAPACITY + 1);
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
