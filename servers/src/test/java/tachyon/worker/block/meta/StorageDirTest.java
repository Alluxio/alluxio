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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import tachyon.exception.BlockAlreadyExistsException;
import tachyon.exception.BlockDoesNotExistException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.InvalidWorkerStateException;
import tachyon.exception.WorkerOutOfSpaceException;
import tachyon.util.io.BufferUtils;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.TieredBlockStoreTestUtils;

/**
 * Unit tests for {@link StorageDir}.
 */
public final class StorageDirTest {
  private static final long TEST_SESSION_ID = 2;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_BLOCK_SIZE = 20;
  private static final long TEST_TEMP_BLOCK_ID = 10;
  private static final long TEST_TEMP_BLOCK_SIZE = 30;
  private static final int TEST_TIER_ORDINAL = 0;
  private static final int TEST_DIR_INDEX = 1;
  private static final long TEST_DIR_CAPACITY = 1000;
  private String mTestDirPath;
  private StorageTier mTier;
  private StorageDir mDir;
  private BlockMeta mBlockMeta;
  private TempBlockMeta mTempBlockMeta;

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  /**
   * Sets up all dependencies before a test runs.
   *
   * @throws Exception if setting up a dependency fails
   */
  @Before
  public void before() throws Exception {
    // Creates a dummy test dir under mTestDirPath with 1 byte space so initialization can occur
    mTestDirPath = mFolder.newFolder().getAbsolutePath();
    String[] testDirPaths = {mTestDirPath};
    long[] testDirCapacity = {1};

    TieredBlockStoreTestUtils.setupTachyonConfWithSingleTier(null, TEST_TIER_ORDINAL, "MEM",
        testDirPaths, testDirCapacity, "");

    mTier = StorageTier.newStorageTier("MEM");
    mDir = StorageDir.newStorageDir(mTier, TEST_DIR_INDEX, TEST_DIR_CAPACITY, mTestDirPath);
    mBlockMeta = new BlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, mDir);
    mTempBlockMeta =
        new TempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID, TEST_TEMP_BLOCK_SIZE, mDir);
  }

  /**
   * Resets the context of the worker after a test ran.
   */
  @After
  public void after() {
    WorkerContext.reset();
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

  /**
   * Tests that a new storage directory has metadata for a created block.
   *
   * @throws Exception if creating a new storage directory or block file fails
   */
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

  /**
   * Tests that the metadata of the files and directory is empty when creating an inappropriate
   * file.
   *
   * @throws Exception if creating a new storage directory or block file fails
   */
  @Test
  public void initializeMetaDeleteInappropriateFileTest() throws Exception {
    File testDir = mFolder.newFolder();

    newBlockFile(testDir, "block", 1);

    mDir = newStorageDir(testDir);
    assertStorageDirEmpty(testDir, mDir, TEST_DIR_CAPACITY);
  }

  /**
   * Tests that the metadata of the files and directory is empty when creating an inappropriate
   * directory.
   *
   * @throws Exception if creating a new storage directory or block file fails
   */
  @Test
  public void initializeMetaDeleteInappropriateDirTest() throws Exception {
    File testDir = mFolder.newFolder();

    File newDir = new File(testDir, "dir");
    boolean created = newDir.mkdir();
    Assert.assertTrue(created);

    mDir = newStorageDir(testDir);
    assertStorageDirEmpty(testDir, mDir, TEST_DIR_CAPACITY);
  }

  /**
   * Tests that an exception is thrown when trying to initialize a block that is larger than the
   * capacity.
   *
   * @throws Exception if creating a new storage directory or block file fails
   */
  @Test
  public void initializeMetaBlockLargerThanCapacityTest() throws Exception {
    File testDir = mFolder.newFolder();

    newBlockFile(testDir, String.valueOf(TEST_BLOCK_ID), Ints.checkedCast(TEST_DIR_CAPACITY + 1));
    String alias = "MEM";
    mThrown.expect(WorkerOutOfSpaceException.class);
    mThrown.expectMessage(ExceptionMessage.NO_SPACE_FOR_BLOCK_META.getMessage(TEST_BLOCK_ID,
        TEST_DIR_CAPACITY + 1, TEST_DIR_CAPACITY, alias));
    mDir = newStorageDir(testDir);
    assertMetadataEmpty(mDir, TEST_DIR_CAPACITY);
    // assert file not deleted
    File[] files = testDir.listFiles();
    Assert.assertNotNull(files);
    Assert.assertEquals(1, files.length);
  }

  /**
   * Tests the {@link StorageDir#getCapacityBytes()}, the {@link StorageDir#getAvailableBytes()} and
   * the {@link StorageDir#getCommittedBytes()} methods.
   *
   * @throws Exception if adding or removing the metadata of a block fails
   */
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

  /**
   * Tests the {@link StorageDir#getDirPath()} method.
   */
  @Test
  public void getDirPathTest() {
    Assert.assertEquals(mTestDirPath, mDir.getDirPath());
  }

  /**
   * Tests the {@link StorageDir#getParentTier()} method.
   */
  @Test
  public void getParentTierTest() {
    Assert.assertEquals(mTier, mDir.getParentTier());
  }

  /**
   * Tests the {@link StorageDir#getDirIndex()} method.
   */
  @Test
  public void getDirIndexTest() {
    Assert.assertEquals(TEST_DIR_INDEX, mDir.getDirIndex());
  }

  /**
   * Tests the {@link StorageDir#getBlockIds()} method.
   *
   * @throws Exception if adding the metadata of the block fails
   */
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

  /**
   * Tests the {@link StorageDir#getBlocks()} method.
   *
   * @throws Exception if adding the metadata of the block fails
   */
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

  /**
   * Tests that an exception is thrown when trying to add metadata of a block that is too big.
   *
   * @throws Exception if adding the metadata of the block fails
   */
  @Test
  public void addBlockMetaTooBigTest() throws Exception {
    final long bigBlockSize = TEST_DIR_CAPACITY + 1;
    BlockMeta bigBlockMeta = new BlockMeta(TEST_BLOCK_ID, bigBlockSize, mDir);
    String alias = bigBlockMeta.getBlockLocation().tierAlias();
    mThrown.expect(WorkerOutOfSpaceException.class);
    mThrown.expectMessage(ExceptionMessage.NO_SPACE_FOR_BLOCK_META.getMessage(TEST_BLOCK_ID,
        bigBlockSize, TEST_DIR_CAPACITY, alias));
    mDir.addBlockMeta(bigBlockMeta);
  }

  /**
   * Tests that an exception is thrown when trying to add metadata of a block that already exists.
   *
   * @throws Exception if adding the metadata of the block fails
   */
  @Test
  public void addBlockMetaExistingTest() throws Exception {
    mThrown.expect(BlockAlreadyExistsException.class);
    mThrown.expectMessage(ExceptionMessage.ADD_EXISTING_BLOCK.getMessage(TEST_BLOCK_ID, "MEM"));
    mDir.addBlockMeta(mBlockMeta);
    BlockMeta dupBlockMeta = new BlockMeta(TEST_BLOCK_ID, TEST_BLOCK_SIZE, mDir);
    mDir.addBlockMeta(dupBlockMeta);
  }

  /**
   * Tests that an exception is thrown when trying to remove the metadata of a block which does not
   * exist.
   *
   * @throws Exception if removing the metadata of the block fails
   */
  @Test
  public void removeBlockMetaNotExistingTest() throws Exception {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(TEST_BLOCK_ID));
    mDir.removeBlockMeta(mBlockMeta);
  }

  /**
   * Tests that an exception is thrown when trying to get the metadata of a block which does not
   * exist.
   *
   * @throws Exception if getting the metadata of the block fails
   */
  @Test
  public void getBlockMetaNotExistingTest() throws Exception {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(TEST_BLOCK_ID));
    mDir.getBlockMeta(TEST_BLOCK_ID);
  }

  /**
   * Tests that an exception is thrown when trying to add the metadata of a temporary block which is
   * too big.
   *
   * @throws Exception if adding the metadata of the tempoary block fails
   */
  @Test
  public void addTempBlockMetaTooBigTest() throws Exception {
    final long bigBlockSize = TEST_DIR_CAPACITY + 1;
    TempBlockMeta bigTempBlockMeta =
        new TempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID, bigBlockSize, mDir);
    String alias = bigTempBlockMeta.getBlockLocation().tierAlias();
    mThrown.expect(WorkerOutOfSpaceException.class);
    mThrown.expectMessage(ExceptionMessage.NO_SPACE_FOR_BLOCK_META.getMessage(TEST_TEMP_BLOCK_ID,
        bigBlockSize, TEST_DIR_CAPACITY, alias));
    mDir.addTempBlockMeta(bigTempBlockMeta);
  }

  /**
   * Tests that an exception is thrown when trying to add the metadata of a termpoary block which
   * already exists.
   *
   * @throws Exception if adding the metadata of the temporary block fails
   */
  @Test
  public void addTempBlockMetaExistingTest() throws Exception {
    mThrown.expect(BlockAlreadyExistsException.class);
    mThrown
        .expectMessage(ExceptionMessage.ADD_EXISTING_BLOCK.getMessage(TEST_TEMP_BLOCK_ID, "MEM"));
    mDir.addTempBlockMeta(mTempBlockMeta);
    TempBlockMeta dupTempBlockMeta =
        new TempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID, TEST_TEMP_BLOCK_SIZE, mDir);
    mDir.addTempBlockMeta(dupTempBlockMeta);
  }

  /**
   * Tests that an exception is thrown when trying to remove the metadata of a tempoary block which
   * does not exist.
   *
   * @throws Exception if removing the metadata of the temporary block fails
   */
  @Test
  public void removeTempBlockMetaNotExistingTest() throws Exception {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(TEST_TEMP_BLOCK_ID));
    mDir.removeTempBlockMeta(mTempBlockMeta);
  }

  /**
   * Tests that an exception is thrown when trying to remove the metadata of a temporary block which
   * is not owned.
   *
   * @throws Exception if adding or removing the metadata of a temporary block fails
   */
  @Test
  public void removeTempBlockMetaNotOwnerTest() throws Exception {
    final long wrongSessionId = TEST_SESSION_ID + 1;
    String alias = "MEM";
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_NOT_FOUND_FOR_SESSION
        .getMessage(TEST_TEMP_BLOCK_ID, alias, wrongSessionId));
    mDir.addTempBlockMeta(mTempBlockMeta);
    TempBlockMeta wrongTempBlockMeta =
        new TempBlockMeta(wrongSessionId, TEST_TEMP_BLOCK_ID, TEST_TEMP_BLOCK_SIZE, mDir);
    mDir.removeTempBlockMeta(wrongTempBlockMeta);
  }

  /**
   * Tests that an exception is thrown when trying to get the metadata of a temporary block that
   * does not exist.
   *
   * @throws Exception if getting the metadata of the block fails
   */
  @Test
  public void getTempBlockMetaNotExistingTest() throws Exception {
    mThrown.expect(BlockDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.BLOCK_META_NOT_FOUND.getMessage(TEST_TEMP_BLOCK_ID));
    mDir.getBlockMeta(TEST_TEMP_BLOCK_ID);
  }

  /**
   * Tests the {@link StorageDir#addBlockMeta(BlockMeta)} and the
   * {@link StorageDir#removeBlockMeta(BlockMeta)} methods.
   *
   * @throws Exception if adding the temporary block metadata fails
   */
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

  /**
   * Tests the {@link StorageDir#addTempBlockMeta(TempBlockMeta)} and the
   * {@link StorageDir#removeTempBlockMeta(TempBlockMeta)} methods.
   *
   * @throws Exception if adding the temporary block metadata fails
   */
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

  /**
   * Tests the {@link StorageDir#resizeTempBlockMeta(TempBlockMeta, long)} method.
   *
   * @throws Exception if adding the temporary block metadata fails
   */
  @Test
  public void resizeTempBlockMetaTest() throws Exception {
    mDir.addTempBlockMeta(mTempBlockMeta);
    Assert.assertEquals(TEST_DIR_CAPACITY - TEST_TEMP_BLOCK_SIZE, mDir.getAvailableBytes());
    final long newSize = TEST_TEMP_BLOCK_SIZE + 10;
    mDir.resizeTempBlockMeta(mTempBlockMeta, newSize);
    Assert.assertEquals(TEST_DIR_CAPACITY - newSize, mDir.getAvailableBytes());
  }

  /**
   * Tests that an {@link InvalidWorkerStateException} is thrown when trying to shrink a block via
   * the {@link StorageDir#resizeTempBlockMeta(TempBlockMeta, long)} method.
   *
   * @throws Exception if adding the temporary block metadata fails
   */
  @Test
  public void resizeTempBlockMetaInvalidStateExceptionTest() throws Exception {
    mDir.addTempBlockMeta(mTempBlockMeta);
    final long newSize = TEST_TEMP_BLOCK_SIZE - 10;
    try {
      mDir.resizeTempBlockMeta(mTempBlockMeta, newSize);
      Assert.fail("Should throw an Exception when newSize is smaller than oldSize");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof InvalidWorkerStateException);
      Assert.assertTrue(e.getMessage().equals("Shrinking block, not supported!"));
      Assert.assertEquals(TEST_TEMP_BLOCK_SIZE, mTempBlockMeta.getBlockSize());
    }
  }

  /**
   * Tests that an exception is thrown when trying to resize a temporary block via the
   * {@link StorageDir#resizeTempBlockMeta(TempBlockMeta, long)} method without no available bytes.
   *
   * @throws Exception if an operation on the metadata of the temporary block fails
   */
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

  /**
   * Tests the {@link StorageDir#cleanupSessionTempBlocks(long, List)} method.
   *
   * @throws Exception if adding metadata of a block fails
   */
  @Test
  public void cleanupSessionTest() throws Exception {
    // TODO(bin): Also test claimed space.
    // Create blocks under TEST_SESSION_ID
    mDir.addBlockMeta(mBlockMeta);

    // Create temp blocks under TEST_SESSION_ID
    long tempBlockId1 = TEST_TEMP_BLOCK_ID + 1;
    long tempBlockId2 = TEST_TEMP_BLOCK_ID + 2;
    long tempBlockId3 = TEST_TEMP_BLOCK_ID + 3;
    long otherSessionId = TEST_SESSION_ID + 1;

    TempBlockMeta tempBlockMeta1 =
        new TempBlockMeta(TEST_SESSION_ID, tempBlockId1, TEST_TEMP_BLOCK_SIZE, mDir);
    TempBlockMeta tempBlockMeta2 =
        new TempBlockMeta(TEST_SESSION_ID, tempBlockId2, TEST_TEMP_BLOCK_SIZE, mDir);
    TempBlockMeta tempBlockMeta3 =
        new TempBlockMeta(otherSessionId, tempBlockId3, TEST_TEMP_BLOCK_SIZE, mDir);
    mDir.addTempBlockMeta(tempBlockMeta1);
    mDir.addTempBlockMeta(tempBlockMeta2);
    mDir.addTempBlockMeta(tempBlockMeta3);

    // Check the temporary blocks belonging to TEST_SESSION_ID
    List<TempBlockMeta> actual = mDir.getSessionTempBlocks(TEST_SESSION_ID);
    List<Long> actualBlockIds = new ArrayList<Long>(actual.size());
    for (TempBlockMeta tempBlockMeta : actual) {
      actualBlockIds.add(tempBlockMeta.getBlockId());
    }
    Assert.assertEquals(Sets.newHashSet(tempBlockMeta1, tempBlockMeta2),
        new HashSet<TempBlockMeta>(actual));
    Assert.assertTrue(mDir.hasTempBlockMeta(tempBlockId1));
    Assert.assertTrue(mDir.hasTempBlockMeta(tempBlockId2));

    // Two temp blocks created by TEST_SESSION_ID are expected to be removed
    mDir.cleanupSessionTempBlocks(TEST_SESSION_ID, actualBlockIds);
    Assert.assertFalse(mDir.hasTempBlockMeta(tempBlockId1));
    Assert.assertFalse(mDir.hasTempBlockMeta(tempBlockId2));
    // Temp block created by otherSessionId is expected to stay
    Assert.assertTrue(mDir.hasTempBlockMeta(tempBlockId3));
    // Block created by TEST_SESSION_ID is expected to stay
    Assert.assertTrue(mDir.hasBlockMeta(TEST_BLOCK_ID));
  }

  /**
   * Tests the {@link StorageDir#toBlockStoreLocation()} method.
   */
  @Test
  public void toBlockStoreLocationTest() {
    StorageTier tier = mDir.getParentTier();
    Assert.assertEquals(new BlockStoreLocation(tier.getTierAlias(), mDir.getDirIndex()),
        mDir.toBlockStoreLocation());
  }
}
