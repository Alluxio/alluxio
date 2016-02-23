/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.meta;

import alluxio.worker.WorkerContext;
import alluxio.worker.block.TieredBlockStoreTestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.util.List;

/**
 * Unit tests for {@link StorageTier}.
 */
public class StorageTierTest {
  private static final long TEST_SESSION_ID = 2;
  private static final long TEST_TEMP_BLOCK_ID = 10;
  private static final long TEST_BLOCK_SIZE = 20;
  private static final long TEST_DIR1_CAPACITY = 2000;
  private static final long TEST_DIR2_CAPACITY = 3000;
  private static final int TEST_TIER_ORDINAL = 0;
  private static final String TEST_TIER_ALIAS = "MEM";

  private static final long[] TIER_CAPACITY_BYTES = {TEST_DIR1_CAPACITY, TEST_DIR2_CAPACITY};

  private StorageTier mTier;
  private StorageDir mDir1;
  private TempBlockMeta mTempBlockMeta;
  private String mTestDirPath1;
  private String mTestDirPath2;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up all dependencies before a test runs.
   *
   * @throws Exception if setting up a dependency fails
   */
  @Before
  public final void before() throws Exception {
    mTestDirPath1 = mFolder.newFolder().getAbsolutePath();
    mTestDirPath2 = mFolder.newFolder().getAbsolutePath();
    String[] tierPath = {mTestDirPath1, mTestDirPath2};

    TieredBlockStoreTestUtils.setupConfWithSingleTier(null, TEST_TIER_ORDINAL,
        TEST_TIER_ALIAS, tierPath, TIER_CAPACITY_BYTES, "");

    mTier = StorageTier.newStorageTier("MEM");
    mDir1 = mTier.getDir(0);
    mTempBlockMeta = new TempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, mDir1);
  }

  /**
   * Resets the context of the worker after a test ran.
   */
  @After
  public void after() {
    WorkerContext.reset();
  }

  /**
   * Tests the {@link StorageTier#getTierAlias()} method.
   */
  @Test
  public void getTierAliasTest() {
    Assert.assertEquals(TEST_TIER_ALIAS, mTier.getTierAlias());
  }

  /**
   * Tests the {@link StorageTier#getTierOrdinal()} method.
   */
  @Test
  public void getTierLevelTest() {
    Assert.assertEquals(TEST_TIER_ORDINAL, mTier.getTierOrdinal());
  }

  /**
   * Tests the {@link StorageTier#getCapacityBytes()} method.
   *
   * @throws Exception if adding the temporary block metadata fails
   */
  @Test
  public void getCapacityBytesTest() throws Exception {
    Assert.assertEquals(TEST_DIR1_CAPACITY + TEST_DIR2_CAPACITY, mTier.getCapacityBytes());

    // Capacity should not change after adding block to a dir.
    mDir1.addTempBlockMeta(mTempBlockMeta);
    Assert.assertEquals(TEST_DIR1_CAPACITY + TEST_DIR2_CAPACITY, mTier.getCapacityBytes());
  }

  /**
   * Tests the {@link StorageTier#getAvailableBytes()} method.
   *
   * @throws Exception if adding the temporary block metadata fails
   */
  @Test
  public void getAvailableBytesTest() throws Exception {
    Assert.assertEquals(TEST_DIR1_CAPACITY + TEST_DIR2_CAPACITY, mTier.getAvailableBytes());

    // Capacity should subtract block size after adding block to a dir.
    mDir1.addTempBlockMeta(mTempBlockMeta);
    Assert.assertEquals(TEST_DIR1_CAPACITY + TEST_DIR2_CAPACITY - TEST_BLOCK_SIZE,
        mTier.getAvailableBytes());
  }

  /**
   * Tests that an exception is thrown when trying to get a directory by a non-existing index.
   */
  @Test
  public void getDirTest() {
    mThrown.expect(IndexOutOfBoundsException.class);
    StorageDir dir1 = mTier.getDir(0);
    Assert.assertEquals(mTestDirPath1, dir1.getDirPath());
    StorageDir dir2 = mTier.getDir(1);
    Assert.assertEquals(mTestDirPath2, dir2.getDirPath());
    // Get dir by a non-existing index, expect getDir to fail and throw IndexOutOfBoundsException
    mTier.getDir(2);
  }

  /**
   * Tests the {@link StorageTier#getStorageDirs()} method.
   */
  @Test
  public void getStorageDirsTest() {
    List<StorageDir> dirs = mTier.getStorageDirs();
    Assert.assertEquals(2, dirs.size());
    Assert.assertEquals(mTestDirPath1, dirs.get(0).getDirPath());
    Assert.assertEquals(mTestDirPath2, dirs.get(1).getDirPath());
  }
}
