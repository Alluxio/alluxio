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

import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;
import alluxio.worker.WorkerContext;
import alluxio.worker.block.TieredBlockStoreTestUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Unit tests for {@link BlockMeta}.
 */
public class BlockMetaTest {
  private static final long TEST_SESSION_ID = 2;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_BLOCK_SIZE = 100;
  private static final int TEST_TIER_ORDINAL = 0;
  private static final String TEST_TIER_ALIAS = "MEM";
  private static final long[] TEST_TIER_CAPACITY_BYTES = {100};
  private StorageDir mDir;
  private BlockMeta mBlockMeta;
  private TempBlockMeta mTempBlockMeta;
  private String mTestDirPath;

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
    mTestDirPath = mFolder.newFolder().getAbsolutePath();
    // Sets up tier with one storage dir under mTestDirPath with 100 bytes capacity.
    TieredBlockStoreTestUtils.setupConfWithSingleTier(null, TEST_TIER_ORDINAL,
        TEST_TIER_ALIAS, new String[] {mTestDirPath}, TEST_TIER_CAPACITY_BYTES, "");

    StorageTier tier = StorageTier.newStorageTier(TEST_TIER_ALIAS);
    mDir = tier.getDir(0);
    mTempBlockMeta = new TempBlockMeta(TEST_SESSION_ID, TEST_BLOCK_ID, TEST_BLOCK_SIZE, mDir);
  }

  /**
   * Resets the context of the worker after a test ran.
   */
  @After
  public void after() {
    WorkerContext.reset();
  }

  /**
   * Tests the {@link BlockMeta#getBlockSize()} method.
   *
   * @throws IOException if writting the buffer to the file fails
   */
  @Test
  public void getBlockSizeTest() throws IOException {
    // With the block file not really existing, expect committed block size to be zero.
    mBlockMeta = new BlockMeta(mTempBlockMeta);
    Assert.assertEquals(0, mBlockMeta.getBlockSize());

    // With the block file partially written, expect committed block size equals real file size.
    byte[] buf = BufferUtils.getIncreasingByteArray((int) TEST_BLOCK_SIZE - 1);
    BufferUtils.writeBufferToFile(mTempBlockMeta.getCommitPath(), buf);
    mBlockMeta = new BlockMeta(mTempBlockMeta);
    Assert.assertEquals(TEST_BLOCK_SIZE - 1, mBlockMeta.getBlockSize());

    // With the block file fully written, expect committed block size equals target block size.
    buf = BufferUtils.getIncreasingByteArray((int) TEST_BLOCK_SIZE);
    BufferUtils.writeBufferToFile(mTempBlockMeta.getCommitPath(), buf);
    mBlockMeta = new BlockMeta(mTempBlockMeta);
    Assert.assertEquals(TEST_BLOCK_SIZE, mBlockMeta.getBlockSize());
  }

  /**
   * Tests the {@link BlockMeta#getPath()} method.
   */
  @Test
  public void getPathTest() {
    mBlockMeta = new BlockMeta(mTempBlockMeta);
    Assert.assertEquals(PathUtils.concatPath(mTestDirPath, TEST_BLOCK_ID), mBlockMeta.getPath());
  }
}
