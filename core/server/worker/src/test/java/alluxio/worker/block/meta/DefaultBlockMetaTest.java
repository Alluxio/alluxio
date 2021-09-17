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

package alluxio.worker.block.meta;

import alluxio.Constants;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.TieredBlockStoreTestUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Unit tests for {@link DefaultBlockMeta}.
 */
public class DefaultBlockMetaTest {
  private static final long TEST_SESSION_ID = 2;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_BLOCK_SIZE = 100;
  private static final int TEST_TIER_ORDINAL = 0;
  private static final String TEST_TIER_ALIAS = Constants.MEDIUM_MEM;
  private static final long[] TEST_TIER_CAPACITY_BYTES = {100};
  private static final String[] TEST_TIER_MEDIUM_TYPES = {Constants.MEDIUM_MEM};
  private static final String TEST_WORKER_DIR = "testworker";
  private BlockMeta mBlockMeta;
  private TempBlockMeta mTempBlockMeta;
  private String mTestDirPath;
  private String mTestBlockDirPath;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    mTestDirPath = mFolder.newFolder().getAbsolutePath();
    // Sets up tier with one storage dir under mTestDirPath with 100 bytes capacity.
    TieredBlockStoreTestUtils.setupConfWithSingleTier(null, TEST_TIER_ORDINAL,
        TEST_TIER_ALIAS, new String[] {mTestDirPath}, TEST_TIER_CAPACITY_BYTES,
        TEST_TIER_MEDIUM_TYPES, TEST_WORKER_DIR);

    mTestBlockDirPath = PathUtils.concatPath(mTestDirPath, TEST_WORKER_DIR);
    StorageTier tier = DefaultStorageTier.newStorageTier(TEST_TIER_ALIAS, false);
    StorageDir dir = tier.getDir(0);
    mTempBlockMeta = new DefaultTempBlockMeta(TEST_SESSION_ID, TEST_BLOCK_ID, TEST_BLOCK_SIZE, dir);
  }

  /**
   * Tests the {@link BlockMeta#getBlockSize()} method.
   */
  @Test
  public void getBlockSize() throws IOException {
    // With the block file not really existing, expect committed block size to be zero.
    mBlockMeta = new DefaultBlockMeta(mTempBlockMeta);
    Assert.assertEquals(0, mBlockMeta.getBlockSize());

    // With the block file partially written, expect committed block size equals real file size.
    byte[] buf = BufferUtils.getIncreasingByteArray((int) TEST_BLOCK_SIZE - 1);
    BufferUtils.writeBufferToFile(mTempBlockMeta.getCommitPath(), buf);
    mBlockMeta = new DefaultBlockMeta(mTempBlockMeta);
    Assert.assertEquals(TEST_BLOCK_SIZE - 1, mBlockMeta.getBlockSize());

    // With the block file fully written, expect committed block size equals target block size.
    buf = BufferUtils.getIncreasingByteArray((int) TEST_BLOCK_SIZE);
    BufferUtils.writeBufferToFile(mTempBlockMeta.getCommitPath(), buf);
    mBlockMeta = new DefaultBlockMeta(mTempBlockMeta);
    Assert.assertEquals(TEST_BLOCK_SIZE, mBlockMeta.getBlockSize());
  }

  /**
   * Tests the {@link BlockMeta#getPath()} method.
   */
  @Test
  public void getPath() {
    mBlockMeta = new DefaultBlockMeta(mTempBlockMeta);
    Assert.assertEquals(PathUtils.concatPath(mTestBlockDirPath, TEST_BLOCK_ID),
        mBlockMeta.getPath());
  }
}
