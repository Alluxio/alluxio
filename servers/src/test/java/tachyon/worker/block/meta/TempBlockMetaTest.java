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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import tachyon.util.io.PathUtils;
import tachyon.worker.WorkerContext;
import tachyon.worker.block.TieredBlockStoreTestUtils;

/**
 * Unit tests for {@link TempBlockMeta}.
 */
public class TempBlockMetaTest {
  private static final long TEST_SESSION_ID = 2;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_BLOCK_SIZE = 100;
  private static final int TEST_TIER_ORDINAL = 0;
  private static final String TEST_TIER_ALIAS = "MEM";
  private static final long[] TEST_TIER_CAPACITY_BYTES = {100};
  private String mTestDirPath;
  private TempBlockMeta mTempBlockMeta;

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
    TieredBlockStoreTestUtils.setupTachyonConfWithSingleTier(null, TEST_TIER_ORDINAL,
        TEST_TIER_ALIAS, new String[] {mTestDirPath}, TEST_TIER_CAPACITY_BYTES, "");

    StorageTier tier = StorageTier.newStorageTier(TEST_TIER_ALIAS);
    StorageDir dir = tier.getDir(0);
    mTempBlockMeta = new TempBlockMeta(TEST_SESSION_ID, TEST_BLOCK_ID, TEST_BLOCK_SIZE, dir);
  }

  /**
   * Resets the context of the worker after a test ran.
   */
  @After
  public void after() {
    WorkerContext.reset();
  }

  /**
   * Tests the {@link TempBlockMeta#getPath()} method.
   */
  @Test
  public void getPathTest() {
    Assert.assertEquals(PathUtils.concatPath(mTestDirPath, TEST_SESSION_ID, TEST_BLOCK_ID),
        mTempBlockMeta.getPath());
  }

  /**
   * Tests the {@link TempBlockMeta#getCommitPath()} method.
   */
  @Test
  public void getCommitPathTest() {
    Assert.assertEquals(PathUtils.concatPath(mTestDirPath, TEST_BLOCK_ID),
        mTempBlockMeta.getCommitPath());
  }

  /**
   * Tests the {@link TempBlockMeta#getSessionId()} method.
   */
  @Test
  public void getSessionIdTest() {
    Assert.assertEquals(TEST_SESSION_ID, mTempBlockMeta.getSessionId());
  }

  /**
   * Tests the {@link TempBlockMeta#setBlockSize(long)} method.
   */
  @Test
  public void setBlockSizeTest() {
    Assert.assertEquals(TEST_BLOCK_SIZE, mTempBlockMeta.getBlockSize());
    mTempBlockMeta.setBlockSize(1);
    Assert.assertEquals(1, mTempBlockMeta.getBlockSize());
    mTempBlockMeta.setBlockSize(100);
    Assert.assertEquals(100, mTempBlockMeta.getBlockSize());
  }
}
