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

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import tachyon.StorageLevelAlias;
import tachyon.util.io.PathUtils;
import tachyon.worker.block.TieredBlockStoreTestUtils;

public class TempBlockMetaTest {
  private static final long TEST_SESSION_ID = 2;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_BLOCK_SIZE = 100;
  private static final int TEST_TIER_LEVEL = 0;
  private static final StorageLevelAlias TEST_TIER_ALIAS = StorageLevelAlias.MEM;
  private static final long[] TEST_TIER_CAPACITY_BYTES = {100};
  private static String sTestDirPath;
  private TempBlockMeta mTempBlockMeta;

  @ClassRule
  public static TemporaryFolder sFolder = new TemporaryFolder();

  @BeforeClass
  public static void setupTieredStorage() throws Exception {
    sTestDirPath = sFolder.newFolder().getAbsolutePath();
    // Sets up tier with one storage dir under sTestDirPath with 100 bytes capacity.
    TieredBlockStoreTestUtils.setupTachyonConfWithSingleTier(null, TEST_TIER_LEVEL,
        TEST_TIER_ALIAS, new String[] {sTestDirPath}, TEST_TIER_CAPACITY_BYTES, "");
  }

  @Before
  public void before() throws Exception {
    StorageTier tier = StorageTier.newStorageTier(TEST_TIER_LEVEL);
    StorageDir dir = tier.getDir(0);
    mTempBlockMeta = new TempBlockMeta(TEST_SESSION_ID, TEST_BLOCK_ID, TEST_BLOCK_SIZE, dir);
  }

  @Test
  public void getPathTest() {
    Assert.assertEquals(PathUtils.concatPath(sTestDirPath, TEST_SESSION_ID, TEST_BLOCK_ID),
        mTempBlockMeta.getPath());
  }

  @Test
  public void getCommitPathTest() {
    Assert.assertEquals(PathUtils.concatPath(sTestDirPath, TEST_BLOCK_ID),
        mTempBlockMeta.getCommitPath());
  }

  @Test
  public void getSessionIdTest() {
    Assert.assertEquals(TEST_SESSION_ID, mTempBlockMeta.getSessionId());
  }

  @Test
  public void setBlockSizeTest() {
    Assert.assertEquals(TEST_BLOCK_SIZE, mTempBlockMeta.getBlockSize());
    mTempBlockMeta.setBlockSize(1);
    Assert.assertEquals(1, mTempBlockMeta.getBlockSize());
    mTempBlockMeta.setBlockSize(100);
    Assert.assertEquals(100, mTempBlockMeta.getBlockSize());
  }
}
