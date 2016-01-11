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

import tachyon.worker.WorkerContext;
import tachyon.worker.block.BlockStoreLocation;
import tachyon.worker.block.TieredBlockStoreTestUtils;

/**
 * Unit tests for {@link BlockMetaBase}.
 */
public class BlockMetaBaseTest {
  // This class extending BlockMetaBase is only for test purpose
  private class BlockMetaBaseForTest extends BlockMetaBase {
    public BlockMetaBaseForTest(long blockId, StorageDir dir) {
      super(blockId, dir);
    }

    @Override
    public long getBlockSize() {
      return 0;
    }

    @Override
    public String getPath() {
      return "";
    }
  }

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  private static final long TEST_BLOCK_ID = 9;
  private static final int TEST_TIER_ORDINAL = 0;
  private static final String TEST_TIER_ALIAS = "MEM";
  private static final long[] TEST_TIER_CAPACITY_BYTES = {100};
  private String mTestDirPath;
  private StorageTier mTier;
  private StorageDir mDir;
  private BlockMetaBaseForTest mBlockMeta;

  @Before
  public void before() throws Exception {
    mTestDirPath = mFolder.newFolder().getAbsolutePath();
    // Sets up tier with one storage dir under mTestDirPath with 100 bytes capacity.
    TieredBlockStoreTestUtils.setupTachyonConfWithSingleTier(null, TEST_TIER_ORDINAL,
        TEST_TIER_ALIAS, new String[] {mTestDirPath}, TEST_TIER_CAPACITY_BYTES, "");

    mTier = StorageTier.newStorageTier(TEST_TIER_ALIAS);
    mDir = mTier.getDir(0);
    mBlockMeta = new BlockMetaBaseForTest(TEST_BLOCK_ID, mDir);
  }

  @After
  public void after() {
    WorkerContext.reset();
  }

  @Test
  public void getBlockIdTest() {
    Assert.assertEquals(TEST_BLOCK_ID, mBlockMeta.getBlockId());
  }

  @Test
  public void getBlockLocationTest() {
    BlockStoreLocation expectedLocation =
        new BlockStoreLocation(mTier.getTierAlias(), mDir.getDirIndex());
    Assert.assertEquals(expectedLocation, mBlockMeta.getBlockLocation());
  }

  @Test
  public void getParentDirTest() {
    Assert.assertEquals(mDir, mBlockMeta.getParentDir());
  }
}
