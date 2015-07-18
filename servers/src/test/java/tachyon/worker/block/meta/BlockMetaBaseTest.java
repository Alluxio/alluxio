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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.worker.block.BlockStoreLocation;

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
  private String mTestDirPath;
  private StorageTier mTier;
  private StorageDir mDir;
  private BlockMetaBaseForTest mBlockMeta;

  @Before
  public void before() throws IOException {
    mTestDirPath = mFolder.newFolder().getAbsolutePath();
    // Set up tier with one storage dir under mTestDirPath with 100 bytes capacity.
    TachyonConf tachyonConf = new TachyonConf();
    tachyonConf.set("tachyon.worker.tieredstore.level0.dirs.path", mTestDirPath);
    tachyonConf.set("tachyon.worker.tieredstore.level0.dirs.quota", "100b");
    tachyonConf.set(Constants.WORKER_DATA_FOLDER, "");

    mTier = StorageTier.newStorageTier(tachyonConf, 0 /* level */);
    mDir = mTier.getDir(0);
    mBlockMeta = new BlockMetaBaseForTest(TEST_BLOCK_ID, mDir);
  }

  @Test
  public void getBlockIdTest() {
    Assert.assertEquals(TEST_BLOCK_ID, mBlockMeta.getBlockId());
  }

  @Test
  public void getBlockLocationTest() {
    BlockStoreLocation expectedLocation =
        new BlockStoreLocation(mTier.getTierAlias(), mTier.getTierLevel(), mDir.getDirIndex());
    Assert.assertEquals(expectedLocation, mBlockMeta.getBlockLocation());
  }

  @Test
  public void getParentDirTest() {
    Assert.assertEquals(mDir, mBlockMeta.getParentDir());
  }
}
