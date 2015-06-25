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

import tachyon.TestUtils;
import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

public class BlockMetaTest {
  private static final long TEST_USER_ID = 2;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_BLOCK_SIZE = 100;
  private StorageDir mDir;
  private BlockMeta mBlockMeta;
  private String mTestDirPath;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    TachyonConf tachyonConf = new TachyonConf();
    StorageTier tier = new StorageTier(tachyonConf, 0 /* level */);
    mTestDirPath = mFolder.newFolder().getAbsolutePath();
    mDir = new StorageDir(tier, 0 /* index */, 100 /* capacity */, mTestDirPath);
  }

  @Test
  public void getBlockSizeTest() throws IOException {
    TempBlockMeta tempBlockMeta =
        new TempBlockMeta(TEST_USER_ID, TEST_BLOCK_ID, TEST_BLOCK_SIZE, mDir);

    // With the block file not really existing, expect committed block size to be zero.
    mBlockMeta = new BlockMeta(tempBlockMeta);
    Assert.assertEquals(0, mBlockMeta.getBlockSize());

    // With the block file partially written, expect committed block size equals real file size.
    long expectedBlockSize = TEST_BLOCK_SIZE / 2;
    byte[] buf = TestUtils.getIncreasingByteArray((int) expectedBlockSize);
    TestUtils.writeBufferToFile(tempBlockMeta.getCommitPath(), buf);
    mBlockMeta = new BlockMeta(tempBlockMeta);
    Assert.assertEquals(expectedBlockSize, mBlockMeta.getBlockSize());

    // With the block file fully written, expect committed block size equals target block size.
    expectedBlockSize = TEST_BLOCK_SIZE;
    buf = TestUtils.getIncreasingByteArray((int) expectedBlockSize);
    TestUtils.writeBufferToFile(tempBlockMeta.getCommitPath(), buf);
    mBlockMeta = new BlockMeta(tempBlockMeta);
    Assert.assertEquals(expectedBlockSize, mBlockMeta.getBlockSize());
  }

  @Test
  public void getPathTest() {
    TempBlockMeta tempBlockMeta =
        new TempBlockMeta(TEST_USER_ID, TEST_BLOCK_ID, TEST_BLOCK_SIZE, mDir);
    mBlockMeta = new BlockMeta(tempBlockMeta);
    Assert.assertEquals(CommonUtils.concatPath(mTestDirPath, TEST_BLOCK_ID), mBlockMeta.getPath());
  }
}
