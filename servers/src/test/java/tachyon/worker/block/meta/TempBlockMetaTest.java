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

import tachyon.conf.TachyonConf;
import tachyon.util.CommonUtils;

public class TempBlockMetaTest {
  private static final long TEST_USER_ID = 2;
  private static final long TEST_BLOCK_ID = 9;
  private static final long TEST_BLOCK_SIZE = 100;
  private String mTestDirPath;
  private TempBlockMeta mTempBlockMeta;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    TachyonConf tachyonConf = new TachyonConf();
    StorageTier tier = new StorageTier(tachyonConf, 0 /* level */);
    mTestDirPath = mFolder.newFolder().getAbsolutePath();
    StorageDir dir = new StorageDir(tier, 0 /* index */, 100 /* capacity */, mTestDirPath);
    mTempBlockMeta = new TempBlockMeta(TEST_USER_ID, TEST_BLOCK_ID, TEST_BLOCK_SIZE, dir);
  }

  @Test
  public void getPathTest() {
    Assert.assertEquals(CommonUtils.concatPath(mTestDirPath, TEST_USER_ID, TEST_BLOCK_ID),
        mTempBlockMeta.getPath());
  }

  @Test
  public void getCommitPathTest() {
    Assert.assertEquals(CommonUtils.concatPath(mTestDirPath, TEST_BLOCK_ID),
        mTempBlockMeta.getCommitPath());
  }

  @Test
  public void getUserIdTest() {
    Assert.assertEquals(TEST_USER_ID, mTempBlockMeta.getUserId());
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
