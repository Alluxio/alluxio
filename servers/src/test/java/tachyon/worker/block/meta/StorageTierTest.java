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
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import tachyon.Constants;
import tachyon.StorageLevelAlias;
import tachyon.conf.TachyonConf;

public class StorageTierTest {
  private static final long TEST_USER_ID = 2;
  private static final long TEST_TEMP_BLOCK_ID = 10;
  private static final long TEST_BLOCK_SIZE = 20;
  private static final String TEST_DIR1_PATH = "/mnt/ramdisk/0/";
  private static final String TEST_DIR2_PATH = "/mnt/ramdisk/1/";
  private static final long TEST_DIR1_CAPACITY = 2000;
  private static final long TEST_DIR2_CAPACITY = 3000;
  private static final int TEST_TIER_LEVEL = 0;
  private static final int TEST_TIER_ALIAS = StorageLevelAlias.MEM.getValue();
  private StorageTier mTier;
  private StorageDir mDir1;
  private TempBlockMeta mTempBlockMeta;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public final void before() throws Exception {
    TachyonConf tachyonConf = new TachyonConf();
    tachyonConf.set(
        String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_ALIAS_FORMAT, TEST_TIER_LEVEL), "MEM");
    tachyonConf.set(
        String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_PATH_FORMAT, TEST_TIER_LEVEL),
        TEST_DIR1_PATH + "," + TEST_DIR2_PATH);
    tachyonConf.set(
        String.format(Constants.WORKER_TIERED_STORAGE_LEVEL_DIRS_QUOTA_FORMAT, TEST_TIER_LEVEL),
        TEST_DIR1_CAPACITY + "," + TEST_DIR2_CAPACITY);

    mTier = new StorageTier(tachyonConf, TEST_TIER_LEVEL);
    mDir1 = mTier.getDir(0);
    mTempBlockMeta = new TempBlockMeta(TEST_USER_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, mDir1);

  }

  @Test
  public void getTierAliasTest() {
    Assert.assertEquals(TEST_TIER_ALIAS, mTier.getTierAlias());
  }

  @Test
  public void getTierLevelTest() {
    Assert.assertEquals(TEST_TIER_LEVEL, mTier.getTierLevel());
  }

  @Test
  public void getCapacityBytesTest() throws IOException {
    Assert.assertEquals(TEST_DIR1_CAPACITY + TEST_DIR2_CAPACITY, mTier.getCapacityBytes());

    // Capacity should not change after adding block to a dir.
    mDir1.addTempBlockMeta(mTempBlockMeta);
    Assert.assertEquals(TEST_DIR1_CAPACITY + TEST_DIR2_CAPACITY, mTier.getCapacityBytes());
  }

  @Test
  public void getAvailableBytesTest() throws IOException {
    Assert.assertEquals(TEST_DIR1_CAPACITY + TEST_DIR2_CAPACITY, mTier.getAvailableBytes());

    // Capacity should subtract block size after adding block to a dir.
    mDir1.addTempBlockMeta(mTempBlockMeta);
    Assert.assertEquals(TEST_DIR1_CAPACITY + TEST_DIR2_CAPACITY - TEST_BLOCK_SIZE,
        mTier.getAvailableBytes());
  }

  @Test
  public void getDirTest() {
    mThrown.expect(IndexOutOfBoundsException.class);
    StorageDir dir1 = mTier.getDir(0);
    Assert.assertEquals(TEST_DIR1_PATH, dir1.getDirPath());
    StorageDir dir2 = mTier.getDir(1);
    Assert.assertEquals(TEST_DIR2_PATH, dir2.getDirPath());
    // Get dir by a non-existing index, expect getDir to fail and throw IndexOutOfBoundsException
    mTier.getDir(2);
  }

  @Test
  public void getStorageDirsTest() {
    List<StorageDir> dirs = mTier.getStorageDirs();
    Assert.assertEquals(2, dirs.size());
    Assert.assertEquals(TEST_DIR1_PATH, dirs.get(0).getDirPath());
    Assert.assertEquals(TEST_DIR2_PATH, dirs.get(1).getDirPath());
  }
}
