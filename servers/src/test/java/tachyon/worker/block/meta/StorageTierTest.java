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

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import tachyon.StorageLevelAlias;
import tachyon.worker.block.TieredBlockStoreTestUtils;

public class StorageTierTest {
  private static final long TEST_SESSION_ID = 2;
  private static final long TEST_TEMP_BLOCK_ID = 10;
  private static final long TEST_BLOCK_SIZE = 20;
  private static final long TEST_DIR1_CAPACITY = 2000;
  private static final long TEST_DIR2_CAPACITY = 3000;
  private static final int TEST_TIER_LEVEL = 0;
  private static final StorageLevelAlias TEST_TIER_ALIAS = StorageLevelAlias.MEM;

  private static final long[] TIER_CAPACITY_BYTES = {TEST_DIR1_CAPACITY, TEST_DIR2_CAPACITY};

  private static String sTestDirPath1;
  private static String sTestDirPath2;

  private StorageTier mTier;
  private StorageDir mDir1;
  private TempBlockMeta mTempBlockMeta;

  @ClassRule
  public static TemporaryFolder sFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @BeforeClass
  public static void setupTieredStorage() throws Exception {
    sTestDirPath1 = sFolder.newFolder().getAbsolutePath();
    sTestDirPath2 = sFolder.newFolder().getAbsolutePath();
    String[] tierPath = {sTestDirPath1, sTestDirPath2};

    TieredBlockStoreTestUtils.setupTachyonConfWithSingleTier(null, TEST_TIER_LEVEL,
        TEST_TIER_ALIAS, tierPath, TIER_CAPACITY_BYTES, "");
  }

  @Before
  public void before() throws Exception {
    mTier = StorageTier.newStorageTier(TEST_TIER_LEVEL);
    mDir1 = mTier.getDir(0);
    mTempBlockMeta = new TempBlockMeta(TEST_SESSION_ID, TEST_TEMP_BLOCK_ID, TEST_BLOCK_SIZE, mDir1);
  }

  @Test
  public void getTierAliasTest() {
    Assert.assertEquals(TEST_TIER_ALIAS.getValue(), mTier.getTierAlias());
  }

  @Test
  public void getTierLevelTest() {
    Assert.assertEquals(TEST_TIER_LEVEL, mTier.getTierLevel());
  }

  @Test
  public void getCapacityBytesTest() throws Exception {
    Assert.assertEquals(TEST_DIR1_CAPACITY + TEST_DIR2_CAPACITY, mTier.getCapacityBytes());

    // Capacity should not change after adding block to a dir.
    mDir1.addTempBlockMeta(mTempBlockMeta);
    Assert.assertEquals(TEST_DIR1_CAPACITY + TEST_DIR2_CAPACITY, mTier.getCapacityBytes());
  }

  @Test
  public void getAvailableBytesTest() throws Exception {
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
    Assert.assertEquals(sTestDirPath1, dir1.getDirPath());
    StorageDir dir2 = mTier.getDir(1);
    Assert.assertEquals(sTestDirPath2, dir2.getDirPath());
    // Get dir by a non-existing index, expect getDir to fail and throw IndexOutOfBoundsException
    mTier.getDir(2);
  }

  @Test
  public void getStorageDirsTest() {
    List<StorageDir> dirs = mTier.getStorageDirs();
    Assert.assertEquals(2, dirs.size());
    Assert.assertEquals(sTestDirPath1, dirs.get(0).getDirPath());
    Assert.assertEquals(sTestDirPath2, dirs.get(1).getDirPath());
  }
}
