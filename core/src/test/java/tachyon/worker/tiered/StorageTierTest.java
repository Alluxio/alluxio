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
package tachyon.worker.tiered;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.StorageLevelAlias;
import tachyon.TestUtils;
import tachyon.conf.TachyonConf;
import tachyon.thrift.InvalidPathException;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;
import tachyon.worker.BlockHandler;

public class StorageTierTest {

  private static final long USER_ID = 1;

  private StorageTier[] mStorageTiers;

  @Before
  public final void before() throws IOException, InvalidPathException {
    String tachyonHome =
        File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.currentTimeMillis();

    final int maxLevel = 2;

    TachyonConf tachyonConf = new TachyonConf();
    tachyonConf.set(Constants.TACHYON_HOME, tachyonHome);

    // Setup conf for worker
    tachyonConf.set(Constants.WORKER_MAX_TIERED_STORAGE_LEVEL, Integer.toString(maxLevel));
    tachyonConf.set("tachyon.worker.tieredstore.level0.alias", "MEM");
    tachyonConf.set("tachyon.worker.tieredstore.level0.dirs.path", tachyonHome + "/ramdisk");
    tachyonConf.set("tachyon.worker.tieredstore.level0.dirs.quota", 1000 + "");
    tachyonConf.set("tachyon.worker.tieredstore.level1.alias", "HDD");
    tachyonConf.set("tachyon.worker.tieredstore.level1.dirs.path", tachyonHome + "/disk1,"
        + tachyonHome + "/disk2");
    tachyonConf.set("tachyon.worker.tieredstore.level1.dirs.quota", 4000 + "," + 4000);

    mStorageTiers = new StorageTier[maxLevel];
    StorageTier nextTier = null;
    for (int level = maxLevel - 1; level >= 0; level --) {
      String tierLevelAliasProp = "tachyon.worker.tieredstore.level" + level + ".alias";
      String tierLevelDirPath = "tachyon.worker.tieredstore.level" + level + ".dirs.path";
      StorageLevelAlias storageLevelAlias = tachyonConf.getEnum(tierLevelAliasProp,
          StorageLevelAlias.MEM);
      String tierDirsCapacityProp = "tachyon.worker.tieredstore.level" + level + ".dirs.quota";
      int index = level;
      if (index >= Constants.DEFAULT_STORAGE_TIER_DIR_QUOTA.length) {
        index = level - 1;
      }
      String tierDirsCapacity = tachyonConf.get(tierDirsCapacityProp,
          Constants.DEFAULT_STORAGE_TIER_DIR_QUOTA[index]);

      String[] dirPaths = tachyonConf.get(tierLevelDirPath, "/mnt/ramdisk").split(",");
      for (int i = 0; i < dirPaths.length; i ++) {
        dirPaths[i] = dirPaths[i].trim();
      }
      String[] strDirCapacities = tierDirsCapacity.split(",");
      long[] dirCapacities = new long[dirPaths.length];
      for (int i = 0, j = 0; i < dirPaths.length; i ++) {
        // The storage directory quota for each storage directory
        dirCapacities[i] = CommonUtils.parseSpaceSize(strDirCapacities[j].trim());
        if (j < strDirCapacities.length - 1) {
          j ++;
        }
      }
      StorageTier curTier =
          new StorageTier(level, storageLevelAlias, dirPaths, dirCapacities, "/data", "/user",
              nextTier, null, tachyonConf);
      mStorageTiers[level] = curTier;
      curTier.initialize();
      for (StorageDir dir : curTier.getStorageDirs()) {
        initializeStorageDir(dir, USER_ID);
      }
      nextTier = curTier;
    }
  }

  private void createBlockFile(StorageDir dir, long blockId, int blockSize) throws IOException {
    byte[] buf = TestUtils.getIncreasingByteArray(blockSize);
    BlockHandler bhSrc =
        BlockHandler.get(dir.getUserTempFilePath(USER_ID, blockId));
    dir.requestSpace(USER_ID, blockSize);
    dir.updateTempBlockAllocatedBytes(USER_ID, blockId, blockSize);
    try {
      bhSrc.append(0, ByteBuffer.wrap(buf));
    } finally {
      bhSrc.close();
    }
    dir.cacheBlock(USER_ID, blockId);
  }

  @Test
  public void getStorageDirTest() throws IOException {
    long blockId = 1;
    StorageDir dir = mStorageTiers[0].requestSpace(USER_ID, 100, new HashSet<Integer>());
    createBlockFile(dir, blockId, 100);
    StorageDir dir1 = mStorageTiers[0].getStorageDirByBlockId(1);
    Assert.assertEquals(dir, dir1);
    dir1 = mStorageTiers[0].getStorageDirByBlockId(2);
    Assert.assertEquals(null, dir1);
    dir = mStorageTiers[1].getStorageDirByIndex(1);
    Assert.assertEquals(mStorageTiers[1].getStorageDirs()[1], dir);
    dir1 = mStorageTiers[1].getStorageDirByIndex(2);
    Assert.assertEquals(null, dir1);
  }

  private void initializeStorageDir(StorageDir dir, long userId) throws IOException {
    UnderFileSystem ufs = dir.getUfs();
    ufs.mkdirs(dir.getUserTempPath(userId), true);
    CommonUtils.changeLocalFileToFullPermission(dir.getUserTempPath(userId));
  }

  @Test
  public void isLastTierTest() {
    Assert.assertEquals(false, mStorageTiers[0].isLastTier());
    Assert.assertEquals(true, mStorageTiers[1].isLastTier());
  }

  @Test
  public void requestSpaceTest() throws IOException {
    long blockId = 1;
    Assert.assertEquals(1000, mStorageTiers[0].getCapacityBytes());
    Assert.assertEquals(8000, mStorageTiers[1].getCapacityBytes());
    StorageDir dir = mStorageTiers[0].requestSpace(USER_ID, 500, new HashSet<Integer>());
    dir.updateTempBlockAllocatedBytes(USER_ID, blockId, 500);
    Assert.assertEquals(mStorageTiers[0].getStorageDirs()[0], dir);
    Assert.assertEquals(500, dir.getAvailableBytes());
    Assert.assertEquals(500, dir.getUsedBytes());
    StorageDir dir1 = mStorageTiers[0].requestSpace(USER_ID, 501, new HashSet<Integer>());
    Assert.assertEquals(null, dir1);
    createBlockFile(dir, blockId, 500);
    boolean request = mStorageTiers[0].requestSpace(dir, USER_ID, 501, new HashSet<Integer>());
    Assert.assertTrue(request);
    Assert.assertEquals(499, dir.getAvailableBytes());
    Assert.assertEquals(501, dir.getUsedBytes());
    Assert.assertTrue(mStorageTiers[1].containsBlock(blockId));
    Assert.assertEquals(500, mStorageTiers[1].getUsedBytes());
    request = mStorageTiers[0].requestSpace(dir, USER_ID, 500, new HashSet<Integer>());
    Assert.assertEquals(false, request);
  }
}
