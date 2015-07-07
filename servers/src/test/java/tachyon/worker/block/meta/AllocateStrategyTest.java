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

// TODO: Reenable this test
/*
package tachyon.worker.block.meta;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.TestUtils;
import tachyon.conf.TachyonConf;
import tachyon.worker.block.io.BlockHandler;
import tachyon.worker.WorkerSource;
import tachyon.worker.block.allocator.AllocateStrategies;
import tachyon.worker.block.allocator.AllocateStrategy;
import tachyon.worker.block.allocator.AllocateStrategyType;
import tachyon.master.BlockInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;

public class AllocateStrategyTest {
  private final StorageDir[] mStorageDirs = new StorageDir[3];
  private static final long USER_ID = 1;
  private static final long[] CAPACITIES = new long[] {1000, 1100, 1200};

  @Before
  public final void before() throws IOException {
    String tachyonHome =
        File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.currentTimeMillis();
    String workerDirFolder = tachyonHome + "/ramdisk";
    String[] dirPaths = "/dir1,/dir2,/dir3".split(",");
    for (int i = 0; i < 3; i ++) {
      mStorageDirs[i] =
          new StorageDir(i + 1, workerDirFolder + dirPaths[i], CAPACITIES[i], "/data", "/user",
              null, new TachyonConf(), new WorkerSource(null));
      initializeStorageDir(mStorageDirs[i], USER_ID);
    }
  }

  private void createBlockFile(StorageDir dir, long blockId, int blockSize) throws IOException {
    byte[] buf = TestUtils.getIncreasingByteArray(blockSize);
    BlockHandler bhSrc = BlockHandler.get(dir.getUserTempFilePath(USER_ID, blockId));
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
  public void AllocateMaxFreeTest() throws IOException {
    AllocateStrategy allocator =
        AllocateStrategies.getAllocateStrategy(AllocateStrategyType.MAX_FREE);
    StorageDir storageDir = allocator.getStorageDir(mStorageDirs, USER_ID, 300);
    Assert.assertEquals(3, storageDir.getStorageDirId());
    createBlockFile(storageDir, BlockInfo.computeBlockId(1, 0), 300);
    storageDir = allocator.getStorageDir(mStorageDirs, USER_ID, 300);
    Assert.assertEquals(2, storageDir.getStorageDirId());
    createBlockFile(storageDir, BlockInfo.computeBlockId(2, 0), 300);
    storageDir = allocator.getStorageDir(mStorageDirs, USER_ID, 300);
    Assert.assertEquals(1, storageDir.getStorageDirId());
    createBlockFile(storageDir, BlockInfo.computeBlockId(3, 0), 300);
    storageDir = allocator.getStorageDir(mStorageDirs, USER_ID, 1000);
    Assert.assertEquals(null, storageDir);
    boolean fitIn = allocator.fitInPossible(mStorageDirs, 1200);
    Assert.assertEquals(true, fitIn);
    mStorageDirs[2].lockBlock(BlockInfo.computeBlockId(1, 0), USER_ID);
    fitIn = allocator.fitInPossible(mStorageDirs, 1200);
    Assert.assertEquals(false, fitIn);
  }

  @Test
  public void AllocateRoundRobinTest() throws IOException {
    AllocateStrategy allocator =
        AllocateStrategies.getAllocateStrategy(AllocateStrategyType.ROUND_ROBIN);
    StorageDir storageDir = allocator.getStorageDir(mStorageDirs, USER_ID, 300);
    Assert.assertEquals(1, storageDir.getStorageDirId());
    createBlockFile(storageDir, BlockInfo.computeBlockId(1, 0), 300);
    storageDir = allocator.getStorageDir(mStorageDirs, USER_ID, 300);
    Assert.assertEquals(2, storageDir.getStorageDirId());
    createBlockFile(storageDir, BlockInfo.computeBlockId(2, 0), 300);
    storageDir = allocator.getStorageDir(mStorageDirs, USER_ID, 300);
    Assert.assertEquals(3, storageDir.getStorageDirId());
    createBlockFile(storageDir, BlockInfo.computeBlockId(3, 0), 300);
    storageDir = allocator.getStorageDir(mStorageDirs, USER_ID, 1000);
    Assert.assertEquals(null, storageDir);
    boolean fitIn = allocator.fitInPossible(mStorageDirs, 1200);
    Assert.assertEquals(true, fitIn);
    mStorageDirs[2].lockBlock(BlockInfo.computeBlockId(3, 0), USER_ID);
    fitIn = allocator.fitInPossible(mStorageDirs, 1200);
    Assert.assertEquals(false, fitIn);
  }

  @Test
  public void AllocateRandomTest() throws IOException {
    AllocateStrategy allocator =
        AllocateStrategies.getAllocateStrategy(AllocateStrategyType.RANDOM);
    StorageDir storageDir = allocator.getStorageDir(mStorageDirs, USER_ID, 300);
    Assert.assertEquals(true, storageDir != null);
    createBlockFile(storageDir, BlockInfo.computeBlockId(1, 0), 300);
    storageDir = allocator.getStorageDir(mStorageDirs, USER_ID, 300);
    Assert.assertEquals(true, storageDir != null);
    createBlockFile(storageDir, BlockInfo.computeBlockId(2, 0), 300);
    storageDir = allocator.getStorageDir(mStorageDirs, USER_ID, 300);
    Assert.assertEquals(true, storageDir != null);
    createBlockFile(storageDir, BlockInfo.computeBlockId(3, 0), 300);
    storageDir = allocator.getStorageDir(mStorageDirs, USER_ID, 1300);
    Assert.assertEquals(null, storageDir);
    boolean fitIn = allocator.fitInPossible(mStorageDirs, 1200);
    Assert.assertEquals(true, fitIn);
    fitIn = allocator.fitInPossible(mStorageDirs, 1300);
    Assert.assertEquals(false, fitIn);
  }

  private void initializeStorageDir(StorageDir dir, long userId) throws IOException {
    dir.initialize();
    UnderFileSystem ufs = dir.getUfs();
    ufs.mkdirs(dir.getUserTempPath(userId), true);
    CommonUtils.changeLocalFileToFullPermission(dir.getUserTempPath(userId));
  }
}
*/
