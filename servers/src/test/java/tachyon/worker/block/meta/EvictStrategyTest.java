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

// TODO: Reenable this
/*
package tachyon.worker.block.meta;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Pair;
import tachyon.TestUtils;
import tachyon.conf.TachyonConf;
import tachyon.master.BlockInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;
import tachyon.worker.block.io.BlockHandler;
import tachyon.worker.WorkerSource;
import tachyon.worker.block.meta.StorageDir;
import tachyon.worker.block.evictor.EvictLRU;
import tachyon.worker.block.evictor.EvictPartialLRU;
import tachyon.worker.block.evictor.EvictStrategy;

public class EvictStrategyTest {
  private final StorageDir[] mStorageDirs = new StorageDir[3];
  private static final long USER_ID = 1;
  private static final long CAPACITY = 1000;

  @Before
  public final void before() throws IOException {
    String tachyonHome =
        File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.currentTimeMillis();
    String workerDirFolder = tachyonHome + "/ramdisk";
    String[] dirPaths = "/dir1,/dir2,/dir3".split(",");
    for (int i = 0; i < 3; i ++) {
      mStorageDirs[i] =
          new StorageDir(i + 1, workerDirFolder + dirPaths[i], CAPACITY, "/data", "/user", null,
              new TachyonConf(), new WorkerSource(null));
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
  public void EvictLRUTest() throws IOException {
    createBlockFile(mStorageDirs[0], BlockInfo.computeBlockId(1, 0), 300);
    createBlockFile(mStorageDirs[1], BlockInfo.computeBlockId(2, 0), 300);
    createBlockFile(mStorageDirs[2], BlockInfo.computeBlockId(3, 0), 350);
    createBlockFile(mStorageDirs[0], BlockInfo.computeBlockId(4, 0), 550);
    createBlockFile(mStorageDirs[1], BlockInfo.computeBlockId(5, 0), 600);
    createBlockFile(mStorageDirs[2], BlockInfo.computeBlockId(6, 0), 500);

    Set<Integer> pinList = new HashSet<Integer>();
    EvictStrategy eviction = new EvictLRU(true);

    pinList.add(1);
    Pair<StorageDir, List<tachyon.worker.block.meta.BlockInfo>> lruResult =
        eviction.getDirCandidate(mStorageDirs, pinList, 300);
    Assert.assertEquals(mStorageDirs[1], lruResult.getFirst());
    Assert.assertEquals(1, lruResult.getSecond().size());
    Assert.assertEquals(lruResult.getSecond().get(0).getBlockId(), BlockInfo.computeBlockId(2, 0));

    pinList.add(2);
    lruResult = eviction.getDirCandidate(mStorageDirs, pinList, 300);
    Assert.assertEquals(mStorageDirs[2], lruResult.getFirst());
    Assert.assertEquals(1, lruResult.getSecond().size());
    Assert.assertEquals(lruResult.getSecond().get(0).getBlockId(), BlockInfo.computeBlockId(3, 0));

    eviction = new EvictLRU(false);
    lruResult = eviction.getDirCandidate(mStorageDirs, pinList, 300);
    Assert.assertEquals(mStorageDirs[0], lruResult.getFirst());
    Assert.assertEquals(1, lruResult.getSecond().size());
    Assert.assertEquals(lruResult.getSecond().get(0).getBlockId(), BlockInfo.computeBlockId(1, 0));

    lruResult = eviction.getDirCandidate(mStorageDirs, pinList, 1001);
    Assert.assertEquals(null, lruResult);
  }

  @Test
  public void EvictPartialLRUTest() throws IOException {
    createBlockFile(mStorageDirs[0], BlockInfo.computeBlockId(1, 0), 450);
    createBlockFile(mStorageDirs[1], BlockInfo.computeBlockId(2, 0), 600);
    createBlockFile(mStorageDirs[2], BlockInfo.computeBlockId(3, 0), 500);
    createBlockFile(mStorageDirs[0], BlockInfo.computeBlockId(4, 0), 400);
    createBlockFile(mStorageDirs[1], BlockInfo.computeBlockId(5, 0), 300);
    createBlockFile(mStorageDirs[2], BlockInfo.computeBlockId(6, 0), 450);

    Set<Integer> pinList = new HashSet<Integer>();
    EvictStrategy eviction = new EvictPartialLRU(true);

    pinList.add(1);
    Pair<StorageDir, List<tachyon.worker.block.meta.BlockInfo>> lruResult =
        eviction.getDirCandidate(mStorageDirs, pinList, 600);
    Assert.assertEquals(mStorageDirs[1], lruResult.getFirst());
    Assert.assertEquals(1, lruResult.getSecond().size());
    Assert.assertEquals(lruResult.getSecond().get(0).getBlockId(), BlockInfo.computeBlockId(2, 0));

    pinList.add(2);
    lruResult = eviction.getDirCandidate(mStorageDirs, pinList, 600);
    Assert.assertEquals(mStorageDirs[2], lruResult.getFirst());
    Assert.assertEquals(2, lruResult.getSecond().size());
    Assert.assertEquals(lruResult.getSecond().get(0).getBlockId(), BlockInfo.computeBlockId(3, 0));
    Assert.assertEquals(lruResult.getSecond().get(1).getBlockId(), BlockInfo.computeBlockId(6, 0));

    eviction = new EvictLRU(false);
    lruResult = eviction.getDirCandidate(mStorageDirs, pinList, 400);
    Assert.assertEquals(mStorageDirs[0], lruResult.getFirst());
    Assert.assertEquals(1, lruResult.getSecond().size());
    Assert.assertEquals(lruResult.getSecond().get(0).getBlockId(), BlockInfo.computeBlockId(1, 0));

    lruResult = eviction.getDirCandidate(mStorageDirs, pinList, 1001);
    Assert.assertEquals(null, lruResult);
  }

  private void initializeStorageDir(StorageDir dir, long userId) throws IOException {
    dir.initialize();
    UnderFileSystem ufs = dir.getUfs();
    ufs.mkdirs(dir.getUserTempPath(userId), true);
    CommonUtils.changeLocalFileToFullPermission(dir.getUserTempPath(userId));
  }
}
*/
