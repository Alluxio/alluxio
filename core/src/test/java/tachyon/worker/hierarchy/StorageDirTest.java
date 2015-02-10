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
package tachyon.worker.hierarchy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.TestUtils;
import tachyon.UnderFileSystem;
import tachyon.thrift.InvalidPathException;
import tachyon.util.CommonUtils;
import tachyon.worker.BlockHandler;

public class StorageDirTest {
  private StorageDir mSrcDir;
  private StorageDir mDstDir;
  private static final long USER_ID = 1;
  private static final long CAPACITY = 1000;

  @Before
  public final void before() throws IOException, InvalidPathException {
    String tachyonHome =
        File.createTempFile("Tachyon", "").getAbsoluteFile() + "U" + System.currentTimeMillis();
    String workerDirFolder = tachyonHome + "/ramdisk";
    mSrcDir = new StorageDir(1, workerDirFolder + "/src", CAPACITY, "/data", "/user", null);
    mDstDir = new StorageDir(2, workerDirFolder + "/dst", CAPACITY, "/data", "/user", null);

    initializeStorageDir(mSrcDir, USER_ID);
    initializeStorageDir(mDstDir, USER_ID);
  }

  @Test
  public void cacheBlockCancelTest() throws  IOException {
    long blockId = 100;
    int blockSize = 500;
    Exception exception = null;

    mSrcDir.requestSpace(USER_ID, blockSize);
    mSrcDir.updateTempBlockAllocatedBytes(USER_ID, blockId, blockSize);
    try {
      // cacheBlock calls cancelBlock and throws IOException
      mSrcDir.cacheBlock(USER_ID, blockId);
    } catch (IOException e) {
      exception = e;
    }
    Assert.assertEquals(
        "Block file doesn't exist! blockId:100 " + mSrcDir.getUserTempFilePath(USER_ID, blockId),
        exception.getMessage());
    Assert.assertEquals(CAPACITY, mSrcDir.getAvailableBytes());
    Assert.assertEquals(0, mSrcDir.getUserOwnBytes(USER_ID));
  }

  @Test
  public void copyBlockTest() throws IOException {
    long blockId = 100;
    int blockSize = 500;

    createBlockFile(mSrcDir, blockId, blockSize);
    Assert.assertTrue(mSrcDir.containsBlock(blockId));
    Assert.assertEquals(CAPACITY - blockSize, mSrcDir.getAvailableBytes());
    Assert.assertEquals(blockSize, mSrcDir.getBlockSize(blockId));
    boolean requestDst = mDstDir.requestSpace(USER_ID, blockSize);
    Assert.assertTrue(requestDst);
    mSrcDir.copyBlock(blockId, mDstDir);
    Assert.assertTrue(mDstDir.containsBlock(blockId));
    Assert.assertEquals(CAPACITY - blockSize, mDstDir.getAvailableBytes());
    Assert.assertEquals(blockSize, mDstDir.getBlockSize(blockId));
    mSrcDir.deleteBlock(blockId);
    Assert.assertFalse(mSrcDir.containsBlock(blockId));
    Assert.assertEquals(CAPACITY, mSrcDir.getAvailableBytes());
  }

  private void createBlockFile(StorageDir dir, long blockId, int blockSize) throws IOException {
    byte[] buf = TestUtils.getIncreasingByteArray(blockSize);
    BlockHandler bhSrc =
        BlockHandler.get(CommonUtils.concat(dir.getUserTempFilePath(USER_ID, blockId)));
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
  public void deleteLockedBlockTest() throws IOException{
    long blockId = 100;
    int blockSize = 500;

    createBlockFile(mSrcDir, blockId, blockSize);
    mSrcDir.lockBlock(blockId, USER_ID);
    mSrcDir.deleteBlock(blockId);
    Assert.assertFalse(mSrcDir.containsBlock(blockId));
    Assert.assertEquals(CAPACITY - blockSize, mSrcDir.getAvailableBytes());
    Assert.assertEquals(blockSize, mSrcDir.getLockedSizeBytes());
    mSrcDir.unlockBlock(blockId, USER_ID);
    Assert.assertEquals(CAPACITY, mSrcDir.getAvailableBytes());
    Assert.assertEquals(0, mSrcDir.getLockedSizeBytes());
  }

  @Test
  public void getBlockDataTest() throws IOException {
    long blockId = 100;
    int blockSize = 500;

    createBlockFile(mSrcDir, blockId, blockSize);
    byte[] buf = TestUtils.getIncreasingByteArray(blockSize);
    ByteBuffer dataBuf = mSrcDir.getBlockData(blockId, blockSize / 2, blockSize / 2);
    Assert.assertEquals(ByteBuffer.wrap(buf, blockSize / 2, blockSize / 2), dataBuf);
    dataBuf = mSrcDir.getBlockData(blockId, 0, -1);
    Assert.assertEquals(ByteBuffer.wrap(buf), dataBuf);
  }

  private void initializeStorageDir(StorageDir dir, long userId) throws IOException {
    dir.initailize();
    UnderFileSystem ufs = dir.getUfs();
    ufs.mkdirs(dir.getUserTempPath(userId), true);
    CommonUtils.changeLocalFileToFullPermission(dir.getUserTempPath(userId));
  }

  @Test
  public void lockBlockTest() throws IOException {
    long blockId = 100;

    createBlockFile(mSrcDir, blockId, 500);
    mSrcDir.lockBlock(blockId, USER_ID);
    Assert.assertTrue(mSrcDir.isBlockLocked(blockId));
    mSrcDir.unlockBlock(blockId, USER_ID);
    Assert.assertFalse(mSrcDir.isBlockLocked(blockId));
  }

  @Test
  public void moveBlockTest() throws IOException {
    long blockId = 100;
    int blockSize = 500;

    createBlockFile(mSrcDir, blockId, blockSize);
    mDstDir.requestSpace(USER_ID, blockSize);
    mSrcDir.moveBlock(blockId, mDstDir);
    Assert.assertFalse(mSrcDir.containsBlock(blockId));
    Assert.assertTrue(mDstDir.containsBlock(blockId));
    Assert.assertEquals(blockSize, mDstDir.getBlockSize(blockId));
  }

  @Test
  public void requestSpaceTest() {
    boolean requestSrc = mSrcDir.requestSpace(USER_ID, CAPACITY / 2);
    Assert.assertTrue(requestSrc);
    requestSrc = mSrcDir.requestSpace(USER_ID, CAPACITY / 2 + 1);
    Assert.assertFalse(requestSrc);
    Assert.assertEquals(CAPACITY / 2, mSrcDir.getUsedBytes());
    Assert.assertEquals(CAPACITY / 2, mSrcDir.getUserOwnBytes(USER_ID));
    Assert.assertEquals(CAPACITY / 2, mSrcDir.getAvailableBytes());
    mSrcDir.returnSpace(USER_ID, CAPACITY / 2);
    Assert.assertEquals(CAPACITY, mSrcDir.getAvailableBytes());
    Assert.assertEquals(0, mSrcDir.getUserOwnBytes(USER_ID));
  }
}
