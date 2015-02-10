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
package tachyon.worker;

import java.io.File;
import java.io.IOException;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.UnderFileSystem;
import tachyon.client.TachyonFS;
import tachyon.client.WriteType;
import tachyon.conf.WorkerConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.master.MasterInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.OutOfSpaceException;
import tachyon.util.CommonUtils;

/**
 * Unit tests for tachyon.WorkerServiceHandler
 */
public class WorkerServiceHandlerTest {
  private static final long WORKER_CAPACITY_BYTES = 10000;
  private static final int USER_QUOTA_UNIT_BYTES = 100;
  private static final int SLEEP_MS = 
      WorkerConf.get().TO_MASTER_HEARTBEAT_INTERVAL_MS * 2 + 10;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private MasterInfo mMasterInfo = null;
  private WorkerServiceHandler mWorkerServiceHandler = null;
  private TachyonFS mTfs = null;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES);
    mLocalTachyonCluster.start();
    mWorkerServiceHandler = mLocalTachyonCluster.getWorker().getWorkerServiceHandler();
    mMasterInfo = mLocalTachyonCluster.getMasterInfo();
    mTfs = mLocalTachyonCluster.getClient();
  }

  @Test
  public void cancelBlockTest() throws TException, IOException {
    final long userId = 1L;
    final long blockId = 12345L;
    String filename = mWorkerServiceHandler.requestBlockLocation(userId, blockId,
        WORKER_CAPACITY_BYTES / 10L);
    Assert.assertTrue(filename != null);
    createBlockFile(filename, (int)(WORKER_CAPACITY_BYTES / 10L - 10L));
    mWorkerServiceHandler.cancelBlock(userId, blockId);
    Assert.assertFalse(new File(filename).exists());
    CommonUtils.sleepMs(null, SLEEP_MS);
    Assert.assertEquals(0, mMasterInfo.getUsedBytes());
  }

  @Test
  public void cacheBlockTest() throws TException, IOException {
    final long userId = 1L;
    final int fileId = mTfs.createFile(new TachyonURI("/testFile1"));
    final long blockId0 = mTfs.getBlockId(fileId, 0);
    final long blockId1 = mTfs.getBlockId(fileId, 1);
    String filename = mWorkerServiceHandler.requestBlockLocation(userId, blockId0,
        WORKER_CAPACITY_BYTES / 10L);
    Assert.assertTrue(filename != null);
    createBlockFile(filename, (int)(WORKER_CAPACITY_BYTES / 10L - 10L));
    mWorkerServiceHandler.cacheBlock(userId, blockId0);
    Assert.assertEquals(WORKER_CAPACITY_BYTES / 10L - 10, mMasterInfo.getUsedBytes());

    Exception exception = null;
    try {
      mWorkerServiceHandler.cacheBlock(userId, blockId1);
    } catch (FileDoesNotExistException e) {
      exception = e;
    }
    Assert.assertEquals(
        new FileDoesNotExistException("Block doesn't exist! blockId:" + blockId1), exception);
  }

  private void createBlockFile(String filename, int fileLen)
      throws IOException, InvalidPathException {
    UnderFileSystem.get(filename).mkdirs(CommonUtils.getParent(filename), true);
    BlockHandler handler = BlockHandler.get(filename);
    handler.append(0, TestUtils.getIncreasingByteArray(fileLen), 0, fileLen);
    handler.close();
  }

  @Test
  public void evictionTest() throws InvalidPathException, FileAlreadyExistException, IOException,
      FileDoesNotExistException, TException {
    int fileId1 =
        TestUtils.createByteFile(mTfs, "/file1", WriteType.MUST_CACHE,
            (int) WORKER_CAPACITY_BYTES / 3);
    Assert.assertTrue(fileId1 >= 0);
    ClientFileInfo fileInfo1 = mMasterInfo.getClientFileInfo(new TachyonURI("/file1"));
    Assert.assertEquals(100, fileInfo1.inMemoryPercentage);
    int fileId2 =
        TestUtils.createByteFile(mTfs, "/file2", WriteType.MUST_CACHE,
            (int) WORKER_CAPACITY_BYTES / 3);
    Assert.assertTrue(fileId2 >= 0);
    fileInfo1 = mMasterInfo.getClientFileInfo(new TachyonURI("/file1"));
    ClientFileInfo fileInfo2 = mMasterInfo.getClientFileInfo(new TachyonURI("/file2"));
    Assert.assertEquals(100, fileInfo1.inMemoryPercentage);
    Assert.assertEquals(100, fileInfo2.inMemoryPercentage);
    int fileId3 =
        TestUtils.createByteFile(mTfs, "/file3", WriteType.MUST_CACHE,
            (int) WORKER_CAPACITY_BYTES / 2);
    CommonUtils.sleepMs(null, SLEEP_MS);
    fileInfo1 = mMasterInfo.getClientFileInfo(new TachyonURI("/file1"));
    fileInfo2 = mMasterInfo.getClientFileInfo(new TachyonURI("/file2"));
    ClientFileInfo fileInfo3 = mMasterInfo.getClientFileInfo(new TachyonURI("/file3"));
    Assert.assertTrue(fileId3 >= 0);
    Assert.assertEquals(0, fileInfo1.inMemoryPercentage);
    Assert.assertEquals(100, fileInfo2.inMemoryPercentage);
    Assert.assertEquals(100, fileInfo3.inMemoryPercentage);
  }

  @Test
  public void requestSpaceTest() throws TException, IOException {
    final long userId = 1L;
    final long blockId1 = 12345L;
    final long blockId2 = 12346L;
    String filename = mWorkerServiceHandler.requestBlockLocation(userId, blockId1,
        WORKER_CAPACITY_BYTES / 10L);
    Assert.assertTrue(filename != null);
    boolean result =
        mWorkerServiceHandler.requestSpace(userId, blockId1, WORKER_CAPACITY_BYTES / 10L);
    Assert.assertEquals(true, result);
    result = mWorkerServiceHandler.requestSpace(userId, blockId1, WORKER_CAPACITY_BYTES);
    Assert.assertEquals(false, result);
    Exception exception = null;
    try {
      mWorkerServiceHandler.requestSpace(userId, blockId2, WORKER_CAPACITY_BYTES / 10L);
    } catch (FileDoesNotExistException e) {
      exception = e;
    }
    Assert.assertEquals(new FileDoesNotExistException(
        "Temporary block file doesn't exist! blockId:" + blockId2), exception);

    try {
      mWorkerServiceHandler.requestBlockLocation(userId, blockId2, WORKER_CAPACITY_BYTES + 1);
    } catch (OutOfSpaceException e) {
      exception = e;
    }
    Assert.assertEquals(new OutOfSpaceException(String.format("Failed to allocate space for block!"
        + " blockId(%d) sizeBytes(%d)", blockId2, WORKER_CAPACITY_BYTES + 1)), exception);
    
  }

  @Test
  public void totalOverCapacityRequestSpaceTest() throws TException {
    final long userId1 = 1L;
    final long blockId1 = 12345L;
    final long userId2 = 2L;
    final long blockId2 = 23456L;
    String filePath1 = mWorkerServiceHandler.requestBlockLocation(userId1, blockId1,
        WORKER_CAPACITY_BYTES / 2);
    Assert.assertTrue(filePath1 != null);
    String filePath2 = mWorkerServiceHandler.requestBlockLocation(userId2, blockId2,
        WORKER_CAPACITY_BYTES / 2);
    Assert.assertTrue(filePath2 != null);

    Assert.assertFalse(mWorkerServiceHandler.requestSpace(userId1, blockId1,
        WORKER_CAPACITY_BYTES / 2));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(userId2, blockId2,
        WORKER_CAPACITY_BYTES / 2));
  }
}
