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
import java.io.OutputStream;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.TestUtils;
import tachyon.client.OutStream;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.WriteType;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.master.MasterInfo;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.OutOfSpaceException;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.CommonUtils;
import tachyon.worker.block.BlockServiceHandler;
import tachyon.worker.block.io.BlockWriter;

/**
 * Integration tests for tachyon.BlockServiceHandler
 */
public class BlockServiceHandlerIntegrationTest {
  private static final long WORKER_CAPACITY_BYTES = 10000;
  private static final long USER_ID = 1L;
  private static final int USER_QUOTA_UNIT_BYTES = 100;

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private MasterInfo mMasterInfo = null;
  private BlockServiceHandler mWorkerServiceHandler = null;
  private TachyonFS mTfs = null;
  private TachyonConf mMasterTachyonConf;
  private TachyonConf mWorkerTachyonConf;

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("fs.hdfs.impl.disable.cache");
  }

  @Before
  public final void before() throws IOException {
    // Disable hdfs client caching to avoid file system close() affecting other clients
    System.setProperty("fs.hdfs.impl.disable.cache", "true");

    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES,
        Constants.GB);
    mLocalTachyonCluster.start();
    mWorkerServiceHandler = mLocalTachyonCluster.getWorker().getWorkerServiceHandler();
    mMasterInfo = mLocalTachyonCluster.getMasterInfo();
    mTfs = mLocalTachyonCluster.getClient();
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
    mWorkerTachyonConf = mLocalTachyonCluster.getWorkerTachyonConf();
  }

  // Tests that checkpointing a file successfully informs master of the update
  @Test
  public void addCheckpointTest() throws Exception {
    final int fileId = mTfs.createFile(new TachyonURI("/testFile"));
    final int blockSize = (int) WORKER_CAPACITY_BYTES / 10;

    String tmpFolder = mWorkerServiceHandler.getUserUfsTempFolder(USER_ID);
    UnderFileSystem ufs = UnderFileSystem.get(tmpFolder, mMasterTachyonConf);
    ufs.mkdirs(tmpFolder, true);
    String filename = CommonUtils.concatPath(tmpFolder, fileId);
    OutputStream out = ufs.create(filename);
    out.write(TestUtils.getIncreasingByteArray(blockSize));
    out.close();
    mWorkerServiceHandler.addCheckpoint(USER_ID, fileId);

    // No space should be used in Tachyon, but the file should be complete
    Assert.assertEquals(0, mMasterInfo.getUsedBytes());
    Assert.assertTrue(mTfs.getFile(fileId).isComplete());
  }

  // Tests that caching a block successfully persists the block if the block exists
  @Test
  public void cacheBlockTest() throws Exception {
    final int fileId = mTfs.createFile(new TachyonURI("/testFile"));
    final int blockSize = (int) WORKER_CAPACITY_BYTES / 10;
    final long blockId0 = mTfs.getBlockId(fileId, 0);
    final long blockId1 = mTfs.getBlockId(fileId, 1);

    String filename = mWorkerServiceHandler.requestBlockLocation(USER_ID, blockId0, blockSize);
    createBlockFile(filename, blockSize);
    mWorkerServiceHandler.cacheBlock(USER_ID, blockId0);

    // The master should be immediately updated with the persisted block
    Assert.assertEquals(blockSize, mMasterInfo.getUsedBytes());

    // Attempting to cache a non existent block should throw an exception
    Exception exception = null;
    try {
      mWorkerServiceHandler.cacheBlock(USER_ID, blockId1);
    } catch (TException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);
  }

  // Tests that cancelling a block will remove the temporary file
  @Test
  public void cancelBlockTest() throws Exception {
    final int fileId = mTfs.createFile(new TachyonURI("/testFile"));
    final int blockSize = (int) WORKER_CAPACITY_BYTES / 2;
    final long blockId = mTfs.getBlockId(fileId, 0);

    String filename = mWorkerServiceHandler.requestBlockLocation(USER_ID, blockId, blockSize);
    createBlockFile(filename, blockSize);
    mWorkerServiceHandler.cancelBlock(USER_ID, blockId);

    // The block should not exist after being cancelled
    Assert.assertFalse(new File(filename).exists());

    // The master should not have recorded any used space after the block is cancelled
    waitForHeartbeat();
    Assert.assertEquals(0, mMasterInfo.getUsedBytes());
  }

  // Tests that lock block returns the correct path
  @Test
  public void lockBlockTest() throws Exception {
    final int fileId = mTfs.createFile(new TachyonURI("/testFile"));
    final int blockSize = (int) WORKER_CAPACITY_BYTES / 2;
    final long blockId = mTfs.getBlockId(fileId, 0);

    OutStream out = mTfs.getFile(fileId).getOutStream(WriteType.MUST_CACHE);
    out.write(TestUtils.getIncreasingByteArray(blockSize));
    out.close();

    String localPath = mWorkerServiceHandler.lockBlock(blockId, USER_ID);

    // The local path should exist
    Assert.assertNotNull(localPath);

    UnderFileSystem ufs = UnderFileSystem.get(localPath, mMasterTachyonConf);
    byte[] data = new byte[blockSize];
    int bytesRead = ufs.open(localPath).read(data);

    // The data in the local file should equal the data we wrote earlier
    Assert.assertEquals(blockSize, bytesRead);
    Assert.assertTrue(TestUtils.equalIncreasingByteArray(bytesRead, data));

    mWorkerServiceHandler.unlockBlock(blockId, USER_ID);
  }

  // Tests that lock block returns error on failure
  @Test
  public void lockBlockFailureTest() throws Exception {
    final int fileId = mTfs.createFile(new TachyonURI("/testFile"));
    Exception exception = null;
    try {
      mWorkerServiceHandler.lockBlock(mTfs.getBlockId(fileId, 0), USER_ID);
    } catch (FileDoesNotExistException fdne) {
      exception = fdne;
    }

    // A file does not exist exception should have been thrown
    Assert.assertNotNull(exception);
  }

  // Tests that files are evicted when there is not enough space in the worker.
  @Test
  public void evictionTest() throws Exception {
    final int blockSize = (int) WORKER_CAPACITY_BYTES / 2;
    int fId1 = TachyonFSTestUtils.createByteFile(mTfs, "/file1", WriteType.MUST_CACHE, blockSize);

    // File should be in memory after it is written with MUST_CACHE
    ClientFileInfo fileInfo1 = mMasterInfo.getClientFileInfo(fId1);
    Assert.assertEquals(100, fileInfo1.inMemoryPercentage);

    int fId2 = TachyonFSTestUtils.createByteFile(mTfs, "/file2", WriteType.MUST_CACHE, blockSize);

    // Both file 1 and 2 should be in memory since the combined size is not larger than worker space
    fileInfo1 = mMasterInfo.getClientFileInfo(fId1);
    ClientFileInfo fileInfo2 = mMasterInfo.getClientFileInfo(fId2);
    Assert.assertEquals(100, fileInfo1.inMemoryPercentage);
    Assert.assertEquals(100, fileInfo2.inMemoryPercentage);

    int fId3 = TachyonFSTestUtils.createByteFile(mTfs, "/file3", WriteType.MUST_CACHE, blockSize);

    waitForHeartbeat();

    fileInfo1 = mMasterInfo.getClientFileInfo(fId1);
    fileInfo2 = mMasterInfo.getClientFileInfo(fId2);
    ClientFileInfo fileInfo3 = mMasterInfo.getClientFileInfo(fId3);

    // File 3 should be in memory and one of file 1 or 2 should be in memory
    Assert.assertEquals(100, fileInfo3.inMemoryPercentage);
    Assert.assertTrue(fileInfo1.inMemoryPercentage == 100 ^ fileInfo2.inMemoryPercentage == 100);
  }

  // Tests that space will be allocated when possible
  @Test
  public void requestSpaceTest() throws Exception {
    final long blockId1 = 12345L;
    final long blockId2 = 12346L;
    final int chunkSize = (int) WORKER_CAPACITY_BYTES / 10;

    mWorkerServiceHandler.requestBlockLocation(USER_ID, blockId1, chunkSize);
    boolean result = mWorkerServiceHandler.requestSpace(USER_ID, blockId1, chunkSize);

    // Initial request and first additional request should succeed
    Assert.assertEquals(true, result);

    result = mWorkerServiceHandler.requestSpace(USER_ID, blockId1, WORKER_CAPACITY_BYTES);

    // Impossible request should fail
    Assert.assertEquals(false, result);

    // Request for space on a nonexistent block should fail
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(USER_ID, blockId2, chunkSize));

    // Request for impossible initial space should fail
    Exception exception = null;
    try {
      mWorkerServiceHandler.requestBlockLocation(USER_ID, blockId2, WORKER_CAPACITY_BYTES + 1);
    } catch (OutOfSpaceException oose) {
      exception = oose;
    }
    Assert.assertNotNull(exception);
  }

  // Tests that multiple users cannot request a combined space greater than worker space
  @Test
  public void totalOverCapacityRequestSpaceTest() throws Exception {
    final int chunkSize = (int) WORKER_CAPACITY_BYTES / 2;
    final long userId1 = USER_ID;
    final long userId2 = USER_ID + 1;
    final long blockId1 = 12345L;
    final long blockId2 = 23456L;

    String filePath1 = mWorkerServiceHandler.requestBlockLocation(userId1, blockId1, chunkSize);
    String filePath2 = mWorkerServiceHandler.requestBlockLocation(userId2, blockId2, chunkSize);

    // Initial requests should succeed
    Assert.assertTrue(filePath1 != null);
    Assert.assertTrue(filePath2 != null);

    // Additional requests for space should fail
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(userId1, blockId1, chunkSize));
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(userId2, blockId2, chunkSize));
  }

  // Creates a block file and write an increasing byte array into it
  private void createBlockFile(String filename, int len) throws IOException, InvalidPathException {
    UnderFileSystem ufs = UnderFileSystem.get(filename, mMasterTachyonConf);
    ufs.mkdirs(CommonUtils.getParent(filename), true);
    OutputStream out = ufs.create(filename);
    out.write(TestUtils.getIncreasingByteArray(len), 0, len);
    out.close();
  }

  // Sleeps for a duration so that the worker heartbeat to master can be processed
  private void waitForHeartbeat() {
    CommonUtils.sleepMs(null, TestUtils.getToMasterHeartBeatIntervalMs(mWorkerTachyonConf) * 3);
  }
}
