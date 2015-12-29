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
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.block.BlockMasterClient;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.InvalidPathException;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.heartbeat.HeartbeatScheduler;
import tachyon.master.block.BlockId;
import tachyon.thrift.FileInfo;
import tachyon.thrift.TachyonTException;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.BufferUtils;
import tachyon.util.io.PathUtils;
import tachyon.worker.block.BlockWorkerClientServiceHandler;

/**
 * Integration tests for {@link BlockWorkerClientServiceHandler}
 */
public class BlockServiceHandlerIntegrationTest {
  private static final long WORKER_CAPACITY_BYTES = 10000;
  private static final long SESSION_ID = 1L;
  private static final int USER_QUOTA_UNIT_BYTES = 100;

  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(WORKER_CAPACITY_BYTES, USER_QUOTA_UNIT_BYTES, Constants.GB,
          Constants.USER_FILE_BUFFER_BYTES, String.valueOf(100));
  private BlockWorkerClientServiceHandler mWorkerServiceHandler = null;
  private TachyonFileSystem mTfs = null;
  private TachyonConf mMasterTachyonConf;
  private TachyonConf mWorkerTachyonConf;
  private BlockMasterClient mBlockMasterClient;

  @BeforeClass
  public static void beforeClass() {
    HeartbeatContext.setTimerClass(HeartbeatContext.WORKER_BLOCK_SYNC,
        HeartbeatContext.SCHEDULED_TIMER_CLASS);
  }

  @AfterClass
  public static void afterClass() {
    HeartbeatContext.setTimerClass(HeartbeatContext.WORKER_BLOCK_SYNC,
        HeartbeatContext.SLEEPING_TIMER_CLASS);
  }

  @Before
  public final void before() throws Exception {
    mTfs = mLocalTachyonClusterResource.get().getClient();
    mMasterTachyonConf = mLocalTachyonClusterResource.get().getMasterTachyonConf();
    mWorkerTachyonConf = mLocalTachyonClusterResource.get().getWorkerTachyonConf();
    mWorkerServiceHandler =
        mLocalTachyonClusterResource.get().getWorker().getWorkerServiceHandler();

    mBlockMasterClient = new BlockMasterClient(
        new InetSocketAddress(mLocalTachyonClusterResource.get().getMasterHostname(),
            mLocalTachyonClusterResource.get().getMasterPort()),
        mWorkerTachyonConf);
  }

  @After
  public final void after() throws Exception {
    mBlockMasterClient.close();
  }

  // Tests that caching a block successfully persists the block if the block exists
  @Test
  public void cacheBlockTest() throws Exception {
    mTfs.getOutStream(new TachyonURI("/testFile"));
    TachyonFile file = mTfs.open(new TachyonURI("/testFile"));

    final int blockSize = (int) WORKER_CAPACITY_BYTES / 10;
    // Construct the block ids for the file.
    final long blockId0 = BlockId.createBlockId(BlockId.getContainerId(file.getFileId()), 0);
    final long blockId1 = BlockId.createBlockId(BlockId.getContainerId(file.getFileId()), 1);

    String filename = mWorkerServiceHandler.requestBlockLocation(SESSION_ID, blockId0, blockSize);
    createBlockFile(filename, blockSize);
    mWorkerServiceHandler.cacheBlock(SESSION_ID, blockId0);

    // The master should be immediately updated with the persisted block
    Assert.assertEquals(blockSize, mBlockMasterClient.getUsedBytes());

    // Attempting to cache a non existent block should throw an exception
    Exception exception = null;
    try {
      mWorkerServiceHandler.cacheBlock(SESSION_ID, blockId1);
    } catch (TException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);
  }

  // Tests that cancelling a block will remove the temporary file
  @Test
  public void cancelBlockTest() throws Exception {
    mTfs.getOutStream(new TachyonURI("/testFile"));
    TachyonFile file = mTfs.open(new TachyonURI("/testFile"));

    final int blockSize = (int) WORKER_CAPACITY_BYTES / 2;
    final long blockId = BlockId.createBlockId(BlockId.getContainerId(file.getFileId()), 0);

    String filename = mWorkerServiceHandler.requestBlockLocation(SESSION_ID, blockId, blockSize);
    createBlockFile(filename, blockSize);
    mWorkerServiceHandler.cancelBlock(SESSION_ID, blockId);

    // The block should not exist after being cancelled
    Assert.assertFalse(new File(filename).exists());

    // The master should not have recorded any used space after the block is cancelled
    waitForHeartbeat();
    Assert.assertEquals(0, mBlockMasterClient.getUsedBytes());
  }

  // Tests that lock block returns the correct path
  @Test
  public void lockBlockTest() throws Exception {
    final int blockSize = (int) WORKER_CAPACITY_BYTES / 2;

    OutStreamOptions options =
        new OutStreamOptions.Builder(new TachyonConf()).setBlockSizeBytes(blockSize)
            .setTachyonStorageType(TachyonStorageType.STORE).build();
    FileOutStream out = mTfs.getOutStream(new TachyonURI("/testFile"), options);
    TachyonFile file = mTfs.open(new TachyonURI("/testFile"));

    final long blockId = BlockId.createBlockId(BlockId.getContainerId(file.getFileId()), 0);

    out.write(BufferUtils.getIncreasingByteArray(blockSize));
    out.close();

    String localPath = mWorkerServiceHandler.lockBlock(blockId, SESSION_ID).blockPath;

    // The local path should exist
    Assert.assertNotNull(localPath);

    UnderFileSystem ufs = UnderFileSystem.get(localPath, mMasterTachyonConf);
    byte[] data = new byte[blockSize];
    int bytesRead = ufs.open(localPath).read(data);

    // The data in the local file should equal the data we wrote earlier
    Assert.assertEquals(blockSize, bytesRead);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(bytesRead, data));

    mWorkerServiceHandler.unlockBlock(blockId, SESSION_ID);
  }

  // Tests that lock block returns error on failure
  @Test
  public void lockBlockFailureTest() throws Exception {
    mTfs.getOutStream(new TachyonURI("/testFile"));
    TachyonFile file = mTfs.open(new TachyonURI("/testFile"));
    final long blockId = BlockId.createBlockId(BlockId.getContainerId(file.getFileId()), 0);

    Exception exception = null;
    try {
      mWorkerServiceHandler.lockBlock(blockId, SESSION_ID);
    } catch (TachyonTException e) {
      exception = e;
    }

    // A file does not exist exception should have been thrown
    Assert.assertNotNull(exception);
  }

  // Tests that files are evicted when there is not enough space in the worker.
  @Test
  public void evictionTest() throws Exception {
    final int blockSize = (int) WORKER_CAPACITY_BYTES / 2;
    TachyonFile file1 = TachyonFSTestUtils.createByteFile(mTfs, "/file1", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, blockSize);

    // File should be in memory after it is written with MUST_CACHE
    FileInfo fileInfo1 = mTfs.getInfo(file1);
    Assert.assertEquals(100, fileInfo1.inMemoryPercentage);

    TachyonFile file2 = TachyonFSTestUtils.createByteFile(mTfs, "/file2", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, blockSize);

    // Both file 1 and 2 should be in memory since the combined size is not larger than worker space
    fileInfo1 = mTfs.getInfo(file1);
    FileInfo fileInfo2 = mTfs.getInfo(file2);
    Assert.assertEquals(100, fileInfo1.inMemoryPercentage);
    Assert.assertEquals(100, fileInfo2.inMemoryPercentage);

    TachyonFile file3 = TachyonFSTestUtils.createByteFile(mTfs, "/file3", TachyonStorageType.STORE,
        UnderStorageType.NO_PERSIST, blockSize);

    waitForHeartbeat();

    fileInfo1 = mTfs.getInfo(file1);
    fileInfo2 = mTfs.getInfo(file2);
    FileInfo fileInfo3 = mTfs.getInfo(file3);

    // File 3 should be in memory and one of file 1 or 2 should be in memory
    Assert.assertEquals(100, fileInfo3.inMemoryPercentage);
    Assert.assertTrue("Exactly one of file1 and file2 should be 100% in memory",
        fileInfo1.inMemoryPercentage == 100 ^ fileInfo2.inMemoryPercentage == 100);
  }

  // Tests that space will be allocated when possible
  @Test
  public void requestSpaceTest() throws Exception {
    final long blockId1 = 12345L;
    final long blockId2 = 12346L;
    final int chunkSize = (int) WORKER_CAPACITY_BYTES / 10;

    mWorkerServiceHandler.requestBlockLocation(SESSION_ID, blockId1, chunkSize);
    boolean result = mWorkerServiceHandler.requestSpace(SESSION_ID, blockId1, chunkSize);

    // Initial request and first additional request should succeed
    Assert.assertTrue(result);

    result = mWorkerServiceHandler.requestSpace(SESSION_ID, blockId1, WORKER_CAPACITY_BYTES);

    // Impossible request should fail
    Assert.assertFalse(result);

    // Request for space on a nonexistent block should fail
    Assert.assertFalse(mWorkerServiceHandler.requestSpace(SESSION_ID, blockId2, chunkSize));

    // Request for impossible initial space should fail
    Exception exception = null;
    try {
      mWorkerServiceHandler.requestBlockLocation(SESSION_ID, blockId2, WORKER_CAPACITY_BYTES + 1);
    } catch (TachyonTException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);
  }

  // Tests that multiple users cannot request a combined space greater than worker space
  @Test
  public void totalOverCapacityRequestSpaceTest() throws Exception {
    final int chunkSize = (int) WORKER_CAPACITY_BYTES / 2;
    final long userId1 = SESSION_ID;
    final long userId2 = SESSION_ID + 1;
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
    ufs.mkdirs(PathUtils.getParent(filename), true);
    OutputStream out = ufs.create(filename);
    out.write(BufferUtils.getIncreasingByteArray(len), 0, len);
    out.close();
  }

  // Waits for a worker heartbeat to master to be processed
  private void waitForHeartbeat() throws InterruptedException {
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 5,
        TimeUnit.SECONDS));
    HeartbeatScheduler.schedule(HeartbeatContext.WORKER_BLOCK_SYNC);
    // Wait for the next heartbeat to be ready to guarantee that the previous heartbeat has finished
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.WORKER_BLOCK_SYNC, 5,
        TimeUnit.SECONDS));
  }
}
