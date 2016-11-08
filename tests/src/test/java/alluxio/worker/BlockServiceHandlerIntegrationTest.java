/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.client.FileSystemTestUtils;
import alluxio.client.WriteType;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.RetryHandlingBlockMasterClient;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.InvalidPathException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.block.BlockId;
import alluxio.thrift.AlluxioTException;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;
import alluxio.worker.block.BlockWorkerClientServiceHandler;

import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * Integration tests for {@link BlockWorkerClientServiceHandler}.
 */
public class BlockServiceHandlerIntegrationTest {
  private static final long WORKER_CAPACITY_BYTES = 10 * Constants.MB;
  private static final long SESSION_ID = 1L;

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.WORKER_BLOCK_SYNC);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.WORKER_MEMORY_SIZE, WORKER_CAPACITY_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, Constants.MB)
          .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, String.valueOf(100))
          .build();
  private BlockWorkerClientServiceHandler mBlockWorkerServiceHandler = null;
  private FileSystem mFileSystem = null;
  private BlockMasterClient mBlockMasterClient;

  @Before
  public final void before() throws Exception {
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();
    mBlockWorkerServiceHandler =
        mLocalAlluxioClusterResource.get().getWorker().getBlockWorker().getWorkerServiceHandler();

    mBlockMasterClient = new RetryHandlingBlockMasterClient(
        new InetSocketAddress(mLocalAlluxioClusterResource.get().getHostname(),
            mLocalAlluxioClusterResource.get().getMasterRpcPort()));
  }

  @After
  public final void after() throws Exception {
    mBlockMasterClient.close();
  }

  // Tests that caching a block successfully persists the block if the block exists
  @Test
  public void cacheBlock() throws Exception {
    mFileSystem.createFile(new AlluxioURI("/testFile")).close();
    URIStatus file = mFileSystem.getStatus(new AlluxioURI("/testFile"));

    final int blockSize = (int) WORKER_CAPACITY_BYTES / 10;
    // Construct the block ids for the file.
    final long blockId0 = BlockId.createBlockId(BlockId.getContainerId(file.getFileId()), 0);
    final long blockId1 = BlockId.createBlockId(BlockId.getContainerId(file.getFileId()), 1);

    String filename =
        mBlockWorkerServiceHandler.requestBlockLocation(SESSION_ID, blockId0, blockSize);
    createBlockFile(filename, blockSize);
    mBlockWorkerServiceHandler.cacheBlock(SESSION_ID, blockId0);

    // The master should be immediately updated with the persisted block
    Assert.assertEquals(blockSize, mBlockMasterClient.getUsedBytes());

    // Attempting to cache a non existent block should throw an exception
    Exception exception = null;
    try {
      mBlockWorkerServiceHandler.cacheBlock(SESSION_ID, blockId1);
    } catch (TException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);
  }

  // Tests that cancelling a block will remove the temporary file
  @Test
  public void cancelBlock() throws Exception {
    mFileSystem.createFile(new AlluxioURI("/testFile")).close();
    URIStatus file = mFileSystem.getStatus(new AlluxioURI("/testFile"));

    final int blockSize = (int) WORKER_CAPACITY_BYTES / 2;
    final long blockId = BlockId.createBlockId(BlockId.getContainerId(file.getFileId()), 0);

    String filename =
        mBlockWorkerServiceHandler.requestBlockLocation(SESSION_ID, blockId, blockSize);
    createBlockFile(filename, blockSize);
    mBlockWorkerServiceHandler.cancelBlock(SESSION_ID, blockId);

    // The block should not exist after being cancelled
    Assert.assertFalse(new File(filename).exists());

    // The master should not have recorded any used space after the block is cancelled
    HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);
    Assert.assertEquals(0, mBlockMasterClient.getUsedBytes());
  }

  // Tests that lock block returns the correct path
  @Test
  public void lockBlock() throws Exception {
    final int blockSize = (int) WORKER_CAPACITY_BYTES / 2;

    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(blockSize)
            .setWriteType(WriteType.MUST_CACHE);
    FileOutStream out = mFileSystem.createFile(new AlluxioURI("/testFile"), options);
    URIStatus file = mFileSystem.getStatus(new AlluxioURI("/testFile"));

    final long blockId = BlockId.createBlockId(BlockId.getContainerId(file.getFileId()), 0);

    out.write(BufferUtils.getIncreasingByteArray(blockSize));
    out.close();

    String localPath = mBlockWorkerServiceHandler.lockBlock(blockId, SESSION_ID).getBlockPath();

    // The local path should exist
    Assert.assertNotNull(localPath);

    UnderFileSystem ufs = UnderFileSystem.get(localPath);
    byte[] data = new byte[blockSize];
    int bytesRead = ufs.open(localPath).read(data);

    // The data in the local file should equal the data we wrote earlier
    Assert.assertEquals(blockSize, bytesRead);
    Assert.assertTrue(BufferUtils.equalIncreasingByteArray(bytesRead, data));

    mBlockWorkerServiceHandler.unlockBlock(blockId, SESSION_ID);
  }

  // Tests that lock block returns error on failure
  @Test
  public void lockBlockFailure() throws Exception {
    mFileSystem.createFile(new AlluxioURI("/testFile")).close();
    URIStatus file = mFileSystem.getStatus(new AlluxioURI("/testFile"));
    final long blockId = BlockId.createBlockId(BlockId.getContainerId(file.getFileId()), 0);

    Exception exception = null;
    try {
      mBlockWorkerServiceHandler.lockBlock(blockId, SESSION_ID);
    } catch (AlluxioTException e) {
      exception = e;
    }

    // A file does not exist exception should have been thrown
    Assert.assertNotNull(exception);
  }

  // Tests that files are evicted when there is not enough space in the worker.
  @Test
  public void eviction() throws Exception {
    final int blockSize = (int) WORKER_CAPACITY_BYTES / 2;
    AlluxioURI file1 = new AlluxioURI("/file1");
    FileSystemTestUtils.createByteFile(mFileSystem, file1, WriteType.MUST_CACHE, blockSize);

    // File should be in memory after it is written with MUST_CACHE
    URIStatus fileInfo1 = mFileSystem.getStatus(file1);
    Assert.assertEquals(100, fileInfo1.getInMemoryPercentage());

    AlluxioURI file2 = new AlluxioURI("/file2");
    FileSystemTestUtils.createByteFile(mFileSystem, file2, WriteType.MUST_CACHE, blockSize);

    // Both file 1 and 2 should be in memory since the combined size is not larger than worker space
    fileInfo1 = mFileSystem.getStatus(file1);
    URIStatus fileInfo2 = mFileSystem.getStatus(file2);
    Assert.assertEquals(100, fileInfo1.getInMemoryPercentage());
    Assert.assertEquals(100, fileInfo2.getInMemoryPercentage());

    AlluxioURI file3 = new AlluxioURI("/file3");
    FileSystemTestUtils.createByteFile(mFileSystem, file3, WriteType.MUST_CACHE, blockSize);

    HeartbeatScheduler.execute(HeartbeatContext.WORKER_BLOCK_SYNC);

    fileInfo1 = mFileSystem.getStatus(file1);
    fileInfo2 = mFileSystem.getStatus(file2);
    URIStatus fileInfo3 = mFileSystem.getStatus(file3);

    // File 3 should be in memory and one of file 1 or 2 should be in memory
    Assert.assertEquals(100, fileInfo3.getInMemoryPercentage());
    Assert.assertTrue("Exactly one of file1 and file2 should be 100% in memory",
        fileInfo1.getInMemoryPercentage() == 100 ^ fileInfo2.getInMemoryPercentage() == 100);
  }

  // Tests that space will be allocated when possible
  @Test
  public void requestSpace() throws Exception {
    final long blockId1 = 12345L;
    final long blockId2 = 12346L;
    final int chunkSize = (int) WORKER_CAPACITY_BYTES / 10;

    mBlockWorkerServiceHandler.requestBlockLocation(SESSION_ID, blockId1, chunkSize);
    boolean result = mBlockWorkerServiceHandler.requestSpace(SESSION_ID, blockId1, chunkSize);

    // Initial request and first additional request should succeed
    Assert.assertTrue(result);

    result = mBlockWorkerServiceHandler.requestSpace(SESSION_ID, blockId1, WORKER_CAPACITY_BYTES);

    // Impossible request should fail
    Assert.assertFalse(result);

    // Request for space on a nonexistent block should fail
    try {
      mBlockWorkerServiceHandler.requestSpace(SESSION_ID, blockId2, chunkSize);
      Assert.fail();
    } catch (AlluxioTException e) {
      Assert.assertEquals(ExceptionMessage.TEMP_BLOCK_META_NOT_FOUND.getMessage(blockId2),
          e.getMessage());
    }

    // Request for impossible initial space should fail
    Exception exception = null;
    try {
      mBlockWorkerServiceHandler.requestBlockLocation(SESSION_ID, blockId2,
          WORKER_CAPACITY_BYTES + 1);
    } catch (AlluxioTException e) {
      exception = e;
    }
    Assert.assertNotNull(exception);
  }

  // Tests that multiple users cannot request a combined space greater than worker space
  @Test
  public void totalOverCapacityRequestSpace() throws Exception {
    final int chunkSize = (int) WORKER_CAPACITY_BYTES / 2;
    final long userId1 = SESSION_ID;
    final long userId2 = SESSION_ID + 1;
    final long blockId1 = 12345L;
    final long blockId2 = 23456L;

    String filePath1 =
        mBlockWorkerServiceHandler.requestBlockLocation(userId1, blockId1, chunkSize);
    String filePath2 =
        mBlockWorkerServiceHandler.requestBlockLocation(userId2, blockId2, chunkSize);

    // Initial requests should succeed
    Assert.assertTrue(filePath1 != null);
    Assert.assertTrue(filePath2 != null);

    // Additional requests for space should fail
    Assert.assertFalse(mBlockWorkerServiceHandler.requestSpace(userId1, blockId1, chunkSize));
    Assert.assertFalse(mBlockWorkerServiceHandler.requestSpace(userId2, blockId2, chunkSize));
  }

  // Creates a block file and write an increasing byte array into it
  private void createBlockFile(String filename, int len) throws IOException, InvalidPathException {
    UnderFileSystem ufs = UnderFileSystem.get(filename);
    ufs.mkdirs(PathUtils.getParent(filename), true);
    OutputStream out = ufs.create(filename);
    out.write(BufferUtils.getIncreasingByteArray(len), 0, len);
    out.close();
  }
}
