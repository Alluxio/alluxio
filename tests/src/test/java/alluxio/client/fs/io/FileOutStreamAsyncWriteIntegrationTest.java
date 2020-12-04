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

package alluxio.client.fs.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.fs.io.AbstractFileOutStreamIntegrationTest;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.MasterClientContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.PersistenceState;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.worker.block.BlockWorker;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Integration tests for {@link alluxio.client.file.FileOutStream} of under storage type being async
 * persist.
 */
public final class FileOutStreamAsyncWriteIntegrationTest
    extends AbstractFileOutStreamIntegrationTest {

  private static final String TINY_WORKER_MEM = "512k";
  private static final String TINY_BLOCK_SIZE = "16k";

  @Test
  public void asyncWrite() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    FileOutStream os = mFileSystem.createFile(filePath,
        CreateFilePOptions.newBuilder().setWriteType(WritePType.ASYNC_THROUGH)
        .setRecursive(true).build());
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    CommonUtils.sleepMs(1);
    checkPersistStateAndWaitForPersist(filePath, 2);
  }

  @Test
  public void asyncWriteWithZeroWaitTime() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    createTwoBytesFile(filePath, 0);

    CommonUtils.sleepMs(1);
    checkPersistStateAndWaitForPersist(filePath, 2);
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.USER_FILE_PERSIST_ON_RENAME, "true"})
  public void asyncWriteRenameWithNoAutoPersist() throws Exception {
    AlluxioURI srcPath = new AlluxioURI(PathUtils.uniqPath());
    AlluxioURI dstPath = new AlluxioURI(PathUtils.uniqPath());
    createTwoBytesFile(srcPath, Constants.NO_AUTO_PERSIST);

    CommonUtils.sleepMs(1);
    // check the file is completed but not persisted
    URIStatus srcStatus = mFileSystem.getStatus(srcPath);
    assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), srcStatus.getPersistenceState());
    Assert.assertTrue(srcStatus.isCompleted());

    mFileSystem.rename(srcPath, dstPath);
    CommonUtils.sleepMs(1);
    checkPersistStateAndWaitForPersist(dstPath, 2);
  }

  @Test
  public void asyncWritePersistWithNoAutoPersist() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    createTwoBytesFile(filePath, Constants.NO_AUTO_PERSIST);

    CommonUtils.sleepMs(1);
    // check the file is completed but not persisted
    URIStatus srcStatus = mFileSystem.getStatus(filePath);
    assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), srcStatus.getPersistenceState());
    Assert.assertTrue(srcStatus.isCompleted());

    mFileSystem.persist(filePath);
    CommonUtils.sleepMs(1);
    checkPersistStateAndWaitForPersist(filePath, 2);
  }

  @Test
  public void asyncWriteWithPersistWaitTime() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    createTwoBytesFile(filePath, 2000);

    CommonUtils.sleepMs(1000);
    checkPersistStateAndWaitForPersist(filePath, 2);
  }

  @Test
  public void asyncWriteTemporaryPin() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    FileSystemTestUtils.createByteFile(mFileSystem, filePath, WritePType.ASYNC_THROUGH, 100);
    URIStatus status = mFileSystem.getStatus(filePath);
    alluxio.worker.file.FileSystemMasterClient fsMasterClient = new
        alluxio.worker.file.FileSystemMasterClient(MasterClientContext
            .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
    Assert.assertTrue(fsMasterClient.getPinList().contains(status.getFileId()));
    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, filePath);
    Assert.assertFalse(fsMasterClient.getPinList().contains(status.getFileId()));
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_FILE_PERSISTENCE_INITIAL_WAIT_TIME, "-1",
      PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, "ASYNC_THROUGH",
      PropertyKey.Name.WORKER_RAMDISK_SIZE, TINY_WORKER_MEM,
      PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, TINY_BLOCK_SIZE,
      PropertyKey.Name.USER_FILE_BUFFER_BYTES, TINY_BLOCK_SIZE,
      "alluxio.worker.tieredstore.level0.watermark.high.ratio", "0.5",
      "alluxio.worker.tieredstore.level0.watermark.low.ratio", "0.25",
      })
  public void asyncWriteNoEvictBeforeBlockCommit() throws Exception {
    long writeSize =
        FormatUtils.parseSpaceSize(TINY_WORKER_MEM) - FormatUtils.parseSpaceSize(TINY_BLOCK_SIZE);
    FileSystem fs = mLocalAlluxioClusterResource.get().getClient();
    AlluxioURI p1 = new AlluxioURI("/p1");
    FileOutStream fos = fs.createFile(p1, CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.ASYNC_THROUGH)
        .setPersistenceWaitTime(-1).build());
    byte[] arr = new byte[(int) writeSize];
    Arrays.fill(arr, (byte) 0x7a);
    fos.write(arr);
    assertEquals(writeSize + FormatUtils.parseSpaceSize(TINY_BLOCK_SIZE), getClusterCapacity());
    // This will succeed.
    FileSystemTestUtils.createByteFile(fs, "/byte-file1", WritePType.MUST_CACHE,
        (int) FormatUtils.parseSpaceSize(TINY_BLOCK_SIZE));
    // This will not until the stream is closed and persisted.
    try {
      FileSystemTestUtils.createByteFile(fs, "/byte-file2", WritePType.MUST_CACHE,
          2 * (int) FormatUtils.parseSpaceSize(TINY_BLOCK_SIZE));
      Assert.fail("Should have failed due to non-evictable block.");
    } catch (Exception e) {
      // expected.
    }
    fos.close();
    FileSystemUtils.persistAndWait(fs, p1, 0);
    // Now this should succeed.
    FileSystemTestUtils.createByteFile(fs, "/byte-file3", WritePType.MUST_CACHE,
        2 * (int) FormatUtils.parseSpaceSize(TINY_BLOCK_SIZE));
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, "ASYNC_THROUGH",
      PropertyKey.Name.USER_FILE_PERSISTENCE_INITIAL_WAIT_TIME, "1min",
      PropertyKey.Name.WORKER_RAMDISK_SIZE, TINY_WORKER_MEM,
      PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, TINY_BLOCK_SIZE,
      PropertyKey.Name.USER_FILE_BUFFER_BYTES, "8k"
      })
  public void asyncWriteNoEvict() throws Exception {
    testLostAsyncBlocks();
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.USER_FILE_PERSISTENCE_INITIAL_WAIT_TIME, "-1",
      PropertyKey.Name.USER_FILE_WRITE_TYPE_DEFAULT, "ASYNC_THROUGH",
      PropertyKey.Name.WORKER_RAMDISK_SIZE, TINY_WORKER_MEM,
      PropertyKey.Name.USER_BLOCK_SIZE_BYTES_DEFAULT, TINY_BLOCK_SIZE,
      PropertyKey.Name.USER_FILE_BUFFER_BYTES, "8k"
      })
  public void asyncPersistNoAutoPersistNoEvict() throws Exception {
    testLostAsyncBlocks();
  }

  /**
   * Test eviction against a file that is slow to persist in Alluxio. The test cluster should be
   * configured with a high initial wait time, or -1
   *
   * This test performs the following actions:
   * - creates a file with ASYNC_THROUGH which fills the entire capacity of a worker
   * - Tries to create another 1-byte file on top and expects it to fail
   * - persists the file to the UFS
   * - Tries to create 1-byte file again and it should succeed this time
   */
  private void testLostAsyncBlocks() throws Exception {
    long cap = FormatUtils.parseSpaceSize(TINY_WORKER_MEM);
    FileSystem fs = mLocalAlluxioClusterResource.get().getClient();
    // Create a large-ish file in the UFS (relative to memory size)
    String p1 = "/test";
    FileSystemTestUtils.createByteFile(fs, p1, WritePType.ASYNC_THROUGH, (int) cap);

    URIStatus fstat = fs.listStatus(new AlluxioURI(p1)).get(0);
    int lostBlocks = fstat.getFileBlockInfos().stream()
        .map(FileBlockInfo::getBlockInfo)
        .filter(blk -> blk.getLocations().size() <= 0)
        .mapToInt(blk -> 1)
        .sum();

    assertEquals(cap, getClusterCapacity());
    assertEquals(cap, getUsedWorkerSpace());
    assertEquals(100, fstat.getInAlluxioPercentage());
    assertEquals(0, lostBlocks);

    // Try to create 1-byte file on top. Expect to fail.
    try {
      FileSystemTestUtils.createByteFile(fs, "/byte-file1", WritePType.MUST_CACHE, 1);
      assertTrue("Shouldn't reach here.", false);
    } catch (Exception e) {
      // expected.
    }

    FileSystemUtils.persistAndWait(fs, new AlluxioURI(p1), 0);
    fstat = fs.listStatus(new AlluxioURI(p1)).get(0);

    assertTrue(fstat.isPersisted());
    assertEquals(0, mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class)
        .getPinIdList().size());

    // Try to create 1-byte file on top. Expect to succeed.
    FileSystemTestUtils.createByteFile(fs, "/byte-file2", WritePType.MUST_CACHE, 1);
  }

  @Test
  public void asyncWriteEmptyFile() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createFile(filePath, CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.ASYNC_THROUGH).setRecursive(true).build()).close();

    checkPersistStateAndWaitForPersist(filePath, 0);
  }

  private long getClusterCapacity() throws UnavailableException {
    // This value shouldn't ever change, so we don't need to trigger eviction or heartbeats
    return mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(BlockMaster.class)
        .getWorkerInfoList()
        .stream()
        .map(WorkerInfo::getCapacityBytes)
        .mapToLong(Long::new).sum();
  }

  /**
   * Executing this will trigger an eviction on the worker, force an update of cached storage
   * info, then retrieve the info from the block worker.
   *
   * @return the amount of space used by the worker
   */
  private long getUsedWorkerSpace() {
    BlockWorker blkWorker =
        mLocalAlluxioClusterResource.get().getWorkerProcess().getWorker(BlockWorker.class);
    return mLocalAlluxioClusterResource.get().getWorkerProcess()
        .getWorker(BlockWorker.class).getStoreMeta().getUsedBytes();
  }

  private void createTwoBytesFile(AlluxioURI path, long persistenceWaitTime) throws Exception {
    FileOutStream os = mFileSystem.createFile(path, CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.ASYNC_THROUGH).setPersistenceWaitTime(persistenceWaitTime)
        .setRecursive(true).build());
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();
  }

  private void checkPersistStateAndWaitForPersist(AlluxioURI path, int length) throws Exception {
    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(path);
    assertEquals(PersistenceState.TO_BE_PERSISTED.toString(),
        status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, path);

    status = mFileSystem.getStatus(path);
    assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    checkFileInAlluxio(path, length);
    checkFileInUnderStorage(path, length);
  }
}
