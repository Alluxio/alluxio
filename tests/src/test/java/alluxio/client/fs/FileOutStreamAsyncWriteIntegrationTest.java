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

package alluxio.client.fs;

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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioJobCluster;
import alluxio.master.MasterClientContext;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.PersistenceState;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileBlockInfo;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.SpaceReserver;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.powermock.reflect.Whitebox;

/**
 * Integration tests for {@link alluxio.client.file.FileOutStream} of under storage type being async
 * persist.
 */
public final class FileOutStreamAsyncWriteIntegrationTest extends BaseIntegrationTest {
  private static final String TINY_WORKER_MEM = "512k";
  private static final String TINY_BLOCK_SIZE = "16k";

  protected static LocalAlluxioJobCluster sLocalAlluxioJobCluster;

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      buildLocalAlluxioClusterResource();

  public static LocalAlluxioClusterResource buildLocalAlluxioClusterResource() {
    LocalAlluxioClusterResource.Builder resource = new LocalAlluxioClusterResource.Builder()
        .setProperty(PropertyKey.USER_FILE_BUFFER_BYTES, "8k")
        .setProperty(PropertyKey.USER_FILE_REPLICATION_DURABLE, 1)
        .setProperty(PropertyKey.MASTER_PERSISTENCE_SCHEDULER_INTERVAL_MS, "100ms")
        .setProperty(PropertyKey.MASTER_PERSISTENCE_CHECKER_INTERVAL_MS, "100ms")
        .setProperty(PropertyKey.WORKER_TIERED_STORE_RESERVER_INTERVAL_MS, "100ms")
        .setProperty(PropertyKey.WORKER_MEMORY_SIZE, TINY_WORKER_MEM)
        .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, TINY_BLOCK_SIZE);
    return resource.build();
  }

  @Rule
  public TestRule mResetRule = sLocalAlluxioClusterResource.getResetResource();

  private FileSystem mFileSystem;

  @BeforeClass
  public static void beforeClass() throws Exception {
    sLocalAlluxioJobCluster = new LocalAlluxioJobCluster();
    sLocalAlluxioJobCluster.setProperty(PropertyKey.JOB_MASTER_WORKER_HEARTBEAT_INTERVAL, "25ms");
    sLocalAlluxioJobCluster.start();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (sLocalAlluxioJobCluster != null) {
      sLocalAlluxioJobCluster.stop();
    }
  }

  @Before
  public void before() throws Exception {
    mFileSystem = sLocalAlluxioClusterResource.get().getClient();
  }

  @After
  public void after() throws Exception {
    mFileSystem.close();
  }

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
    IntegrationTestUtils
        .checkPersistStateAndWaitForPersist(sLocalAlluxioClusterResource, mFileSystem, filePath, 2);
  }

  @Test
  public void asyncWriteWithZeroWaitTime() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    createTwoBytesFile(mFileSystem, filePath, 0);

    CommonUtils.sleepMs(1);
    IntegrationTestUtils
        .checkPersistStateAndWaitForPersist(sLocalAlluxioClusterResource, mFileSystem, filePath, 2);
  }

  @Test
  public void asyncWriteRenameWithNoAutoPersist() throws Exception {
    ServerConfiguration.set(PropertyKey.USER_FILE_PERSIST_ON_RENAME, "true");
    try (FileSystem fs = sLocalAlluxioClusterResource.get().getClient()) {
      AlluxioURI srcPath = new AlluxioURI(PathUtils.uniqPath());
      AlluxioURI dstPath = new AlluxioURI(PathUtils.uniqPath());
      createTwoBytesFile(fs, srcPath, Constants.NO_AUTO_PERSIST);

      CommonUtils.sleepMs(1);
      // check the file is completed but not persisted
      URIStatus srcStatus = fs.getStatus(srcPath);
      assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), srcStatus.getPersistenceState());
      Assert.assertTrue(srcStatus.isCompleted());

      fs.rename(srcPath, dstPath);
      CommonUtils.sleepMs(1);
      IntegrationTestUtils
          .checkPersistStateAndWaitForPersist(sLocalAlluxioClusterResource, fs, dstPath, 2);
    }
  }

  @Test
  public void asyncWritePersistWithNoAutoPersist() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    createTwoBytesFile(mFileSystem, filePath, Constants.NO_AUTO_PERSIST);

    CommonUtils.sleepMs(1);
    // check the file is completed but not persisted
    URIStatus srcStatus = mFileSystem.getStatus(filePath);
    assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), srcStatus.getPersistenceState());
    Assert.assertTrue(srcStatus.isCompleted());

    mFileSystem.persist(filePath);
    CommonUtils.sleepMs(1);
    IntegrationTestUtils
        .checkPersistStateAndWaitForPersist(sLocalAlluxioClusterResource, mFileSystem, filePath, 2);
  }

  @Test
  public void asyncWriteWithPersistWaitTime() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    createTwoBytesFile(mFileSystem, filePath, 2000);

    IntegrationTestUtils
        .checkPersistStateAndWaitForPersist(sLocalAlluxioClusterResource, mFileSystem, filePath, 2);
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
    IntegrationTestUtils.waitForPersist(sLocalAlluxioClusterResource, filePath);
    Assert.assertFalse(fsMasterClient.getPinList().contains(status.getFileId()));
  }

  @Test
  public void asyncWriteNoEvict() throws Exception {
    ServerConfiguration.set(PropertyKey.USER_FILE_PERSISTENCE_INITIAL_WAIT_TIME, "1min");
    ServerConfiguration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WritePType.ASYNC_THROUGH);
    testLostAsyncBlocks();
  }

  @Test
  public void asyncPersistNoAutoPersistNoEvict() throws Exception {
    ServerConfiguration.set(PropertyKey.USER_FILE_PERSISTENCE_INITIAL_WAIT_TIME, "-1");
    ServerConfiguration.set(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WritePType.ASYNC_THROUGH);
    testLostAsyncBlocks();
  }

  /**
   * Test eviction against a file that is slow to persist in Alluxio. The test cluster should be
   * configured with a high initial wait time, or -1
   *
   * This test performs the following actions:
   * - creates a file with ASYNC_THROUGH which fills the entire capacity of a worker
   * - Manually triggers an eviction, making sure that no blocks have been evicted
   * - persists the file to the UFS
   * - triggers an eviction, checking that some of the blocks have been evicted from storage
   */
  private void testLostAsyncBlocks() throws Exception {
    long cap = FormatUtils.parseSpaceSize(TINY_WORKER_MEM);
    try (FileSystem fs = sLocalAlluxioClusterResource.get().getClient()) {
      // Create a large-ish file in the UFS (relative to memory size)
      String p1 = "/test";
      FileSystemTestUtils.createByteFile(fs, p1, WritePType.ASYNC_THROUGH, (int) cap);

      BlockWorker blkWorker = sLocalAlluxioClusterResource.get().getWorkerProcess()
          .getWorker(BlockWorker.class);
      SpaceReserver reserver = Whitebox.getInternalState(blkWorker, SpaceReserver.class);

      // Trigger worker eviction
      reserver.heartbeat();
      URIStatus fstat = fs.listStatus(new AlluxioURI(p1)).get(0);
      int lostBlocks = fstat.getFileBlockInfos().stream().map(FileBlockInfo::getBlockInfo)
          .filter(blk -> blk.getLocations().size() <= 0).mapToInt(blk -> 1).sum();

      assertEquals(cap,
          IntegrationTestUtils.getClusterCapacity(sLocalAlluxioClusterResource.get()));
      assertEquals(cap, IntegrationTestUtils.getUsedWorkerSpace(sLocalAlluxioClusterResource));
      assertEquals(100, fstat.getInAlluxioPercentage());
      assertEquals(0, lostBlocks);

      FileSystemUtils.persistAndWait(fs, new AlluxioURI(p1), 0);
      fstat = fs.listStatus(new AlluxioURI(p1)).get(0);

      assertTrue(fstat.isPersisted());
      assertEquals(0, sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
          .getMaster(FileSystemMaster.class).getPinIdList().size());
      assertTrue(IntegrationTestUtils
          .getUsedWorkerSpace(sLocalAlluxioClusterResource) < IntegrationTestUtils
          .getClusterCapacity(sLocalAlluxioClusterResource.get()));
    }
  }

  @Test
  public void asyncWriteEmptyFile() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createFile(filePath, CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.ASYNC_THROUGH).setRecursive(true).build()).close();
    IntegrationTestUtils
        .checkPersistStateAndWaitForPersist(sLocalAlluxioClusterResource, mFileSystem, filePath, 0);
  }

  private void createTwoBytesFile(FileSystem fs, AlluxioURI path, long persistenceWaitTime)
      throws Exception {
    FileOutStream os = fs.createFile(path, CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.ASYNC_THROUGH).setPersistenceWaitTime(persistenceWaitTime)
        .setRecursive(true).build());
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();
  }
}
