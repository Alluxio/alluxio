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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import alluxio.AlluxioURI;
import alluxio.annotation.dora.DoraTestTodoItem;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DecommissionWorkerPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.GrpcUtils;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.ReadPType;
import alluxio.grpc.WritePType;
import alluxio.security.user.TestUserState;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.SleepUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerInfo;
import alluxio.wire.WorkerNetAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Ignore
@DoraTestTodoItem(action = DoraTestTodoItem.Action.FIX, owner = "jiacheng",
    comment = "check if decommission is still a relevant feature")
public class FileInStreamDecommissionIntegrationTest {
  private static final int BLOCK_SIZE = 1024 * 1024;
  private static final int LENGTH = 2 * BLOCK_SIZE;
  private static final int CLIENT_WORKER_LIST_REFRESH_INTERVAL = 2000; // 2s

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setNumWorkers(2)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE)
          .setProperty(PropertyKey.USER_WORKER_LIST_REFRESH_INTERVAL, "2s")
          .setStartCluster(false)
          .build();
  private FileSystem mFileSystem = null;
  private CreateFilePOptions mWriteBoth;
  private CreateFilePOptions mWriteAlluxio;
  private OpenFilePOptions mReadNoCache;
  private OpenFilePOptions mReadCachePromote;
  private String mTestPath;
  private ExecutorService mThreadPool;

  private String mCacheThroughFilePath;
  private String mMustCacheFilePath;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public final void setUp() throws Exception {
    mLocalAlluxioClusterResource.start();
    mFileSystem = mLocalAlluxioClusterResource.get().getClient();

    // Just use the 1st worker to write everything
    WorkerNetAddress worker1 = mLocalAlluxioClusterResource.get().getWorkerAddress();
    // For each file, 2 blocks on the same worker so we can use the 1st block's location
    // to know which worker to decommission
    mWriteBoth = CreateFilePOptions.newBuilder()
            .setBlockSizeBytes(BLOCK_SIZE)
            .setWriteType(WritePType.CACHE_THROUGH)
            .setWorkerLocation(GrpcUtils.toProto(worker1))
            .setRecursive(true).build();
    mWriteAlluxio = CreateFilePOptions.newBuilder()
            .setBlockSizeBytes(BLOCK_SIZE)
            .setWriteType(WritePType.MUST_CACHE)
            .setWorkerLocation(GrpcUtils.toProto(worker1))
            .setRecursive(true).build();
    mReadCachePromote =
            OpenFilePOptions.newBuilder().setReadType(ReadPType.CACHE_PROMOTE).build();
    mReadNoCache = OpenFilePOptions.newBuilder().setReadType(ReadPType.NO_CACHE).build();
    mTestPath = PathUtils.uniqPath();
    mCacheThroughFilePath = mTestPath + "/file_BOTH";
    mMustCacheFilePath = mTestPath + "/file_CACHE";

    // Create files of varying size and write type to later read from
    AlluxioURI path0 = new AlluxioURI(mCacheThroughFilePath);
    FileSystemTestUtils.createByteFile(mFileSystem, path0, mWriteBoth, LENGTH);

    AlluxioURI path1 = new AlluxioURI(mMustCacheFilePath);
    FileSystemTestUtils.createByteFile(mFileSystem, path1, mWriteAlluxio, LENGTH);

    mThreadPool = Executors.newFixedThreadPool(1,
        ThreadFactoryUtils.build("decommission-worker-%d", true));
  }

  @After
  public final void tearDown() throws Exception {
    mLocalAlluxioClusterResource.stop();
    mThreadPool.shutdownNow();
  }

  private List<CreateFilePOptions> getOptionSet() {
    List<CreateFilePOptions> ret = new ArrayList<>(2);
    ret.add(mWriteBoth);
    ret.add(mWriteAlluxio);
    return ret;
  }

  @Test
  /*
   * If a stream is created after the worker is decommissioned, it cannot pick that worker.
   * And if the block exists in UFS, the client will use the other worker and read from UFS.
   */
  public void readUfsFromUndecommissionedWorker() throws Exception {
    AlluxioURI uri = new AlluxioURI(mCacheThroughFilePath);

    FileSystemContext context = FileSystemContext
            .create(new TestUserState("test", Configuration.global()).getSubject(),
                    Configuration.global());
    List<WorkerInfo> availableWorkers = context.acquireBlockMasterClientResource()
        .get().getWorkerInfoList();
    assertEquals(2, availableWorkers.size());

    URIStatus status = context.acquireMasterClientResource().get()
        .getStatus(uri, GetStatusPOptions.getDefaultInstance());
    List<FileBlockInfo> blockInfos = status.getFileBlockInfos();
    FileBlockInfo block0 = blockInfos.get(0);
    BlockLocation loc0 = block0.getBlockInfo().getLocations().get(0);
    WorkerNetAddress workerToDecommission = loc0.getWorkerAddress();

    DecommissionWorkerPOptions decomOptions = DecommissionWorkerPOptions.newBuilder()
            .setWorkerHostname(workerToDecommission.getHost())
            .setWorkerWebPort(workerToDecommission.getWebPort())
            .setCanRegisterAgain(true).build();
    context.acquireBlockMasterClientResource().get().decommissionWorker(decomOptions);

    // This stream is able to find the undecommissioned worker and use that to read from UFS
    FileInStream is = mFileSystem.openFile(uri, mReadCachePromote);
    byte[] ret = new byte[1024 * 1024]; // 1MB buffer
    // This has created the block stream that reads from the target worker
    int readLength = 0;
    int value = 0;

    while (value != -1) {
      value = is.read(ret);
      if (value != -1) {
        readLength += value;
      }
    }
    assertEquals(readLength, LENGTH);
    is.close();

    // The blocks are read from the other worker, so there should be cache on the way
    URIStatus statusAfterRead = context.acquireMasterClientResource().get()
        .getStatus(uri, GetStatusPOptions.getDefaultInstance());
    assertEquals(2, statusAfterRead.getFileBlockInfos().size());
    List<BlockLocation> block0Locs = statusAfterRead.getFileBlockInfos().get(0)
            .getBlockInfo().getLocations();
    assertEquals(1, block0Locs.size());
    // The block is not on the decommissioned worker, meaning it is cached on the other worker
    assertNotEquals(workerToDecommission, block0Locs.get(0).getWorkerAddress());

    List<BlockLocation> block1Locs = statusAfterRead.getFileBlockInfos().get(1)
            .getBlockInfo().getLocations();
    assertEquals(1, block1Locs.size());
    // The block is not on the decommissioned worker, meaning it is cached on the other worker
    assertNotEquals(workerToDecommission, block1Locs.get(0).getWorkerAddress());
  }

  @Test
  /*
   * If a stream is created after the worker is decommissioned, it cannot pick that worker.
   * And if that worker holds the only cache and the block is not in UFS,
   * the read will fail.
   */
  public void cannotReadCacheFromDecommissionedWorker() throws Exception {
    AlluxioURI uri = new AlluxioURI(mMustCacheFilePath);
    FileSystemContext context = FileSystemContext
            .create(new TestUserState("test", Configuration.global()).getSubject(),
                    Configuration.global());
    List<WorkerInfo> availableWorkers = context.acquireBlockMasterClientResource()
            .get().getWorkerInfoList();
    assertEquals(2, availableWorkers.size());

    URIStatus status = context.acquireMasterClientResource().get()
            .getStatus(uri, GetStatusPOptions.getDefaultInstance());
    List<FileBlockInfo> blockInfos = status.getFileBlockInfos();
    FileBlockInfo block0 = blockInfos.get(0);
    BlockLocation loc0 = block0.getBlockInfo().getLocations().get(0);
    WorkerNetAddress targetWorker = loc0.getWorkerAddress();

    DecommissionWorkerPOptions decomOptions = DecommissionWorkerPOptions.newBuilder()
            .setWorkerHostname(targetWorker.getHost()).setWorkerWebPort(targetWorker.getWebPort())
            .setCanRegisterAgain(true).build();
    context.acquireBlockMasterClientResource().get().decommissionWorker(decomOptions);

    // This stream is able to find the undecommissioned worker and use that to read from UFS
    FileInStream is = mFileSystem.openFile(uri, mReadCachePromote);
    // The worker has been decommissioned and the file only exists in that worker
    // So the client cannot read
    assertThrows(UnavailableException.class, () -> {
      int value = is.read();
    });
    is.close();
  }

  @Test
  /*
   * The target worker is decommissioned while the stream is reading.
   * However, the stream does not know the worker list has changed and keeps reading that worker.
   * This read should succeed.
   */
  public void decommissionWhileReading() throws Exception {
    AlluxioURI uri = new AlluxioURI(mCacheThroughFilePath);
    FileSystemContext context = FileSystemContext
            .create(new TestUserState("test", Configuration.global()).getSubject(),
                    Configuration.global());
    List<WorkerInfo> availableWorkers = context.acquireBlockMasterClientResource()
            .get().getWorkerInfoList();
    assertEquals(2, availableWorkers.size());

    URIStatus status = context.acquireMasterClientResource().get()
            .getStatus(uri, GetStatusPOptions.getDefaultInstance());
    List<FileBlockInfo> blockInfos = status.getFileBlockInfos();
    FileBlockInfo block0 = blockInfos.get(0);
    BlockLocation loc0 = block0.getBlockInfo().getLocations().get(0);
    WorkerNetAddress targetWorker = loc0.getWorkerAddress();

    CountDownLatch streamActive = new CountDownLatch(1);
    CountDownLatch workerDecommissioned = new CountDownLatch(1);
    mThreadPool.submit(() -> {
      try {
        streamActive.await();
        DecommissionWorkerPOptions decomOptions = DecommissionWorkerPOptions.newBuilder()
            .setWorkerHostname(targetWorker.getHost()).setWorkerWebPort(targetWorker.getWebPort())
            .setCanRegisterAgain(true).build();
        context.acquireBlockMasterClientResource().get().decommissionWorker(decomOptions);

        List<WorkerInfo> updatedWorkers = context.acquireBlockMasterClientResource()
            .get().getWorkerInfoList();
        assertEquals(1, updatedWorkers.size());

        workerDecommissioned.countDown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    // This stream is able to find the undecommissioned worker and use that to read from UFS
    FileInStream is = mFileSystem.openFile(uri, mReadNoCache);
    byte[] ret = new byte[1024]; // 1MB buffer
    // This has created the block stream that reads from the target worker
    int value = 0;
    int readLength = 0;

    boolean released = false;
    while (value != -1) {
      if (readLength > 1024 && !released) {
        streamActive.countDown();
        released = true;

        // Wait a bit for the decommission to take effect
        // After the worker is decommissioned, the stream can successfully complete
        workerDecommissioned.await();
        // However, even though the master has refreshed the available worker list
        // The stream does not pick another worker until it sees an exception
        // So when this resumes, the stream will keep reading the decommissioned worker
        // And we want the decommissioned worker to keep serving
      }
      value = is.read(ret);
      if (value != -1) {
        readLength += value;
      }
    }
    assertEquals(readLength, LENGTH);
    is.close();
  }

  @Test
  /*
   * When there is an active stream reading from one worker, decommission that worker.
   * Then we make the stream wait a bit and realize that worker is no longer available.
   * The stream should pick the other available worker in the cluster and read from UFS using that.
   */
  public void halfStreamFromAnotherWorker() throws Exception {
    AlluxioURI uri = new AlluxioURI(mCacheThroughFilePath);

    FileSystemContext context = FileSystemContext
            .create(new TestUserState("test", Configuration.global()).getSubject(),
                    Configuration.global());
    List<WorkerInfo> clusterWorkers = context.acquireBlockMasterClientResource()
            .get().getWorkerInfoList();
    assertEquals(2, clusterWorkers.size());

    URIStatus status = context.acquireMasterClientResource().get()
            .getStatus(uri, GetStatusPOptions.getDefaultInstance());
    List<FileBlockInfo> blockInfos = status.getFileBlockInfos();
    FileBlockInfo block0 = blockInfos.get(0);
    BlockLocation loc0 = block0.getBlockInfo().getLocations().get(0);
    WorkerNetAddress workerToDecommission = loc0.getWorkerAddress();

    CountDownLatch streamActive = new CountDownLatch(1);
    CountDownLatch workerDecommissioned = new CountDownLatch(1);
    mThreadPool.submit(() -> {
      try {
        streamActive.await();
        DecommissionWorkerPOptions decomOptions = DecommissionWorkerPOptions.newBuilder()
                .setWorkerHostname(workerToDecommission.getHost())
                .setWorkerWebPort(workerToDecommission.getWebPort())
                .setCanRegisterAgain(true).build();
        context.acquireBlockMasterClientResource().get().decommissionWorker(decomOptions);

        List<WorkerInfo> updatedWorkers = context.acquireBlockMasterClientResource()
                .get().getWorkerInfoList();
        assertEquals(1, updatedWorkers.size());
        workerDecommissioned.countDown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    });

    // This stream is able to find the undecommissioned worker and use that to read from UFS
    FileInStream is = mFileSystem.openFile(uri, mReadCachePromote);
    byte[] ret = new byte[1024]; // 1MB buffer
    // This has created the block stream that reads from the target worker
    int value = 0;
    int readLength = 0;

    boolean released = false;
    while (value != -1) {
      // 2 blocks on the same worker, decommission at the end of one BlockStream
      // so when the FileStream continues, create the 2nd block stream where there's only one
      // usable worker that does not have the block
      if (readLength == BLOCK_SIZE && !released) {
        streamActive.countDown();
        released = true;

        // Wait a bit for the decommission to take effect
        // After the worker is decommissioned, the stream can successfully complete
        workerDecommissioned.await();

        // Wait a bit for the worker list to refresh in the FileSystemContext
        SleepUtils.sleepMs(CLIENT_WORKER_LIST_REFRESH_INTERVAL);
        // The client realizes the target worker is decommissioned
        List<BlockWorkerInfo> usableWorkers = context.getCachedWorkers();
        assertEquals(1, usableWorkers.size());
        // Continue where the usable worker is not the stream target
        // The client should be able to find the other worker and read UFS through that
      }
      value = is.read(ret);
      if (value != -1) {
        readLength += value;
      }
    }
    assertEquals(readLength, LENGTH);

    // The 2nd block is read from the other worker, so there should be cached on the way
    URIStatus statusAfterRead = context.acquireMasterClientResource()
        .get().getStatus(uri, GetStatusPOptions.getDefaultInstance());
    FileBlockInfo block1 = statusAfterRead.getFileBlockInfos().get(1);
    WorkerNetAddress cachedToWorker = block1.getBlockInfo().getLocations()
            .get(0).getWorkerAddress();
    assertNotEquals(cachedToWorker, workerToDecommission);

    is.close();
  }
}
