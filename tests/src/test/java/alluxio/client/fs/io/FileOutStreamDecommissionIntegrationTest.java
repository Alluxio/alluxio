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

import static org.apache.ratis.util.Preconditions.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import alluxio.AlluxioURI;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.ExceptionMessage;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileOutStreamDecommissionIntegrationTest {
  private static final int BLOCK_SIZE = 1024 * 1024;
  private static final int LENGTH = 2 * BLOCK_SIZE;
  private static final int CLIENT_WORKER_LIST_REFRESH_INTERVAL = 2000; // 2s

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setNumWorkers(2)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, BLOCK_SIZE)
          .setProperty(PropertyKey.USER_WORKER_LIST_REFRESH_INTERVAL, "2s")
          .setProperty(PropertyKey.USER_FILE_WRITE_INIT_MAX_DURATION, "2s")
          // Disable short circuit
          .setProperty(PropertyKey.USER_SHORT_CIRCUIT_ENABLED, false)
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
   * And the client will use the other worker to write to UFS.
   */
  public void writeUfsFromUndecommissionedWorker() throws Exception {
    AlluxioURI uri = new AlluxioURI(mCacheThroughFilePath);

    FileSystemContext context = FileSystemContext
            .create(new TestUserState("test", Configuration.global()).getSubject(),
                    Configuration.global());
    List<WorkerInfo> availableWorkers = context.acquireBlockMasterClientResource()
            .get().getWorkerInfoList();
    assertEquals(2, availableWorkers.size());

    // Decommission one worker in the cluster
    WorkerNetAddress workerToDecommission = mLocalAlluxioClusterResource.get().getWorkerAddress();
    DecommissionWorkerPOptions decomOptions = DecommissionWorkerPOptions.newBuilder()
            .setWorkerHostname(workerToDecommission.getHost())
            .setWorkerWebPort(workerToDecommission.getWebPort())
            .setCanRegisterAgain(true).build();
    context.acquireBlockMasterClientResource().get().decommissionWorker(decomOptions);

    // Create a stream w/o specifying target
    CreateFilePOptions writeOptions = CreateFilePOptions.newBuilder()
            .setBlockSizeBytes(BLOCK_SIZE)
            .setWriteType(WritePType.CACHE_THROUGH)
            .setRecursive(true).build();
    // This stream is able to find the undecommissioned worker and use that to write to UFS
    FileOutStream os = mFileSystem.createFile(uri, writeOptions);
    byte[] ret = new byte[1024 * 1024]; // 1MB buffer
    // This has created the block stream that reads from the target worker
    int writeLength = 0;

    while (writeLength < LENGTH) {
      // Write whatever in the buffer
      os.write(ret);
      writeLength += ret.length;
    }
    assertEquals(writeLength, LENGTH);
    os.close();

    // 2 blocks are written successfully
    URIStatus status = context.acquireMasterClientResource().get()
            .getStatus(uri, GetStatusPOptions.getDefaultInstance());
    assertEquals(LENGTH, status.getLength());
    assertEquals(2, status.getFileBlockInfos().size());
    // The location is on the undecommissioned worker
    List<BlockLocation> block0Locs = status.getFileBlockInfos().get(0)
            .getBlockInfo().getLocations();
    assertEquals(1, block0Locs.size());
    assertNotEquals(workerToDecommission, block0Locs.get(0).getWorkerAddress());
    List<BlockLocation> block1Locs = status.getFileBlockInfos().get(1)
            .getBlockInfo().getLocations();
    assertEquals(1, block1Locs.size());
    assertNotEquals(workerToDecommission, block1Locs.get(0).getWorkerAddress());

    // The file should be able to be read
    FileInStream is = mFileSystem.openFile(uri, mReadCachePromote);
    int readLength = 0;
    int res = 0;
    while (res != -1) {
      res = is.read(ret);
      if (res != -1) {
        readLength += res;
      }
    }
    assertEquals(readLength, LENGTH);
  }

  @Test
  /*
   * If a stream is created after the worker is decommissioned, it cannot pick that worker.
   * And if that worker is the only one to pick from, the request fails.
   */
  public void cannotWriteFromDecommissionedWorker() throws Exception {
    AlluxioURI uri = new AlluxioURI(mCacheThroughFilePath);
    FileSystemContext context = FileSystemContext
            .create(new TestUserState("test", Configuration.global()).getSubject(),
                    Configuration.global());
    List<WorkerInfo> availableWorkers = context.acquireBlockMasterClientResource()
            .get().getWorkerInfoList();
    assertEquals(2, availableWorkers.size());

    // Decommission one worker in the cluster
    WorkerNetAddress workerToDecommission = mLocalAlluxioClusterResource.get().getWorkerAddress();
    DecommissionWorkerPOptions decomOptions = DecommissionWorkerPOptions.newBuilder()
            .setWorkerHostname(workerToDecommission.getHost())
            .setWorkerWebPort(workerToDecommission.getWebPort())
            .setCanRegisterAgain(true).build();
    context.acquireBlockMasterClientResource().get().decommissionWorker(decomOptions);

    // This stream is able to find the undecommissioned worker and use that to read from UFS
    // Create a stream specifying the target
    CreateFilePOptions writeOptions = CreateFilePOptions.newBuilder()
            .setBlockSizeBytes(BLOCK_SIZE)
            .setWriteType(WritePType.CACHE_THROUGH)
            .setWorkerLocation(GrpcUtils.toProto(workerToDecommission))
            .setRecursive(true).build();
    // The worker has been decommissioned so the file cannot be written
    Exception e = assertThrows(UnavailableException.class, () -> {
      FileOutStream os = mFileSystem.createFile(uri, writeOptions);
      os.write(7);
      os.close();
    });
    assertTrue(e.getMessage().contains(ExceptionMessage.NO_WORKER_AVAILABLE.getMessage()));
  }

  @Test
  /*
   * The target worker is decommissioned while the stream is writing.
   * The stream should succeed but there will be no available cache location to the client.
   */
  public void decommissionWhileWriting() throws Exception {
    AlluxioURI uri = new AlluxioURI(mCacheThroughFilePath);
    FileSystemContext context = FileSystemContext
            .create(new TestUserState("test", Configuration.global()).getSubject(),
                    Configuration.global());
    List<WorkerInfo> availableWorkers = context.acquireBlockMasterClientResource()
            .get().getWorkerInfoList();
    assertEquals(2, availableWorkers.size());

    WorkerNetAddress workerToDecommission = mLocalAlluxioClusterResource.get().getWorkerAddress();
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

    // Create a stream specifying the target
    CreateFilePOptions writeOptions = CreateFilePOptions.newBuilder()
            .setBlockSizeBytes(BLOCK_SIZE)
            .setWriteType(WritePType.CACHE_THROUGH)
            .setWorkerLocation(GrpcUtils.toProto(workerToDecommission))
            .setRecursive(true).build();
    // This stream is able to find the undecommissioned worker and use that to read from UFS
    FileOutStream os = mFileSystem.createFile(uri, writeOptions);
    byte[] ret = new byte[1024];
    // This has created the block stream that writes with the target worker
    int writeLength = 0;

    boolean released = false;
    while (writeLength < LENGTH) {
      if (writeLength > 1024 && !released) {
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
      os.write(ret);
      writeLength += ret.length;
    }
    assertEquals(writeLength, LENGTH);
    os.close();

    // The worker has been decommissioned so the block locations are all empty
    // No cache readable for the client
    URIStatus status = context.acquireMasterClientResource().get()
            .getStatus(uri, GetStatusPOptions.getDefaultInstance());
    List<FileBlockInfo> blockInfos = status.getFileBlockInfos();
    FileBlockInfo block0 = blockInfos.get(0);
    assertEquals(0, block0.getBlockInfo().getLocations().size());
    FileBlockInfo block1 = blockInfos.get(1);
    assertEquals(0, block1.getBlockInfo().getLocations().size());

    // The file should be able to be read
    FileInStream is = mFileSystem.openFile(uri, mReadCachePromote);
    int readLength = 0;
    int res = 0;
    while (res != -1) {
      res = is.read(ret);
      if (res != -1) {
        readLength += res;
      }
    }
    assertEquals(readLength, LENGTH);
  }

  @Test
  /*
   * When there is an active stream writing to one worker, decommission that worker.
   * Then we make the stream wait a bit and realize that worker is no longer available.
   */
  public void halfCacheThroughStreamDecommission() throws Exception {
    AlluxioURI uri = new AlluxioURI(mCacheThroughFilePath);

    FileSystemContext context = FileSystemContext
            .create(new TestUserState("test", Configuration.global()).getSubject(),
                    Configuration.global());
    List<WorkerInfo> clusterWorkers = context.acquireBlockMasterClientResource()
            .get().getWorkerInfoList();
    assertEquals(2, clusterWorkers.size());

    WorkerNetAddress workerToDecommission = mLocalAlluxioClusterResource.get().getWorkerAddress();
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

    // This stream is able to find the undecommissioned worker and use that to write to UFS
    FileOutStream os = mFileSystem.createFile(uri, mWriteBoth);
    byte[] ret = new byte[1024];
    // This has created the block stream that reads from the target worker
    int writeLength = 0;

    boolean released = false;
    while (writeLength < LENGTH) {
      // 2 blocks on the same worker, decommission at the end of one BlockStream
      // so when the FileStream continues, create the 2nd block stream where there's only one
      // usable worker that does not have the block
      if (writeLength == BLOCK_SIZE && !released) {
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
        // Continue the stream, the stream is able to keep going using the decommissioned worker
      }
      os.write(ret);
      writeLength += ret.length;
    }
    assertEquals(writeLength, LENGTH);
    os.close();

    // The worker has been decommissioned so the block locations are all empty
    // No cache readable for the client
    URIStatus statusAfterRead = context.acquireMasterClientResource()
            .get().getStatus(uri, GetStatusPOptions.getDefaultInstance());
    assertEquals(LENGTH, statusAfterRead.getLength());
    assertEquals(2, statusAfterRead.getFileBlockInfos().size());
    assertEquals(0, statusAfterRead.getFileBlockInfos()
            .get(0).getBlockInfo().getLocations().size());
    assertEquals(0, statusAfterRead.getFileBlockInfos()
            .get(1).getBlockInfo().getLocations().size());

    // The file should be able to be read
    FileInStream is = mFileSystem.openFile(uri, mReadCachePromote);
    int readLength = 0;
    int res = 0;
    while (res != -1) {
      res = is.read(ret);
      if (res != -1) {
        readLength += res;
      }
    }
    assertEquals(readLength, LENGTH);
  }

  @Test
  /*
   * When there is an active stream writing to one worker, decommission that worker.
   * Then we make the stream wait a bit and realize that worker is no longer available.
   * The stream is not able to continue because the cache target is gone.
   */
  public void halfStreamMustCacheDecommission() throws Exception {
    AlluxioURI uri = new AlluxioURI(mMustCacheFilePath);

    FileSystemContext context = FileSystemContext
            .create(new TestUserState("test", Configuration.global()).getSubject(),
                    Configuration.global());
    List<WorkerInfo> clusterWorkers = context.acquireBlockMasterClientResource()
            .get().getWorkerInfoList();
    assertEquals(2, clusterWorkers.size());

    WorkerNetAddress workerToDecommission = mLocalAlluxioClusterResource.get().getWorkerAddress();
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

    // This stream is able to find the undecommissioned worker and use that to write to UFS
    FileOutStream os = mFileSystem.createFile(uri, mWriteAlluxio);
    byte[] ret = new byte[1024];
    // This has created the block stream that reads from the target worker
    int writeLength = 0;

    boolean released = false;
    while (writeLength < LENGTH) {
      // 2 blocks on the same worker, decommission at the end of one BlockStream
      // so when the FileStream continues, create the 2nd block stream where there's only one
      // usable worker that does not have the block
      if (writeLength == BLOCK_SIZE && !released) {
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
        // Continue the stream, the stream is not able to continue because the worker is
        // no longer available
        assertThrows(IOException.class, () -> {
          os.write(ret);
        });
        break;
      }
      os.write(ret);
      writeLength += ret.length;
    }
    os.close();

    // The worker has been decommissioned so the block locations are all empty
    // No cache readable for the client
    URIStatus statusAfterRead = context.acquireMasterClientResource()
            .get().getStatus(uri, GetStatusPOptions.getDefaultInstance());
    // The stream was not successful so the length is 0
    assertEquals(0, statusAfterRead.getLength());
  }
}
