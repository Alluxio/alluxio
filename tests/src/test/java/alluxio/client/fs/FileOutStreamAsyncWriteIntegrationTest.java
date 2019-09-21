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

import alluxio.AlluxioURI;
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.client.job.JobGrpcClientUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.job.load.LoadConfig;
import alluxio.master.MasterClientContext;
import alluxio.master.file.meta.PersistenceState;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.FormatUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Integration tests for {@link alluxio.client.file.FileOutStream} of under storage type being async
 * persist.
 *
 */
public final class FileOutStreamAsyncWriteIntegrationTest
    extends AbstractFileOutStreamIntegrationTest {

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
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), srcStatus.getPersistenceState());
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
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.toString(), srcStatus.getPersistenceState());
    Assert.assertTrue(srcStatus.isCompleted());

    mFileSystem.persist(filePath);
    CommonUtils.sleepMs(1);
    checkPersistStateAndWaitForPersist(filePath, 2);
  }

  @Test

  @LocalAlluxioClusterResource.Config(confParams = {
      "alluxio.user.block.size.bytes.default", "256",
      "alluxio.user.file.buffer.bytes", "1k",
      "alluxio.worker.memory.size", "32M",
      "alluxio.job.master.finished.job.retention.time", "0sec",
      "alluxio.job.master.job.capacity", "500",
      "alluxio.master.replication.check.interval", "2s"

  })

  public void badSpaceReserverTest() throws Exception {
    FileSystem fs = mLocalAlluxioClusterResource.get().getClient();
    String dir = "/test";
    fs.createDirectory(new AlluxioURI(dir));
    long totalCapacity = FormatUtils.parseSpaceSize("32M");
    double capMultiplier = 0.94;
    long bytesToWrite = (long) Math.ceil(capMultiplier * totalCapacity);
    long fileSize = 4 * Constants.MB; // bytes per fil
    long filesToWrite = bytesToWrite / fileSize;
    ConcurrentSkipListSet<String> files = new ConcurrentSkipListSet<>();
    for (int i = 0; i < filesToWrite; i++) {
      files.add(PathUtils.concatPath(dir, UUID.randomUUID().toString()));
    }
    ExecutorService svc = Executors.newWorkStealingPool(4);
    doSomethingAllFiles(svc, files, name -> FileSystemTestUtils.createByteFile(fs,
        name,
        (int) fileSize,
        CreateFilePOptions.newBuilder()
            .setWriteType(WritePType.THROUGH)
            .setRecursive(true)
            .build()
    ));
    fs.setAttribute(new AlluxioURI(dir), SetAttributePOptions.newBuilder()
        .setReplicationMax(0)
        .setReplicationMin(0)
        .setRecursive(true)
        .build());
    doSomethingAllFiles(svc, files, name -> {
      try {
        JobGrpcClientUtils.run(new LoadConfig(name, 1), 2, ServerConfiguration.global());
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });
    Thread.sleep(Constants.HOUR);
    svc.shutdown();
  }

  void doSomethingAllFiles(ExecutorService svc, Collection<String> operands,
      Consumer<String> callable) throws Exception {
    operands.stream().map(path -> svc.submit(() -> callable.accept(path)))
        .collect(Collectors.toList()).forEach(f -> {
      try {
        f.get();
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
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
  public void asyncWriteEmptyFile() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createFile(filePath, CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.ASYNC_THROUGH).setRecursive(true).build()).close();

    checkPersistStateAndWaitForPersist(filePath, 0);
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
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(),
        status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, path);

    status = mFileSystem.getStatus(path);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    checkFileInAlluxio(path, length);
    checkFileInUnderStorage(path, length);
  }
}
