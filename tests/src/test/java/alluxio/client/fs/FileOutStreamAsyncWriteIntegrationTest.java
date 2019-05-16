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
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.MasterClientContext;
import alluxio.master.file.meta.PersistenceState;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Test;

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
