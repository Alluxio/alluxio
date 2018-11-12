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
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemClientOptions;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.grpc.WritePType;
import alluxio.master.file.meta.PersistenceState;
import alluxio.testutils.IntegrationTestUtils;
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
    final int length = 2;
    FileOutStream os = mFileSystem.createFile(filePath, FileSystemClientOptions
        .getCreateFileOptions().toBuilder().setWriteType(WritePType.WRITE_ASYNC_THROUGH).build());
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    CommonUtils.sleepMs(1);
    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, filePath);

    status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    checkFileInAlluxio(filePath, length);
    checkFileInUnderStorage(filePath, length);
  }

  @Test
  public void asyncWriteEmptyFile() throws Exception {
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createFile(filePath, FileSystemClientOptions.getCreateFileOptions().toBuilder()
        .setWriteType(WritePType.WRITE_ASYNC_THROUGH).build()).close();

    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertNotEquals(PersistenceState.PERSISTED, status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, filePath);

    status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    checkFileInAlluxio(filePath, 0);
    checkFileInUnderStorage(filePath, 0);
  }
}
