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
import alluxio.client.fs.io.AbstractFileOutStreamIntegrationTest;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.file.meta.PersistenceState;
import alluxio.testutils.IntegrationTestUtils;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * Integration tests of file permission propagation for persist and async persist.
 */
public final class PersistPermissionIntegrationTest extends AbstractFileOutStreamIntegrationTest {
  private String mUfsRoot;
  private UnderFileSystem mUfs;

  @Before
  @Override
  public void before() throws Exception {
    super.before();

    mUfsRoot = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    mUfs = UnderFileSystem.Factory.createForRoot(ServerConfiguration.global());
  }

  @Test
  public void syncPersistPermission() throws Exception {
    // Skip non-local and non-HDFS UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isLocal(mUfs) || UnderFileSystemUtils.isHdfs(mUfs));

    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    FileOutStream os = mFileSystem.createFile(filePath, CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.CACHE_THROUGH).setRecursive(true).build());
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    // Check the file is persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());
    short fileMode = (short) status.getMode();
    short parentMode = (short) mFileSystem.getStatus(filePath.getParent()).getMode();

    // Check the permission of the created file and parent dir are in-sync between Alluxio and UFS.
    Assert.assertEquals(fileMode,
        mUfs.getFileStatus(PathUtils.concatPath(mUfsRoot, filePath)).getMode());
    Assert.assertEquals(parentMode,
        mUfs.getDirectoryStatus(PathUtils.concatPath(mUfsRoot, filePath.getParent())).getMode());
  }

  @Test
  public void asyncPersistPermission() throws Exception {
    // Skip non-local and non-HDFS UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isLocal(mUfs) || UnderFileSystemUtils.isHdfs(mUfs));

    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    FileOutStream os = mFileSystem.createFile(filePath, CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.ASYNC_THROUGH).setRecursive(true).build());
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    CommonUtils.sleepMs(1);
    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.TO_BE_PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());
    short fileMode = (short) status.getMode();
    short parentMode = (short) mFileSystem.getStatus(filePath.getParent()).getMode();

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, filePath);

    status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    // Check the permission of the created file and parent dir are in-sync between Alluxio and UFS.
    Assert.assertEquals(fileMode,
        mUfs.getFileStatus(PathUtils.concatPath(mUfsRoot, filePath)).getMode());
    Assert.assertEquals(parentMode,
        mUfs.getDirectoryStatus(PathUtils.concatPath(mUfsRoot, filePath.getParent())).getMode());
  }

  @Test
  public void asyncPersistEmptyFilePermission() throws Exception {
    // Skip non-local and non-HDFS UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isLocal(mUfs) || UnderFileSystemUtils.isHdfs(mUfs));

    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createFile(filePath, CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.ASYNC_THROUGH).setRecursive(true).build()).close();

    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertNotEquals(PersistenceState.PERSISTED, status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());
    short fileMode = (short) status.getMode();
    short parentMode = (short) mFileSystem.getStatus(filePath.getParent()).getMode();

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, filePath);

    status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    // Check the permission of the created file and parent dir are in-sync between Alluxio and UFS.
    Assert.assertEquals(fileMode,
        mUfs.getFileStatus(PathUtils.concatPath(mUfsRoot, filePath)).getMode());
    Assert.assertEquals(parentMode,
        mUfs.getDirectoryStatus(PathUtils.concatPath(mUfsRoot, filePath.getParent())).getMode());
  }
}
