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

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.IntegrationTestUtils;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.master.file.meta.PersistenceState;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.hdfs.HdfsUnderFileSystem;
import alluxio.underfs.local.LocalUnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
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

    mUfsRoot = PathUtils.concatPath(Configuration.get(Constants.UNDERFS_ADDRESS));
    mUfs = UnderFileSystem.get(mUfsRoot);
  }

  @Test
  public void syncPersistPermissionTest() throws Exception {
    if (!(mUfs instanceof LocalUnderFileSystem) && !(mUfs instanceof HdfsUnderFileSystem)) {
      // Skip non-local and non-HDFS UFSs.
      return;
    }
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    FileOutStream os = mFileSystem.createFile(filePath,
        CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH));
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
    Assert.assertEquals(fileMode, mUfs.getMode(PathUtils.concatPath(mUfsRoot, filePath)));
    Assert.assertEquals(parentMode,
        mUfs.getMode(PathUtils.concatPath(mUfsRoot, filePath.getParent())));
  }

  @Test
  public void asyncPersistPermissionTest() throws Exception {
    if (!(mUfs instanceof LocalUnderFileSystem) && !(mUfs instanceof HdfsUnderFileSystem)) {
      // Skip non-local and non-HDFS UFSs.
      return;
    }
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    FileOutStream os = mFileSystem.createFile(filePath,
        CreateFileOptions.defaults().setWriteType(WriteType.ASYNC_THROUGH));
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    CommonUtils.sleepMs(1);
    // check the file is completed but not persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.IN_PROGRESS.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());
    short fileMode = (short) status.getMode();
    short parentMode = (short) mFileSystem.getStatus(filePath.getParent()).getMode();

    IntegrationTestUtils.waitForPersist(mLocalAlluxioClusterResource, filePath);

    status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());

    // Check the permission of the created file and parent dir are in-sync between Alluxio and UFS.
    Assert.assertEquals(fileMode, mUfs.getMode(PathUtils.concatPath(mUfsRoot, filePath)));
    Assert.assertEquals(parentMode,
        mUfs.getMode(PathUtils.concatPath(mUfsRoot, filePath.getParent())));
  }

  @Test
  public void asyncPersistEmptyFilePermissionTest() throws Exception {
    if (!(mUfs instanceof LocalUnderFileSystem) && !(mUfs instanceof HdfsUnderFileSystem)) {
      // Skip non-local and non-HDFS UFSs.
      return;
    }
    AlluxioURI filePath = new AlluxioURI(PathUtils.uniqPath());
    mFileSystem.createFile(filePath, CreateFileOptions.defaults()
        .setWriteType(WriteType.ASYNC_THROUGH)).close();

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
    Assert.assertEquals(fileMode, mUfs.getMode(PathUtils.concatPath(mUfsRoot, filePath)));
    Assert.assertEquals(parentMode,
        mUfs.getMode(PathUtils.concatPath(mUfsRoot, filePath.getParent())));
  }
}
