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
import alluxio.underfs.UnderFileSystem;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Integration tests of file permission propagation for persist and async persist.
 */
public final class PersistMultipleMountsIntegrationTest
    extends AbstractFileOutStreamIntegrationTest {
  private static final String MOUNT_PATH = "/mounted";

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  private String mUfsRoot;
  private UnderFileSystem mUfs;
  private String mMountedUfsRoot;
  private UnderFileSystem mMountedUfs;

  @Before
  @Override
  public void before() throws Exception {
    super.before();

    mUfsRoot = PathUtils.concatPath(ServerConfiguration
        .get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS));
    mUfs = UnderFileSystem.Factory.create(mUfsRoot, ServerConfiguration.global());

    mMountedUfsRoot = mTempFolder.getRoot().getAbsolutePath();
    mFileSystem.mount(new AlluxioURI(MOUNT_PATH), new AlluxioURI(mMountedUfsRoot));
    mMountedUfs = UnderFileSystem.Factory.create(mMountedUfsRoot, ServerConfiguration.global());
  }

  @Test
  public void syncMultipleMountsDefaultPersist() throws Exception {
    // Skip non-local and non-HDFS UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isLocal(mUfs) || UnderFileSystemUtils.isHdfs(mUfs));

    String path = PathUtils.uniqPath();
    AlluxioURI filePath = new AlluxioURI(path);
    FileOutStream os = mFileSystem.createFile(filePath, CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.CACHE_THROUGH).setRecursive(true).build());
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    // Check the file is persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());
    Assert.assertTrue(mUfs.exists(PathUtils.concatPath(mUfsRoot, path)));
    Assert.assertFalse(mMountedUfs.exists(PathUtils.concatPath(mMountedUfsRoot, path)));
  }

  @Test
  public void syncMultipleMountsMountedPersist() throws Exception {
    // Skip non-local and non-HDFS UFSs.
    Assume.assumeTrue(UnderFileSystemUtils.isLocal(mUfs) || UnderFileSystemUtils.isHdfs(mUfs));

    String path = PathUtils.uniqPath();
    AlluxioURI filePath = new AlluxioURI(MOUNT_PATH + path);
    FileOutStream os = mFileSystem.createFile(filePath, CreateFilePOptions.newBuilder()
        .setWriteType(WritePType.CACHE_THROUGH).setRecursive(true).build());
    os.write((byte) 0);
    os.write((byte) 1);
    os.close();

    // Check the file is persisted
    URIStatus status = mFileSystem.getStatus(filePath);
    Assert.assertEquals(PersistenceState.PERSISTED.toString(), status.getPersistenceState());
    Assert.assertTrue(status.isCompleted());
    Assert.assertFalse(mUfs.exists(PathUtils.concatPath(mUfsRoot, path)));
    Assert.assertTrue(mMountedUfs.exists(PathUtils.concatPath(mMountedUfsRoot, path)));
  }
}
