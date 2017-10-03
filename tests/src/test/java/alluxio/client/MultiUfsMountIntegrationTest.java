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
import alluxio.BaseIntegrationTest;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.MountOptions;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.file.FileSystemMaster;
import alluxio.underfs.ConfExpectingUnderFileSystemFactory;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.util.UnderFileSystemUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.MountPointInfo;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Map;

/**
 * Integration tests for mounting multiple UFSes into Alluxio, each with a different configuration.
 */
public final class MultiUfsMountIntegrationTest extends BaseIntegrationTest {
  private static final String MOUNT_POINT1 = "/mnt1";
  private static final String MOUNT_POINT2 = "/mnt2";
  private static final Map<String, String> UFS_CONF1 = ImmutableMap.of("key1", "val1");
  private static final Map<String, String> UFS_CONF2 = ImmutableMap.of("key2", "val2");

  private ConfExpectingUnderFileSystemFactory mUfsFactory1;
  private ConfExpectingUnderFileSystemFactory mUfsFactory2;
  private AlluxioURI mMountPoint1 = new AlluxioURI(MOUNT_POINT1);
  private AlluxioURI mMountPoint2 = new AlluxioURI(MOUNT_POINT2);
  private String mUfsUri1;
  private String mUfsUri2;
  private UnderFileSystem mLocalUfs;
  private FileSystem mFileSystem;
  private LocalAlluxioCluster mLocalAlluxioCluster;

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().setStartCluster(false).build();

  @Before
  public void before() throws Exception {
    mUfsFactory1 = new ConfExpectingUnderFileSystemFactory("ufs1", UFS_CONF1);
    mUfsFactory2 = new ConfExpectingUnderFileSystemFactory("ufs2", UFS_CONF2);
    UnderFileSystemFactoryRegistry.register(mUfsFactory1);
    UnderFileSystemFactoryRegistry.register(mUfsFactory2);

    mUfsUri1 = "ufs1://" + mFolder.newFolder().getAbsoluteFile();
    mUfsUri2 = "ufs2://" + mFolder.newFolder().getAbsoluteFile();
    mLocalUfs = new LocalUnderFileSystemFactory().create(mFolder.getRoot().getAbsolutePath(),
        UnderFileSystemConfiguration.defaults());
    mLocalAlluxioClusterResource.start();
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
    mFileSystem = mLocalAlluxioCluster.getClient();
    // Mount ufs1 to /mnt1 with specified options.
    MountOptions options1 =
        MountOptions.defaults().setProperties(UFS_CONF1);
    mFileSystem.mount(mMountPoint1, new AlluxioURI(mUfsUri1), options1);
    // Mount ufs2 to /mnt2 with specified options.
    MountOptions options2 =
        MountOptions.defaults().setProperties(UFS_CONF2);
    mFileSystem.mount(mMountPoint2, new AlluxioURI(mUfsUri2), options2);
  }

  @After
  public void after() throws Exception {
    UnderFileSystemFactoryRegistry.unregister(mUfsFactory1);
    UnderFileSystemFactoryRegistry.unregister(mUfsFactory2);
  }

  @Test
  public void createFile() throws Exception {
    CreateFileOptions writeBoth =
        CreateFileOptions.defaults().setWriteType(WriteType.CACHE_THROUGH);
    AlluxioURI file1 = mMountPoint1.join("file1");
    AlluxioURI file2 = mMountPoint2.join("file2");
    mFileSystem.createFile(file1, writeBoth).close();
    mFileSystem.createFile(file2, writeBoth).close();
    Assert.assertTrue(mLocalUfs.isFile(PathUtils.concatPath(mUfsUri1, "file1")));
    Assert.assertTrue(mLocalUfs.isFile(PathUtils.concatPath(mUfsUri2, "file2")));
  }

  @Test
  public void createDirectory() throws Exception {
    AlluxioURI dir1 = mMountPoint1.join("dir1");
    AlluxioURI dir2 = mMountPoint2.join("dir2");
    mFileSystem.createDirectory(dir1);
    mFileSystem.createDirectory(dir2);
    Assert.assertTrue(mLocalUfs.isDirectory(PathUtils.concatPath(mUfsUri1, "dir1")));
    Assert.assertTrue(mLocalUfs.isDirectory(PathUtils.concatPath(mUfsUri2, "dir2")));
  }

  @Test
  public void deleteFile() throws Exception {
    String ufsFile1 = PathUtils.concatPath(mUfsUri1, "file1");
    String ufsFile2 = PathUtils.concatPath(mUfsUri2, "file2");
    UnderFileSystemUtils.touch(mLocalUfs, ufsFile1);
    UnderFileSystemUtils.touch(mLocalUfs, ufsFile2);
    AlluxioURI file1 = mMountPoint1.join("file1");
    AlluxioURI file2 = mMountPoint2.join("file2");
    Assert.assertTrue(mFileSystem.exists(file1));
    Assert.assertTrue(mFileSystem.exists(file2));
    mFileSystem.delete(file1);
    mFileSystem.delete(file2);
    Assert.assertFalse(mFileSystem.exists(file1));
    Assert.assertFalse(mFileSystem.exists(file2));
    Assert.assertFalse(mLocalUfs.exists(ufsFile1));
    Assert.assertFalse(mLocalUfs.exists(ufsFile2));
  }

  @Test
  public void deleteDirectory() throws Exception {
    String ufsDir1 = PathUtils.concatPath(mUfsUri1, "dir1");
    String ufsDir2 = PathUtils.concatPath(mUfsUri2, "dir2");
    UnderFileSystemUtils.mkdirIfNotExists(mLocalUfs, ufsDir1);
    UnderFileSystemUtils.mkdirIfNotExists(mLocalUfs, ufsDir2);

    AlluxioURI dir1 = mMountPoint1.join("dir1");
    AlluxioURI dir2 = mMountPoint2.join("dir2");
    Assert.assertTrue(mFileSystem.exists(dir1));
    Assert.assertTrue(mFileSystem.exists(dir2));
    mFileSystem.delete(dir1);
    mFileSystem.delete(dir2);
    Assert.assertFalse(mFileSystem.exists(dir1));
    Assert.assertFalse(mFileSystem.exists(dir2));
  }

  @Test
  public void renameFile() throws Exception {
    String ufsFileSrc1 = PathUtils.concatPath(mUfsUri1, "file1");
    String ufsFileSrc2 = PathUtils.concatPath(mUfsUri2, "file2");
    String ufsFileDst1 = PathUtils.concatPath(mUfsUri1, "renamedFile1");
    String ufsFileDst2 = PathUtils.concatPath(mUfsUri2, "renamedFile2");
    UnderFileSystemUtils.touch(mLocalUfs, ufsFileSrc1);
    UnderFileSystemUtils.touch(mLocalUfs, ufsFileSrc2);
    AlluxioURI fileSrc1 = mMountPoint1.join("file1");
    AlluxioURI fileSrc2 = mMountPoint2.join("file2");
    AlluxioURI fileDst1 = mMountPoint1.join("renamedFile1");
    AlluxioURI fileDst2 = mMountPoint2.join("renamedFile2");
    Assert.assertTrue(mFileSystem.exists(fileSrc1));
    Assert.assertTrue(mFileSystem.exists(fileSrc2));
    mFileSystem.rename(fileSrc1, fileDst1);
    mFileSystem.rename(fileSrc2, fileDst2);
    Assert.assertFalse(mFileSystem.exists(fileSrc1));
    Assert.assertFalse(mFileSystem.exists(fileSrc2));
    Assert.assertTrue(mFileSystem.exists(fileDst1));
    Assert.assertTrue(mFileSystem.exists(fileDst2));
    Assert.assertFalse(mLocalUfs.exists(ufsFileSrc1));
    Assert.assertFalse(mLocalUfs.exists(ufsFileSrc2));
    Assert.assertTrue(mLocalUfs.exists(ufsFileDst1));
    Assert.assertTrue(mLocalUfs.exists(ufsFileDst2));
  }

  @Test
  public void renameDirectory() throws Exception {
    String ufsDirSrc1 = PathUtils.concatPath(mUfsUri1, "dir1");
    String ufsDirSrc2 = PathUtils.concatPath(mUfsUri2, "dir2");
    String ufsDirDst1 = PathUtils.concatPath(mUfsUri1, "renamedDir1");
    String ufsDirDst2 = PathUtils.concatPath(mUfsUri2, "renamedDir2");
    UnderFileSystemUtils.touch(mLocalUfs, ufsDirSrc1);
    UnderFileSystemUtils.touch(mLocalUfs, ufsDirSrc2);
    AlluxioURI dirSrc1 = mMountPoint1.join("dir1");
    AlluxioURI dirSrc2 = mMountPoint2.join("dir2");
    AlluxioURI dirDst1 = mMountPoint1.join("renamedDir1");
    AlluxioURI dirDst2 = mMountPoint2.join("renamedDir2");
    Assert.assertTrue(mFileSystem.exists(dirSrc1));
    Assert.assertTrue(mFileSystem.exists(dirSrc2));
    mFileSystem.rename(dirSrc1, dirDst1);
    mFileSystem.rename(dirSrc2, dirDst2);
    Assert.assertFalse(mFileSystem.exists(dirSrc1));
    Assert.assertFalse(mFileSystem.exists(dirSrc2));
    Assert.assertTrue(mFileSystem.exists(dirDst1));
    Assert.assertTrue(mFileSystem.exists(dirDst2));
    Assert.assertFalse(mLocalUfs.exists(ufsDirSrc1));
    Assert.assertFalse(mLocalUfs.exists(ufsDirSrc2));
    Assert.assertTrue(mLocalUfs.exists(ufsDirDst1));
    Assert.assertTrue(mLocalUfs.exists(ufsDirDst2));
  }

  @Test
  public void openFile() throws Exception {
    String ufsFile1 = PathUtils.concatPath(mUfsUri1, "file1");
    String ufsFile2 = PathUtils.concatPath(mUfsUri2, "file2");
    UnderFileSystemUtils.touch(mLocalUfs, ufsFile1);
    UnderFileSystemUtils.touch(mLocalUfs, ufsFile2);
    AlluxioURI file1 = mMountPoint1.join("file1");
    AlluxioURI file2 = mMountPoint2.join("file2");
    Assert.assertTrue(mFileSystem.exists(file1));
    Assert.assertTrue(mFileSystem.exists(file2));
    FileInStream inStream1 = mFileSystem.openFile(file1);
    Assert.assertNotNull(inStream1);
    inStream1.close();
    FileInStream inStream2 = mFileSystem.openFile(file2);
    Assert.assertNotNull(inStream2);
    inStream2.close();
  }

  @Test
  public void mountAfterMasterRestart() throws Exception {
    mLocalAlluxioCluster.stopFS();
    MasterRegistry registry = MasterTestUtils.createLeaderFileSystemMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
    Map<String, MountPointInfo> mountTable = fsMaster.getMountTable();
    Assert.assertTrue(mountTable.containsKey(MOUNT_POINT1));
    Assert.assertTrue(mountTable.containsKey(MOUNT_POINT2));
    MountPointInfo mountPointInfo1 = mountTable.get(MOUNT_POINT1);
    MountPointInfo mountPointInfo2 = mountTable.get(MOUNT_POINT2);
    Assert.assertEquals(mUfsUri1, mountPointInfo1.getUfsUri());
    Assert.assertEquals(mUfsUri2, mountPointInfo2.getUfsUri());
    Assert.assertEquals(UFS_CONF1, mountPointInfo1.getProperties());
    Assert.assertEquals(UFS_CONF2, mountPointInfo2.getProperties());
  }

  @Test
  public void mountAfterWorkerRestart() throws Exception {
    String ufsFile1 = PathUtils.concatPath(mUfsUri1, "file1");
    String ufsFile2 = PathUtils.concatPath(mUfsUri2, "file2");
    UnderFileSystemUtils.touch(mLocalUfs, ufsFile1);
    UnderFileSystemUtils.touch(mLocalUfs, ufsFile2);
    mLocalAlluxioCluster.stopWorkers();
    mLocalAlluxioCluster.startWorkers();
    AlluxioURI file1 = mMountPoint1.join("file1");
    AlluxioURI file2 = mMountPoint2.join("file2");
    Assert.assertTrue(mFileSystem.exists(file1));
    Assert.assertTrue(mFileSystem.exists(file2));
    FileInStream inStream1 = mFileSystem.openFile(file1);
    Assert.assertNotNull(inStream1);
    inStream1.close();
    FileInStream inStream2 = mFileSystem.openFile(file2);
    Assert.assertNotNull(inStream2);
    inStream2.close();
  }
}
