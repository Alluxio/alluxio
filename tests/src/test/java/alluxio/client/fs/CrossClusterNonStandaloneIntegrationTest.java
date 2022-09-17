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

import static alluxio.testutils.CrossClusterTestUtils.CREATE_DIR_OPTIONS;
import static alluxio.testutils.CrossClusterTestUtils.CREATE_OPTIONS;
import static alluxio.testutils.CrossClusterTestUtils.assertFileDoesNotExist;
import static alluxio.testutils.CrossClusterTestUtils.checkNonCrossClusterWrite;
import static alluxio.testutils.CrossClusterTestUtils.fileExists;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.master.LocalAlluxioCluster;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.underfs.ConfExpectingUnderFileSystemFactory;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CrossClusterNonStandaloneIntegrationTest {

  private static final String MOUNT_POINT1 = "/mnt1";
  private static final String MOUNT_POINT2 = "/mnt2";
  private static final Map<String, String> UFS_CONF1 = ImmutableMap.of("key1", "val1");
  private static final Map<String, String> UFS_CONF2 = ImmutableMap.of("key2", "val2");

  private ConfExpectingUnderFileSystemFactory mUfsFactory1;
  private ConfExpectingUnderFileSystemFactory mUfsFactory2;
  private AlluxioURI mMountPoint1 = new AlluxioURI(MOUNT_POINT1);
  private String mUfsUri1;
  private String mUfsPath1;
  private UnderFileSystem mLocalUfs;

  private FileSystemCrossCluster mClient1;
  private LocalAlluxioCluster mCluster1;

  private FileSystemCrossCluster mClient2;
  private LocalAlluxioCluster mCluster2;

  WaitForOptions mWaitOptions = WaitForOptions.defaults().setTimeoutMs(5000);

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  String mRootUfs = AlluxioTestDirectory
      .createTemporaryDirectory("FileSystemMasterTest").getAbsolutePath();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource1 =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE, 1)
          .setProperty(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE, 1)
          .setProperty(PropertyKey.MASTER_CROSS_CLUSTER_ENABLE, true)
          .setProperty(PropertyKey.CROSS_CLUSTER_MASTER_STANDALONE, false)
          .setProperty(PropertyKey.CROSS_CLUSTER_MASTER_START_LOCAL, true)
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mRootUfs)
          .setProperty(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES, "localhost:1234")
          .setProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c1")
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH)
          .setStartCluster(true).build();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource2 =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE, 1)
          .setProperty(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE, 1)
          .setProperty(PropertyKey.MASTER_CROSS_CLUSTER_ENABLE, true)
          .setProperty(PropertyKey.CROSS_CLUSTER_MASTER_STANDALONE, false)
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mRootUfs)
          .setProperty(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES, "localhost:1234")
          .setProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c2")
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH)
          .setStartCluster(true).build();

  @Before
  public void before() throws Exception {
    mUfsFactory1 = new ConfExpectingUnderFileSystemFactory("ufs1", UFS_CONF1);
    mUfsFactory2 = new ConfExpectingUnderFileSystemFactory("ufs2", UFS_CONF2);
    UnderFileSystemFactoryRegistry.register(mUfsFactory1);
    UnderFileSystemFactoryRegistry.register(mUfsFactory2);

    mUfsPath1 = mFolder.newFolder().getAbsoluteFile().toString();
    mUfsUri1 = "ufs1://" + mUfsPath1;

    mLocalUfs = new LocalUnderFileSystemFactory().create(mFolder.getRoot().getAbsolutePath(),
        UnderFileSystemConfiguration.defaults(Configuration.global()));

    mCluster1 = mLocalAlluxioClusterResource1.get();
    mClient1 = mLocalAlluxioClusterResource1.getCrossClusterClient();

    mCluster2 = mLocalAlluxioClusterResource2.get();
    mClient2 = mLocalAlluxioClusterResource2.getCrossClusterClient();

    // Update the cross cluster configuration service address with the address
    // that the master bound to
    InetSocketAddress[] configAddress;
    configAddress = new InetSocketAddress[]{
        mCluster1.getLocalAlluxioMaster().getAddress()};
    mClient1.updateCrossClusterConfigurationAddress(configAddress);
    mClient2.updateCrossClusterConfigurationAddress(configAddress);

    // Mount ufs1 to /mnt1 with specified options.
    // Both clusters mount the same path
    MountPOptions options1 = MountPOptions.newBuilder().setCrossCluster(true)
        .putAllProperties(UFS_CONF1).build();
    mClient1.mount(mMountPoint1, new AlluxioURI(mUfsUri1), options1);
    mClient2.mount(mMountPoint1, new AlluxioURI(mUfsUri1), options1);
  }

  @After
  public void after() throws Exception {
    UnderFileSystemFactoryRegistry.unregister(mUfsFactory1);
    UnderFileSystemFactoryRegistry.unregister(mUfsFactory2);
  }

  @Test
  public void crossClusterWrite() throws Exception {
    checkNonCrossClusterWrite(mUfsPath1, mMountPoint1, mClient1, mClient2);

    Stopwatch sw = Stopwatch.createStarted();

    AlluxioURI file1 = mMountPoint1.join("file1");

    // Perform a recursive sync so that the invalidation cache is up-to-date
    mClient1.listStatus(mMountPoint1, ListStatusPOptions.newBuilder()
        .setRecursive(true).build());
    mClient2.listStatus(mMountPoint1, ListStatusPOptions.newBuilder()
        .setRecursive(true).build());
    assertFileDoesNotExist(file1, mClient1, mClient2);

    mClient1.createFile(file1, CREATE_OPTIONS).close();
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file1, mClient1, mClient2),
        mWaitOptions);

    // delete the file outside alluxio, be sure we do not see the update
    mLocalUfs.deleteFile(PathUtils.concatPath(mUfsUri1, "file1"));
    // we wait for the timeout to expire, since we should not see the removal
    Assert.assertThrows(TimeoutException.class,
        () -> CommonUtils.waitFor("File removed outside of alluxio",
            () -> !fileExists(file1, mClient1, mClient2),
            mWaitOptions));

    // now delete the file in alluxio
    mClient2.delete(file1);
    CommonUtils.waitFor("File synced across clusters",
        () -> !fileExists(file1, mClient1, mClient2),
        mWaitOptions);

    // create a directory
    AlluxioURI dir1 = mMountPoint1.join("/dir1");
    assertFileDoesNotExist(dir1, mClient1, mClient2);
    mClient1.createDirectory(dir1, CREATE_DIR_OPTIONS);
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(dir1, mClient1, mClient2),
        mWaitOptions);

    System.out.println("Took: " + sw.elapsed(TimeUnit.MILLISECONDS));
  }
}
