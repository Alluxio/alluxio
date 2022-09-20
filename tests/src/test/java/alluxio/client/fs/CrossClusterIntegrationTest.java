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
import static alluxio.testutils.CrossClusterTestUtils.checkClusterSyncAcrossAll;
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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

//@RunWith(PowerMockRunner.class)
//@PrepareForTest({CrossClusterMount.class})
public class CrossClusterIntegrationTest {

  private static final String MOUNT_POINT1 = "/mnt1";
  private static final String MOUNT_POINT2 = "/mnt2";
  private static final String MOUNT_POINT3 = "/mnt3";
  private static final String MOUNT_POINT4_NESTED = "/mnt3/mnt4";
  private static final Map<String, String> UFS_CONF1 = ImmutableMap.of("key1", "val1");
  private static final Map<String, String> UFS_CONF2 = ImmutableMap.of("key2", "val2");

  private ConfExpectingUnderFileSystemFactory mUfsFactory1;
  private ConfExpectingUnderFileSystemFactory mUfsFactory2;
  private AlluxioURI mMountPoint1 = new AlluxioURI(MOUNT_POINT1);
  private AlluxioURI mMountPoint2 = new AlluxioURI(MOUNT_POINT2);
  private String mUfsUri1;
  private String mUfsUri2;
  private String mUfsPath1;
  private String mUfsPath2;
  private UnderFileSystem mLocalUfs;

  private FileSystemCrossCluster mClient1;
  private LocalAlluxioCluster mCluster1;

  private FileSystemCrossCluster mClient2;
  private LocalAlluxioCluster mCluster2;

  WaitForOptions mWaitOptions = WaitForOptions.defaults().setTimeoutMs(5000);

//  private static final String TEST_USER = "test";
//  @Rule
//  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER,
//      Configuration.global());

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
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mRootUfs)
          // .setProperty(PropertyKey.CROSS_CLUSTER_MASTER_STANDALONE, true)
          // .setProperty(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL)
          //.setProperty(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES, format("localhost:%d",
          // Configuration.getInt(PropertyKey.CROSS_CLUSTER_MASTER_RPC_PORT)))
          .setProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c1")
          //.setProperty(PropertyKey.SECURITY_LOGIN_USERNAME, TEST_USER)
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH)
          .setStartCluster(true).includeCrossClusterStandalone().build();

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource2 =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE, 1)
          .setProperty(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE, 1)
          //.setProperty(PropertyKey.MASTER_CROSS_CLUSTER_INVALIDATION_QUEUE_SIZE, 10)
          //.setProperty(PropertyKey.MASTER_CROSS_CLUSTER_INVALIDATION_QUEUE_WAIT, 1000)
          .setProperty(PropertyKey.MASTER_CROSS_CLUSTER_ENABLE, true)
          .setProperty(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, mRootUfs)
          // .setProperty(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.NOSASL)
          //.setProperty(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES, format("localhost:%d",
          //Configuration.getInt(PropertyKey.CROSS_CLUSTER_MASTER_RPC_PORT)))
          .setProperty(PropertyKey.MASTER_CROSS_CLUSTER_ID, "c2")
          //.setProperty(PropertyKey.SECURITY_LOGIN_USERNAME, TEST_USER)
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.CACHE_THROUGH)
          .setStartCluster(true).build();

  @Before
  public void before() throws Exception {
//    PowerMockito.whenNew(InvalidationStream.class).withAnyArguments().thenAnswer(invocation -> {
//      MountSyncAddress mount = invocation.getArgument(0);
//      if (mount.getMountSync().getClusterId().equals("c1")) {
//        InvalidationStream stream = (InvalidationStream) Mockito.spy(invocation.callRealMethod());
//        return stream;
//      } else {
//        return invocation.callRealMethod();
//      }
//    });

    mUfsFactory1 = new ConfExpectingUnderFileSystemFactory("ufs1", UFS_CONF1);
    mUfsFactory2 = new ConfExpectingUnderFileSystemFactory("ufs2", UFS_CONF2);
    UnderFileSystemFactoryRegistry.register(mUfsFactory1);
    UnderFileSystemFactoryRegistry.register(mUfsFactory2);

    mUfsPath1 = mFolder.newFolder().getAbsoluteFile().toString();
    mUfsUri1 = "ufs1://" + mUfsPath1;
    mUfsPath2 = mFolder.newFolder().getAbsoluteFile().toString();
    mUfsUri2 = "ufs2://" + mUfsPath2;

    mLocalUfs = new LocalUnderFileSystemFactory().create(mFolder.getRoot().getAbsolutePath(),
        UnderFileSystemConfiguration.defaults(Configuration.global()));

    mCluster1 = mLocalAlluxioClusterResource1.get();
    mClient1 = mLocalAlluxioClusterResource1.getCrossClusterClient();

    mCluster2 = mLocalAlluxioClusterResource2.get();
    mClient2 = mLocalAlluxioClusterResource2.getCrossClusterClient();

    // Mount ufs1 to /mnt1 with specified options.
    // Both clusters mount the same path
    MountPOptions options1 = MountPOptions.newBuilder().setCrossCluster(true)
        .putAllProperties(UFS_CONF1).build();
    mClient1.mount(mMountPoint1, new AlluxioURI(mUfsUri1), options1);
    mClient2.mount(mMountPoint1, new AlluxioURI(mUfsUri1), options1);

    // Thread.sleep(5000);

    // mClient1.unmount(mMountPoint1);

    // Thread.sleep(200000);
    // Mount ufs2 to /mnt2 with specified options.
    // MountPOptions options2 = MountPOptions.newBuilder().putAllProperties(UFS_CONF2).build();
    // mClient1.mount(mMountPoint2, new AlluxioURI(mUfsUri2), options2);
  }

  @After
  public void after() throws Exception {
    UnderFileSystemFactoryRegistry.unregister(mUfsFactory1);
    UnderFileSystemFactoryRegistry.unregister(mUfsFactory2);
  }

  @Test
  public void crossClusterMasterRestart() throws Exception {
    checkNonCrossClusterWrite(mUfsPath1, mMountPoint1, mClient1, mClient2);

    AlluxioURI file1 = mMountPoint1.join("file1");
    assertFileDoesNotExist(file1, mClient1, mClient2);
    mClient1.createFile(file1, CREATE_OPTIONS).close();
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file1, mClient1, mClient2),
        mWaitOptions);

    mCluster1.restartCrossClusterMaster();

    AlluxioURI file2 = mMountPoint1.join("file2");
    assertFileDoesNotExist(file2, mClient1, mClient2);
    mClient1.createFile(file2, CREATE_OPTIONS).close();
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file2, mClient1, mClient2),
        mWaitOptions);
    // be sure we can make a new mount where files are synced
    MountPOptions options2 = MountPOptions.newBuilder().setCrossCluster(true)
        .putAllProperties(UFS_CONF2).build();
    mClient1.mount(mMountPoint2, new AlluxioURI(mUfsUri2), options2);
    mClient2.mount(mMountPoint2, new AlluxioURI(mUfsUri2), options2);

    AlluxioURI file1Mnt2 = mMountPoint2.join("file1");
    assertFileDoesNotExist(file1Mnt2, mClient1, mClient2);
    mClient1.createFile(file1Mnt2, CREATE_OPTIONS).close();
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file1Mnt2, mClient1, mClient2),
        mWaitOptions);

    AlluxioURI file2Mnt2 = mMountPoint2.join("file2");
    assertFileDoesNotExist(file2Mnt2, mClient1, mClient2);

    // be sure files are synced in both directions
    checkClusterSyncAcrossAll(mMountPoint2, mClient1, mClient2);
  }

  @Test
  public void crossClusterMasterStopped() throws Exception {
    checkNonCrossClusterWrite(mUfsPath1, mMountPoint1, mClient1, mClient2);

    AlluxioURI file1 = mMountPoint1.join("file1");
    assertFileDoesNotExist(file1, mClient1, mClient2);
    mClient1.createFile(file1, CREATE_OPTIONS).close();
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file1, mClient1, mClient2),
        mWaitOptions);

    // stop the cross cluster master
    mCluster1.stopCrossClusterMaster();

    AlluxioURI file2 = mMountPoint1.join("file2");
    assertFileDoesNotExist(file2, mClient1, mClient2);
    mClient1.createFile(file2, CREATE_OPTIONS).close();
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file2, mClient1, mClient2),
        mWaitOptions);
    // be sure we can make a new mount where files are synced
    MountPOptions options2 = MountPOptions.newBuilder().setCrossCluster(true)
        .putAllProperties(UFS_CONF2).build();
    mClient1.mount(mMountPoint2, new AlluxioURI(mUfsUri2), options2);
    mClient2.mount(mMountPoint2, new AlluxioURI(mUfsUri2), options2);

    // be sure files are not synced while the cross cluster master is stopped
    AlluxioURI file1Mnt2 = mMountPoint2.join("file1");
    assertFileDoesNotExist(file1Mnt2, mClient1, mClient2);
    mClient1.createFile(file1Mnt2, CREATE_OPTIONS).close();
    Assert.assertThrows(TimeoutException.class,
        () -> CommonUtils.waitFor("File synced when cross cluster master not started",
            () -> {
              assertFileDoesNotExist(file1Mnt2, mClient2);
              return false;
            }, mWaitOptions));
    // start the cross cluster master, ensure syncing happens
    mCluster1.startCrossClusterMaster();
    CommonUtils.waitFor("File synced across clusters",
        () -> fileExists(file1Mnt2, mClient1, mClient2),
        mWaitOptions.setTimeoutMs(10000));

    AlluxioURI file2Mnt2 = mMountPoint2.join("file2");
    assertFileDoesNotExist(file2Mnt2, mClient1, mClient2);

    // be sure files are synced in both directions
    checkClusterSyncAcrossAll(mMountPoint2, mClient1, mClient2);
  }

  @Test
  public void crossClusterWrite() throws Exception {
    checkNonCrossClusterWrite(mUfsPath1, mMountPoint1, mClient1, mClient2);

    Stopwatch sw = Stopwatch.createStarted();

    AlluxioURI file1 = mMountPoint1.join("file1");
    AlluxioURI file2 = mMountPoint1.join("file2");

    //assertFileDoesNotExist(file1, mClient1, mClient2);
    //assertFileDoesNotExist(file2, mClient1, mClient2);

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

    // be sure files are synced in both directions
    checkClusterSyncAcrossAll(mMountPoint1, mClient1, mClient2);

    // chane the ACL
    // TODO(tcrain) need to use a UFS that supports ACL
//    Set<String> newEntries = Sets.newHashSet("default:user::rwx",
//        "default:group::rwx", "default:other::r-x");
//    URIStatus status1 = mClient1.getStatus(dir1);
//    assertNotEquals(newEntries,
//        Sets.newHashSet(status1.getFileInfo().convertDefaultAclToStringEntries()));
//    mClient1.setAcl(dir1, SetAclAction.REPLACE,
//        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()),
//        SetAclPOptions.getDefaultInstance());
//    HashSet<String> entries = Sets.newHashSet(mClient1
//        .getStatus(dir1).getFileInfo().convertDefaultAclToStringEntries());
//    assertEquals(newEntries, entries);
//    // ensure it is updated on the other cluster
//    CommonUtils.waitFor("ACL updated across clusters", () -> {
//      try {
//        return Sets.newHashSet(mClient2
//                .getStatus(dir1).getFileInfo().convertDefaultAclToStringEntries())
//            .equals(newEntries);
//      } catch (Exception e) {
//        throw new RuntimeException(e);
//      }
//    });
    System.out.println("Took: " + sw.elapsed(TimeUnit.MILLISECONDS));
  }
}
