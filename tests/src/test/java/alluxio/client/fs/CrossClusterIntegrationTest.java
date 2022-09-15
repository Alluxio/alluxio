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

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.client.WriteType;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioCluster;
import alluxio.security.authorization.AclEntry;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.underfs.ConfExpectingUnderFileSystemFactory;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.underfs.local.LocalUnderFileSystemFactory;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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

  static final CreateFilePOptions CREATE_OPTIONS =
      CreateFilePOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build();

  static final CreateDirectoryPOptions CREATE_DIR_OPTIONS =
      CreateDirectoryPOptions.newBuilder().setWriteType(WritePType.CACHE_THROUGH).build();

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
          .setProperty(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES, format("localhost:%d",
              Configuration.getInt(PropertyKey.CROSS_CLUSTER_MASTER_RPC_PORT)))
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
          .setProperty(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES, format("localhost:%d",
              Configuration.getInt(PropertyKey.CROSS_CLUSTER_MASTER_RPC_PORT)))
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

    mUfsUri1 = "ufs1://" + mFolder.newFolder().getAbsoluteFile();
    mUfsUri2 = "ufs2://" + mFolder.newFolder().getAbsoluteFile();

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

  static boolean fileExists(AlluxioURI path, FileSystem ... fsArray) {
    for (FileSystem fs : fsArray) {
      try {
        fs.getStatus(path);
      } catch (FileDoesNotExistException e) {
        return false;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return true;
  }

  static void assertFileDoesNotExist(AlluxioURI path, FileSystem ... fsArray) {
    for (FileSystem fs : fsArray) {
      assertThrows(FileDoesNotExistException.class,
          () -> fs.getStatus(path));
    }
  }

  static void assertFileExists(AlluxioURI path, FileSystem ... fsArray)
      throws Exception {
    for (FileSystem fs : fsArray) {
      fs.getStatus(path);
    }
  }

  static void checkNonCrossClusterWrite(FileSystem client1, FileSystem client2) throws Exception {
    // ensure without cross cluster sync there is no visibility across clusters
    AlluxioURI file1 = new AlluxioURI("/file1");
    assertFileDoesNotExist(file1, client1, client2);
    client1.createFile(file1, CREATE_OPTIONS).close();
    assertFileExists(file1, client1);
    // be sure after a timeout the file still does not exist on cluster2
    Assert.assertThrows(TimeoutException.class,
        () -> CommonUtils.waitFor("File synced across clusters",
            () -> fileExists(file1, client2),
            WaitForOptions.defaults().setTimeoutMs(3000)));
  }

  @After
  public void after() throws Exception {
    UnderFileSystemFactoryRegistry.unregister(mUfsFactory1);
    UnderFileSystemFactoryRegistry.unregister(mUfsFactory2);
  }

  @Test
  public void crossClusterWrite() throws Exception {
    checkNonCrossClusterWrite(mClient1, mClient2);

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
