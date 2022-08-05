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

package alluxio.master.file.meta.cross.cluster;

import static org.junit.Assert.assertThrows;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.ConfigurationRule;
import alluxio.client.WriteType;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.FileDoesNotExistException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.MountList;
import alluxio.grpc.MountPOptions;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.cross.cluster.CrossClusterState;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.FileSystemMasterTest;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.meta.TtlIntervalRule;
import alluxio.master.journal.JournalType;

import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class CrossClusterFileSystemTest {

  private CrossClusterState mCrossClusterState;
  private Map<String, FileSystemMasterTest> mFileSystems;
  private Map<String, CrossClusterMount> mCrossClusterMounts;
  private final int mClusterCount = 3;

  DeleteContext deleteContext(boolean recursive) {
    return DeleteContext.mergeFrom(DeletePOptions.newBuilder()
        .setRecursive(recursive));
  }

  CreateFileContext createFileContext(boolean recursive) {
    return CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setRecursive(recursive)).setWriteType(WriteType.CACHE_THROUGH);
  }

  CreateDirectoryContext createDirectoryContext(boolean recursive) {
    return CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder()
        .setRecursive(recursive)).setWriteType(WriteType.CACHE_THROUGH);
  }

  private static final String TEST_USER = "test";

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER,
      Configuration.global());

  @Rule
  public ConfigurationRule mConfigurationRule =
      new ConfigurationRule(new HashMap<PropertyKey, Object>() {
        {
          put(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS);
          put(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "000");
          put(PropertyKey.MASTER_JOURNAL_TAILER_SLEEP_TIME_MS, 20);
          put(PropertyKey.MASTER_JOURNAL_TAILER_SHUTDOWN_QUIET_WAIT_TIME_MS, 0);
          put(PropertyKey.WORK_DIR,
              AlluxioTestDirectory.createTemporaryDirectory("workdir").getAbsolutePath());
          put(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS, AlluxioTestDirectory
              .createTemporaryDirectory("FileSystemMasterTest").getAbsolutePath());
          put(PropertyKey.MASTER_FILE_SYSTEM_OPERATION_RETRY_CACHE_ENABLED, false);
          put(PropertyKey.MASTER_CROSS_CLUSTER_ENABLE, true);
          put(PropertyKey.MASTER_CROSS_CLUSTER_RPC_ADDRESSES, "host:1000");
        }
      }, Configuration.modifiableGlobal());

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.MASTER_TTL_CHECK, HeartbeatContext.MASTER_LOST_FILES_DETECTION);

  // Set ttl interval to 0 so that there is no delay in detecting expired files.
  @ClassRule
  public static TtlIntervalRule sTtlIntervalRule = new TtlIntervalRule(0);

  @Rule
  public TemporaryFolder mUfsPath = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    // the configuration process tracking cluster mount changes
    mCrossClusterState = new CrossClusterState();

    mCrossClusterMounts = new HashMap<>();
    mFileSystems = new HashMap<>();
    for (int i = 0; i < mClusterCount; i++) {
      Configuration.modifiableGlobal().set(PropertyKey.MASTER_RPC_ADDRESSES,
          String.format("other.host%d:%d", i, 1234), Source.RUNTIME);
      String clusterId = "c" + i;
      FileSystemMasterTest ft = new FileSystemMasterTest();
      ft.mTestFolder = mTestFolder;
      ft.before();
      mFileSystems.put(clusterId, ft);
      // setup to publish configuration changes
      InetSocketAddress[] addresses = new InetSocketAddress[] {
          new InetSocketAddress("other.host." + i, 1234)};
      //ft.mFileSystemMaster.getMountTable().mState.mLocalMountState =
       //   new LocalMountState(clusterId, addresses,
        //      mountList -> mCrossClusterState.setMountList(mountList));
    }

    for (Map.Entry<String, FileSystemMasterTest> entry : mFileSystems.entrySet()) {
      // Set up the cross cluster subscriptions
      CrossClusterMount ccMount = new CrossClusterMount(entry.getKey(), entry.getValue()
          .mFileSystemMaster.getInvalidationSyncCache(),
          stream -> {
            InvalidationStream invalidationStream =
                (InvalidationStream) stream;
            mFileSystems.get(invalidationStream.getMountSyncAddress().getMountSync().getClusterId())
                .mFileSystemMaster.subscribeInvalidations(new CrossClusterInvalidationStream(
                    invalidationStream.getMountSyncAddress().getMountSync(), stream));
          }, stream -> {
        // nothing to do since the streams are the same at both the publisher and subscriber,
        // so when cancellation is called, the stream has already been completed
      });
     // entry.getValue().mFileSystemMaster.getMountTable().mState.mCrossClusterMount = ccMount;
      mCrossClusterMounts.put(entry.getKey(), ccMount);

      // setup to subscribe to configuration changes
      mCrossClusterState.setStream(entry.getKey(), new StreamObserver<MountList>() {
        @Override
        public void onNext(MountList value) {
          ccMount.setExternalMountList(value);
        }

        @Override
        public void onError(Throwable t) {
          throw new RuntimeException(t);
        }

        @Override
        public void onCompleted() {
          throw new IllegalStateException("not expected during test");
        }
      });
    }
  }

  private final MountPOptions.Builder mMountOptions =
      MountPOptions.newBuilder().setCrossCluster(true);

  @After
  public void after() throws Exception {
    for (FileSystemMasterTest fs : mFileSystems.values()) {
      fs.stopServices();
      Configuration.reloadProperties();
    }
  }

  void assertFileDoesNotExist(String path, DefaultFileSystemMaster ... fsArray) {
    for (DefaultFileSystemMaster fs : fsArray) {
      assertThrows(FileDoesNotExistException.class,
          () -> fs.getFileInfo(new AlluxioURI(path),
              GetStatusContext.defaults()));
    }
  }

  void assertFileExists(String path, DefaultFileSystemMaster ... fsArray)
      throws Exception {
    for (DefaultFileSystemMaster fs : fsArray) {
      fs.getFileInfo(new AlluxioURI(path), GetStatusContext.defaults());
    }
  }

  void checkUnMounted() throws Exception {
    FileSystemMasterTest c0 = mFileSystems.get("c0");

    // Be sure files are not synced across clusters when no intersecting mounts
    String f1Path = "/f1";
    assertFileDoesNotExist(f1Path, mFileSystems.values().stream().map(fs -> fs.mFileSystemMaster)
        .toArray(DefaultFileSystemMaster[]::new));
    c0.createFileWithSingleBlock(new AlluxioURI(f1Path), createFileContext(false));
    assertFileExists(f1Path, c0.mFileSystemMaster);
    assertFileDoesNotExist(f1Path,
        mFileSystems.values().stream().filter((fs) -> fs != c0).map(fs -> fs.mFileSystemMaster)
            .toArray(DefaultFileSystemMaster[]::new));
  }

  @Test
  public void crossClusterIntersectionBasic() throws Exception {
    checkUnMounted();
    String ufsMountPath = mUfsPath.newFolder("ufs").getAbsolutePath();
    FileSystemMasterTest c0 = mFileSystems.get("c0");
    FileSystemMasterTest c1 = mFileSystems.get("c1");
    FileSystemMasterTest c2 = mFileSystems.get("c2");

    // Create an intersecting mount between c0 and c1
    String mountPath = "/mount";
    c0.mFileSystemMaster.mount(new AlluxioURI(mountPath), new AlluxioURI(ufsMountPath),
        MountContext.create(mMountOptions));
    c1.mFileSystemMaster.mount(new AlluxioURI(mountPath), new AlluxioURI(ufsMountPath),
        MountContext.create(mMountOptions));

    // First read the path so the absent cache is checked
    String f1MountPath = "/mount/f1";
    assertFileDoesNotExist(f1MountPath, c0.mFileSystemMaster, c1.mFileSystemMaster,
        c2.mFileSystemMaster);
    c0.createFileWithSingleBlock(new AlluxioURI(f1MountPath), createFileContext(false));
    assertFileExists(f1MountPath, c0.mFileSystemMaster, c1.mFileSystemMaster);
    assertFileDoesNotExist(f1MountPath, c2.mFileSystemMaster);

    // Create the same mount for c2
    c2.mFileSystemMaster.mount(new AlluxioURI(mountPath), new AlluxioURI(ufsMountPath),
        MountContext.create(mMountOptions));
    // the file should now be synced
    assertFileExists(f1MountPath, c2.mFileSystemMaster);

    // Create a new file on c2 after checking existence on all clusters
    String f2MountPath = "/mount/f2";
    assertFileDoesNotExist(f2MountPath, c0.mFileSystemMaster, c1.mFileSystemMaster,
        c2.mFileSystemMaster);
    c2.createFileWithSingleBlock(new AlluxioURI(f2MountPath), createFileContext(false));
    assertFileExists(f2MountPath, c0.mFileSystemMaster, c1.mFileSystemMaster, c2.mFileSystemMaster);

    // Create a directory on c1
    String dirPath = "/mount/adir";
    assertFileDoesNotExist(dirPath, c0.mFileSystemMaster, c1.mFileSystemMaster,
        c2.mFileSystemMaster);
    c1.mFileSystemMaster.createDirectory(new AlluxioURI(dirPath),
        createDirectoryContext(false));
    assertFileExists(dirPath, c0.mFileSystemMaster, c1.mFileSystemMaster, c2.mFileSystemMaster);

    // Create a nested directory recursively
    dirPath = "/mount";
    for (int i = 0; i < 5; i++) {
      dirPath += "/nested-dir";
      assertFileDoesNotExist(dirPath, c0.mFileSystemMaster, c1.mFileSystemMaster,
          c2.mFileSystemMaster);
    }
    c1.mFileSystemMaster.createDirectory(new AlluxioURI(dirPath),
        createDirectoryContext(true));
    assertFileExists(dirPath, c0.mFileSystemMaster, c1.mFileSystemMaster, c2.mFileSystemMaster);

    // Create a nested file recursively
    String filePath = "/mount";
    for (int i = 0; i < 5; i++) {
      filePath += "/file-dir";
      assertFileDoesNotExist(filePath, c0.mFileSystemMaster, c1.mFileSystemMaster,
          c2.mFileSystemMaster);
    }
    filePath += "/a-file";
    c1.createFileWithSingleBlock(new AlluxioURI(filePath), createFileContext(true));
    assertFileExists(filePath, c0.mFileSystemMaster, c1.mFileSystemMaster, c2.mFileSystemMaster);

    // Remove the file
    c0.mFileSystemMaster.delete(new AlluxioURI(filePath), deleteContext(false));
    assertFileDoesNotExist(filePath, c0.mFileSystemMaster, c1.mFileSystemMaster,
        c2.mFileSystemMaster);

    // Remove the file's nested directories
    filePath = "/mount/file-dir";
    c2.mFileSystemMaster.delete(new AlluxioURI(filePath), deleteContext(true));
    assertFileDoesNotExist(filePath, c0.mFileSystemMaster, c1.mFileSystemMaster,
        c2.mFileSystemMaster);
  }

  @Test
  public void crossClusterNestedIntersection() throws Exception {
    checkUnMounted();
    String ufsMountPath = mUfsPath.newFolder("ufs").getAbsolutePath();
    FileSystemMasterTest c0 = mFileSystems.get("c0");
    FileSystemMasterTest c1 = mFileSystems.get("c1");

    // Create a mount at c0
    String mountPath = "/mount";
    c0.mFileSystemMaster.mount(new AlluxioURI(mountPath), new AlluxioURI(ufsMountPath),
        MountContext.create(mMountOptions));
    // create a nested path
    String filePath = mountPath + "/nested/a-file";
    c0.createFileWithSingleBlock(new AlluxioURI(filePath), createFileContext(true));
    assertFileExists(filePath, c0.mFileSystemMaster);
    assertFileDoesNotExist(filePath, c1.mFileSystemMaster);

    // mount the nested directory on c1
    String nestedFilePath = "/mount/a-file";
    assertFileDoesNotExist(nestedFilePath, c1.mFileSystemMaster);
    String ufsNestedMountPath = ufsMountPath + "/nested";
    c1.mFileSystemMaster.mount(new AlluxioURI(mountPath), new AlluxioURI(ufsNestedMountPath),
        MountContext.create(mMountOptions));
    assertFileExists(nestedFilePath, c1.mFileSystemMaster);

    // Create a file on c1 in the mounted directory, it should exist on c0 in the nested path
    nestedFilePath = "/mount/b-file";
    filePath = "/mount/nested/b-file";
    c1.createFileWithSingleBlock(new AlluxioURI(nestedFilePath), createFileContext(false));
    assertFileExists(nestedFilePath, c1.mFileSystemMaster);
    assertFileExists(filePath, c0.mFileSystemMaster);

    // Create a file on c0 in the nested directory, it should exist on c1 in the nested path
    nestedFilePath = "/mount/c-file";
    filePath = "/mount/nested/c-file";
    c0.createFileWithSingleBlock(new AlluxioURI(filePath), createFileContext(false));
    assertFileExists(nestedFilePath, c1.mFileSystemMaster);
    assertFileExists(filePath, c0.mFileSystemMaster);
  }

  @Test
  public void crossClusterIntersectionRemoval() throws Exception {
    checkUnMounted();
    String ufsMountPath = mUfsPath.newFolder("ufs").getAbsolutePath();
    FileSystemMasterTest c0 = mFileSystems.get("c0");
    FileSystemMasterTest c1 = mFileSystems.get("c1");
    FileSystemMasterTest c2 = mFileSystems.get("c2");

    // Create a mount at c0, c1, c2
    String mountPath = "/mount";
    c0.mFileSystemMaster.mount(new AlluxioURI(mountPath), new AlluxioURI(ufsMountPath),
        MountContext.create(mMountOptions));
    c1.mFileSystemMaster.mount(new AlluxioURI(mountPath), new AlluxioURI(ufsMountPath),
        MountContext.create(mMountOptions));
    c2.mFileSystemMaster.mount(new AlluxioURI(mountPath), new AlluxioURI(ufsMountPath),
        MountContext.create(mMountOptions));

    // create a file
    String filePath = mountPath + "/a-file";
    assertFileDoesNotExist(filePath, c0.mFileSystemMaster, c1.mFileSystemMaster,
        c2.mFileSystemMaster);
    c0.createFileWithSingleBlock(new AlluxioURI(filePath), createFileContext(false));
    assertFileExists(filePath, c0.mFileSystemMaster, c1.mFileSystemMaster, c2.mFileSystemMaster);

    // remove a mount at c0
    c0.mFileSystemMaster.unmount(new AlluxioURI(mountPath));
    // be sure files are not synced with c0
    String filePath2 = mountPath + "/b-file";
    assertFileDoesNotExist(filePath2, c0.mFileSystemMaster, c1.mFileSystemMaster,
        c2.mFileSystemMaster);
    c1.createFileWithSingleBlock(new AlluxioURI(filePath2), createFileContext(true));
    assertFileExists(filePath2, c1.mFileSystemMaster, c2.mFileSystemMaster);
    assertFileDoesNotExist(filePath2, c0.mFileSystemMaster);

    // remount at c0, be sure all files exist
    c0.mFileSystemMaster.mount(new AlluxioURI(mountPath), new AlluxioURI(ufsMountPath),
        MountContext.create(mMountOptions));
    assertFileExists(filePath, c0.mFileSystemMaster, c1.mFileSystemMaster, c2.mFileSystemMaster);
    assertFileExists(filePath2, c0.mFileSystemMaster, c1.mFileSystemMaster, c2.mFileSystemMaster);
  }
}
