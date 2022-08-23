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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.ConfigurationRule;
import alluxio.client.WriteType;
import alluxio.client.cross.cluster.CrossClusterClientContextBuilder;
import alluxio.client.cross.cluster.RetryHandlingCrossClusterMasterClient;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.conf.Source;
import alluxio.exception.FileDoesNotExistException;
import alluxio.file.options.DescendantType;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.MasterClientContext;
import alluxio.master.cross.cluster.CrossClusterState;
import alluxio.master.file.DefaultFileSystemMaster;
import alluxio.master.file.FileSystemMasterTest;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.contexts.RenameContext;
import alluxio.master.file.contexts.SetAclContext;
import alluxio.master.file.contexts.SetAttributeContext;
import alluxio.master.file.meta.TtlIntervalRule;
import alluxio.master.journal.JournalType;
import alluxio.proto.journal.CrossCluster.MountList;
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.Mode;
import alluxio.wire.FileInfo;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystemCrossCluster.Factory.class, CrossClusterMount.class,
    CrossClusterClientContextBuilder.class, RetryHandlingCrossClusterMasterClient.class,
    CrossClusterMasterState.class})
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
    // mock the connection and rpc client contexts so that they do nothing
    PowerMockito.mockStatic(CrossClusterClientContextBuilder.class);
    CrossClusterClientContextBuilder mockBuilder = Mockito.mock(
        CrossClusterClientContextBuilder.class);
    Mockito.when(mockBuilder.build()).thenReturn(Mockito.mock(MasterClientContext.class));
    PowerMockito.whenNew(CrossClusterClientContextBuilder.class).withAnyArguments()
        .thenReturn(mockBuilder);
    PowerMockito.whenNew(RetryHandlingCrossClusterMasterClient.class).withAnyArguments()
        .thenReturn(Mockito.mock(RetryHandlingCrossClusterMasterClient.class));
    PowerMockito.mockStatic(FileSystemCrossCluster.Factory.class);
    Mockito.when(FileSystemCrossCluster.Factory.create(any(FileSystemContext.class)))
        .thenReturn(Mockito.mock(FileSystemCrossCluster.class));

    // mock the client runner to update the configuration state when
    // modifications happen
    CrossClusterMountClientRunner clientRunner = Mockito.mock(
        CrossClusterMountClientRunner.class);
    PowerMockito.whenNew(CrossClusterMountClientRunner.class)
        .withAnyArguments().thenReturn(clientRunner);
    PowerMockito.whenNew(CrossClusterMountSubscriber.class)
        .withAnyArguments().thenReturn(Mockito.mock(CrossClusterMountSubscriber.class));

    // mock the client connections so that subscribing to an invalidation stream
    // calls subscribe invalidations on the target master directly
    CrossClusterConnections mockConnections = Mockito.mock(CrossClusterConnections.class);
    PowerMockito.whenNew(CrossClusterConnections.class).withAnyArguments()
        .thenReturn(mockConnections);

    // the configuration process tracking cluster mount changes
    mCrossClusterState = new CrossClusterState();
    Mockito.doAnswer(mock -> {
      MountList mount = mock.getArgument(0);
      mCrossClusterState.setMountList(mount);
      return null;
    }).when(clientRunner).onLocalMountChange(any());

    mCrossClusterMounts = new HashMap<>();
    mFileSystems = new HashMap<>();
    for (int i = 0; i < mClusterCount; i++) {
      Configuration.modifiableGlobal().set(PropertyKey.MASTER_RPC_ADDRESSES,
          String.format("localhost:%d", i + 1234), Source.RUNTIME);
      String clusterId = "c" + i;
      Configuration.modifiableGlobal().set(PropertyKey.MASTER_CROSS_CLUSTER_ID,
          clusterId, Source.RUNTIME);
      FileSystemMasterTest ft = new FileSystemMasterTest();
      ft.mTestFolder = mTestFolder;
      ft.before();
      mFileSystems.put(clusterId, ft);
    }

    for (Map.Entry<String, FileSystemMasterTest> entry : mFileSystems.entrySet()) {
      // Set up the cross cluster subscriptions
      mCrossClusterMounts.put(entry.getKey(), entry.getValue().mFileSystemMaster
          .getCrossClusterState().getCrossClusterMount().get());

      // setup to subscribe to configuration changes
      mCrossClusterState.setStream(entry.getKey(), new StreamObserver<MountList>() {
        @Override
        public void onNext(MountList value) {
          try {
            mCrossClusterMounts.get(entry.getKey()).setExternalMountList(value);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
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
    Mockito.doAnswer(mock -> {
      InvalidationStream stream = mock.getArgument(1);
      String destinationCluster = stream.getMountSyncAddress().getMountSync().getClusterId();
      String subscriberCluster = mock.getArgument(0);
      mFileSystems.get(destinationCluster)
          .mFileSystemMaster.subscribeInvalidations(new CrossClusterInvalidationStream(
              new MountSync(subscriberCluster,
                  stream.getMountSyncAddress().getMountSync().getUfsPath()), stream));
      return null;
    }).when(mockConnections).addStream(any(), any());
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

  @Test
  public void crossClusterIntersectionRecursive() throws Exception {
    String ufsPath = Configuration.global().getString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    FileSystemMasterTest c0 = mFileSystems.get("c0");

    TrackingCrossClusterPublisher publisher = new TrackingCrossClusterPublisher();
    MountSync c1MountSync = new MountSync("c1", ufsPath);
    c0.mFileSystemMaster.subscribeInvalidations(new CrossClusterInvalidationStream(
        c1MountSync, publisher.getStream(c1MountSync)));
    ArrayList<String> c1 = new ArrayList<>();
    c1.add(ufsPath);

    ArrayList<String> dirs = new ArrayList<>();
    ArrayList<String> dirsUfs = new ArrayList<>();
    StringBuilder dir = new StringBuilder();
    StringBuilder dirUfs = new StringBuilder(ufsPath);
    for (int i = 0; i < 5; i++) {
      dir.append("/dir");
      dirUfs.append("/dir");
      dirs.add(dir.toString());
      dirsUfs.add(dirUfs.toString());
    }
    c1.addAll(dirsUfs);

    // Create directory recursively, then delete them recursively
    c0.mFileSystemMaster.createDirectory(new AlluxioURI(dir.toString()),
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder()
            .setRecursive(true).setWriteType(WritePType.CACHE_THROUGH)));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));

    for (int i = dirsUfs.size(); i > 0; i--) {
      c1.add(dirsUfs.get(i - 1));
    }
    c0.mFileSystemMaster.delete(new AlluxioURI(dirs.get(0)),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));

    // Create file recursively, then delete them recursively
    c1.addAll(dirsUfs);
    c0.mFileSystemMaster.createFile(new AlluxioURI(dir.toString()),
        CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
            .setRecursive(true).setWriteType(WritePType.CACHE_THROUGH)));
    c0.mFileSystemMaster.completeFile(new AlluxioURI(dir.toString()),
        CompleteFileContext.defaults());
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));

    for (int i = dirsUfs.size(); i > 0; i--) {
      c1.add(dirsUfs.get(i - 1));
    }
    c0.mFileSystemMaster.delete(new AlluxioURI(dirs.get(0)),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));

    // create dir recursive, then set ACL
    c1.addAll(dirsUfs);
    c0.mFileSystemMaster.createDirectory(new AlluxioURI(dir.toString()),
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder()
            .setRecursive(true).setWriteType(WritePType.CACHE_THROUGH)));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));

    c1.addAll(dirsUfs);
    Set<String> newEntries = Sets.newHashSet("default:user::rwx",
        "default:group::rwx", "default:other::r-x");
    c0.mFileSystemMaster.setAcl(new AlluxioURI(dirs.get(0)), SetAclAction.REPLACE,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()),
        SetAclContext.mergeFrom(SetAclPOptions.newBuilder().setRecursive(true)));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
  }

  @Test
  public void crossClusterIntersection() throws Exception {
    String ufsPath = Configuration.global().getString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    FileSystemMasterTest c0 = mFileSystems.get("c0");

    MountSync c1MountSync = new MountSync("c1", ufsPath);
    TrackingCrossClusterPublisher publisher =
        new TrackingCrossClusterPublisher();
    c0.mFileSystemMaster.subscribeInvalidations(new CrossClusterInvalidationStream(
        c1MountSync, publisher.getStream(c1MountSync)));
    String c2SubscribePath = ufsPath + "/test";
    MountSync c2MountSync = new MountSync("c2", c2SubscribePath);
    c0.mFileSystemMaster.subscribeInvalidations(new CrossClusterInvalidationStream(
        c2MountSync, publisher.getStream(c2MountSync)));
    ArrayList<String> c1 = new ArrayList<>();
    c1.add(ufsPath);
    ArrayList<String> c2 = new ArrayList<>();
    c2.add(c2SubscribePath);

    String testFile = "/testFile";
    String testFileUfs = ufsPath + testFile;
    c1.add(testFileUfs);
    c0.createFileWithSingleBlock(new AlluxioURI(testFile),
        CreateFileContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c2.toArray(),
        publisher.getPublishedPaths(c2MountSync).toArray(String[]::new));

    String testDir = "/test";
    String testDirUfs = ufsPath + testDir;
    c1.add(testDirUfs);
    c2.add(testDirUfs);
    c0.mFileSystemMaster.createDirectory(new AlluxioURI(testDir),
        CreateDirectoryContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c2.toArray(),
        publisher.getPublishedPaths(c2MountSync).toArray(String[]::new));

    String testFile2 = "/test/testFile2";
    String testFile2Ufs = ufsPath + testFile2;
    c1.add(testFile2Ufs);
    c2.add(testFile2Ufs);
    c0.createFileWithSingleBlock(new AlluxioURI(testFile2),
        CreateFileContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c2.toArray(),
        publisher.getPublishedPaths(c2MountSync).toArray(String[]::new));

    c1.add(testFile2Ufs);
    c2.add(testFile2Ufs);
    c0.mFileSystemMaster.delete(new AlluxioURI(testFile2), DeleteContext.defaults());
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c2.toArray(),
        publisher.getPublishedPaths(c2MountSync).toArray(String[]::new));

    String testDirNested = testDir + "/test";
    String testDirNestedUfs = ufsPath + testDirNested;
    c1.add(testDirNestedUfs);
    c2.add(testDirNestedUfs);
    c0.mFileSystemMaster.createDirectory(new AlluxioURI(testDirNested),
        CreateDirectoryContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c2.toArray(),
        publisher.getPublishedPaths(c2MountSync).toArray(String[]::new));

    String testFileNested = testDirNested + "testFile";
    String testFileNestedUfs = ufsPath + testFileNested;
    c1.add(testFileNestedUfs);
    c2.add(testFileNestedUfs);
    c0.createFileWithSingleBlock(new AlluxioURI(testFileNested),
        CreateFileContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c2.toArray(),
        publisher.getPublishedPaths(c2MountSync).toArray(String[]::new));

    // create a directory that is not persisted, that will be persisted once the rename happens
    String testDirRename = "/testRename";
    String testDirRenameUfs = ufsPath + testDirRename;
    c0.mFileSystemMaster.createDirectory(new AlluxioURI(testDirRename),
        CreateDirectoryContext.defaults().setWriteType(WriteType.MUST_CACHE));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c2.toArray(),
        publisher.getPublishedPaths(c2MountSync).toArray(String[]::new));

    String testDirRenameNested = testDirRename + "/nestedRename";
    String testDirRenameNestedUfs = ufsPath + testDirRenameNested;
    c1.add(testDirRenameUfs);
    c1.add(testDirNestedUfs);
    c1.add(testDirRenameNestedUfs);
    c2.add(testDirNestedUfs);
    c0.mFileSystemMaster.rename(new AlluxioURI(testDirNested), new AlluxioURI(testDirRenameNested),
        RenameContext.defaults());
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c2.toArray(),
        publisher.getPublishedPaths(c2MountSync).toArray(String[]::new));

    c1.add(testDirRenameNestedUfs);
    c0.mFileSystemMaster.setAttribute(new AlluxioURI(testDirRenameNested), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c2.toArray(),
        publisher.getPublishedPaths(c2MountSync).toArray(String[]::new));

    c1.add(testDirUfs);
    c2.add(testDirUfs);
    Set<String> newEntries = Sets.newHashSet("default:user::rwx",
        "default:group::rwx", "default:other::r-x");
    c0.mFileSystemMaster.setAcl(new AlluxioURI(testDir), SetAclAction.REPLACE,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()),
        SetAclContext.defaults());
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c2.toArray(),
        publisher.getPublishedPaths(c2MountSync).toArray(String[]::new));

    // create a directory that is not persisted, that will be persisted once a persisted
    // directory is created
    String testDirOther = testDir + "/testOther";
    String testDirOtherUfs = ufsPath + testDirOther;
    c0.mFileSystemMaster.createDirectory(new AlluxioURI(testDirOther),
        CreateDirectoryContext.defaults().setWriteType(WriteType.MUST_CACHE));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c2.toArray(),
        publisher.getPublishedPaths(c2MountSync).toArray(String[]::new));

    String testDirOtherNested = testDirOther + "/testOtherNested";
    String testDirOtherNestedUfs = ufsPath + testDirOtherNested;
    c1.add(testDirOtherUfs);
    c1.add(testDirOtherNestedUfs);
    c2.add(testDirOtherUfs);
    c2.add(testDirOtherNestedUfs);
    c0.mFileSystemMaster.createDirectory(new AlluxioURI(testDirOtherNested),
        CreateDirectoryContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c2.toArray(),
        publisher.getPublishedPaths(c2MountSync).toArray(String[]::new));
  }

  @Test
  public void crossClusterIntersectionMultiMount() throws Exception {
    String ufsPath = Configuration.global().getString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    FileSystemMasterTest c0 = mFileSystems.get("c0");

    String c1SubscribePath = ufsPath + "/test";
    String c1SubscribePath2 = ufsPath + "/other";
    MountSync c1MountSync = new MountSync("c1", c1SubscribePath);
    MountSync c1MountSync2 = new MountSync("c1", c1SubscribePath2);
    TrackingCrossClusterPublisher publisher =
        new TrackingCrossClusterPublisher();
    c0.mFileSystemMaster.subscribeInvalidations(new CrossClusterInvalidationStream(
        c1MountSync, publisher.getStream(c1MountSync)));
    c0.mFileSystemMaster.subscribeInvalidations(new CrossClusterInvalidationStream(
        c1MountSync2, publisher.getStream(c1MountSync2)));
    ArrayList<String> c1 = new ArrayList<>();
    c1.add(c1SubscribePath);
    ArrayList<String> c1Path2 = new ArrayList<>();
    c1Path2.add(c1SubscribePath2);

    String testFile = "/testFile";
    c0.createFileWithSingleBlock(new AlluxioURI(testFile),
        CreateFileContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c1Path2.toArray(),
        publisher.getPublishedPaths(c1MountSync2).toArray(String[]::new));

    String testDir = "/test";
    String testDirUfs = ufsPath + testDir;
    c1.add(testDirUfs);
    c0.mFileSystemMaster.createDirectory(new AlluxioURI(testDir),
        CreateDirectoryContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c1Path2.toArray(),
        publisher.getPublishedPaths(c1MountSync2).toArray(String[]::new));

    testDir = "/other";
    testDirUfs = ufsPath + testDir;
    c1Path2.add(testDirUfs);
    c0.mFileSystemMaster.createDirectory(new AlluxioURI(testDir),
        CreateDirectoryContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c1Path2.toArray(),
        publisher.getPublishedPaths(c1MountSync2).toArray(String[]::new));

    testFile = "/other/file";
    String testFileUfs = ufsPath + testFile;
    c1Path2.add(testFileUfs);
    c0.createFileWithSingleBlock(new AlluxioURI(testFile),
        CreateFileContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    assertArrayEquals(c1.toArray(),
        publisher.getPublishedPaths(c1MountSync).toArray(String[]::new));
    assertArrayEquals(c1Path2.toArray(),
        publisher.getPublishedPaths(c1MountSync2).toArray(String[]::new));
  }

  Set<String> listNames(FileSystemMasterTest cluster, AlluxioURI path, boolean recursive)
      throws Exception {
    ListStatusContext listContext = ListStatusContext.mergeFrom(
        ListStatusPOptions.newBuilder().setRecursive(recursive));
    return cluster.mFileSystemMaster.listStatus(path, listContext).stream().map(FileInfo::getName)
        .collect(Collectors.toSet());
  }

  @Test
  public void CrossClusterChildSyncRecursiveTest() throws Exception {
    checkUnMounted();
    String ufsMountPath = mUfsPath.newFolder("ufs").getAbsolutePath();
    FileSystemMasterTest c0 = mFileSystems.get("c0");
    FileSystemMasterTest c1 = mFileSystems.get("c1");

    // Create an intersecting mount between c0 and c1
    String mountPath = "/mount";
    c0.mFileSystemMaster.mount(new AlluxioURI(mountPath), new AlluxioURI(ufsMountPath),
        MountContext.create(mMountOptions));
    c1.mFileSystemMaster.mount(new AlluxioURI(mountPath), new AlluxioURI(ufsMountPath),
        MountContext.create(mMountOptions));

    // create a directory on c0
    String dirPath = mountPath + "/dir";
    c0.mFileSystemMaster.createDirectory(new AlluxioURI(dirPath),
        CreateDirectoryContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    // sync the directory on c1
    assertTrue(listNames(c1, new AlluxioURI(dirPath), true).isEmpty());

    // create some nested files
    c0.createFileWithSingleBlock(new AlluxioURI(dirPath).join("f1"),
        createFileContext(false));
    c0.createFileWithSingleBlock(new AlluxioURI(dirPath).join("f2"),
        createFileContext(false));
    c0.createFileWithSingleBlock(new AlluxioURI(dirPath).join("f3"),
        createFileContext(false));
    // read the files on c1
    assertEquals(ImmutableSet.of("f1", "f2", "f3"),
        listNames(c1, new AlluxioURI(dirPath), false));

    // update two files on c0
    c0.mFileSystemMaster.setAttribute(new AlluxioURI(dirPath).join("f1"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    c0.mFileSystemMaster.setAttribute(new AlluxioURI(dirPath).join("f2"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));

    // sync one of the files on c1
    assertEquals((short) 0777, c1.mFileSystemMaster.getFileInfo(new AlluxioURI(dirPath).join("f1"),
        GetStatusContext.defaults()).getMode());
    // the directory should need a sync
    assertTrue(c1.mFileSystemMaster.getMountTable().getSyncPathCacheByPath(new AlluxioURI(dirPath))
        .shouldSyncPath(new AlluxioURI(dirPath), 0, DescendantType.ONE).isShouldSync());
    // sync the directory
    assertEquals(ImmutableSet.of("f1", "f2", "f3"),
        listNames(c1, new AlluxioURI(dirPath), false));
    // the directory should no longer need a sync
    assertFalse(c1.mFileSystemMaster.getMountTable().getSyncPathCacheByPath(new AlluxioURI(dirPath))
        .shouldSyncPath(new AlluxioURI(dirPath), 0, DescendantType.ONE).isShouldSync());
    // the other file should have the updated information
    assertEquals((short) 0777, c1.mFileSystemMaster.getFileInfo(new AlluxioURI(dirPath).join("f2"),
        GetStatusContext.defaults()).getMode());
  }
}
