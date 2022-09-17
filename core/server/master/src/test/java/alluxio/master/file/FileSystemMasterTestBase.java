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

package alluxio.master.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.StorageList;
import alluxio.grpc.WritePType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.TtlIntervalRule;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalTestUtils;
import alluxio.master.journal.JournalType;
import alluxio.master.metastore.InodeStore;
import alluxio.master.metastore.ReadOnlyInodeStore;
import alluxio.master.metastore.caching.CachingInodeStore;
import alluxio.master.metastore.heap.HeapBlockMetaStore;
import alluxio.master.metastore.heap.HeapInodeStore;
import alluxio.master.metastore.rocks.RocksInodeStore;
import alluxio.master.metrics.MetricsMaster;
import alluxio.master.metrics.MetricsMasterFactory;
import alluxio.metrics.Metric;
import alluxio.metrics.MetricsSystem;
import alluxio.security.GroupMappingServiceTestUtils;
import alluxio.security.user.TestUserState;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileSystemMasterTestBase {
  static final AlluxioURI NESTED_BASE_URI = new AlluxioURI("/nested");
  static final AlluxioURI NESTED_URI = new AlluxioURI("/nested/test");
  static final AlluxioURI NESTED_FILE_URI = new AlluxioURI("/nested/test/file");
  static final AlluxioURI NESTED_FILE2_URI = new AlluxioURI("/nested/test/file2");
  static final AlluxioURI NESTED_DIR_URI = new AlluxioURI("/nested/test/dir");
  static final AlluxioURI NESTED_DIR_FILE_URI = new AlluxioURI("/nested/test/dir/file");
  static final AlluxioURI NESTED_TEST_FILE_URI = new AlluxioURI("/nested/test_file");
  static final AlluxioURI ROOT_URI = new AlluxioURI("/");
  static final AlluxioURI ROOT_FILE_URI = new AlluxioURI("/file");
  static final AlluxioURI ROOT_AFILE_URI = new AlluxioURI("/afile");
  static final AlluxioURI TEST_URI = new AlluxioURI("/test");
  static final String TEST_USER = "test";
  static final GetStatusContext GET_STATUS_CONTEXT = GetStatusContext.defaults();

  // Constants for tests on persisted directories.
  static final String DIR_PREFIX = "dir";
  static final String DIR_TOP_LEVEL = "top";
  static final String FILE_PREFIX = "file";
  static final String MOUNT_PARENT_URI = "/mnt";
  static final String MOUNT_URI = "/mnt/local";
  static final int DIR_WIDTH = 2;

  CreateFileContext mNestedFileContext;
  MasterRegistry mRegistry;
  JournalSystem mJournalSystem;
  BlockMaster mBlockMaster;
  ExecutorService mExecutorService;
  public DefaultFileSystemMaster mFileSystemMaster;
  InodeTree mInodeTree;
  ReadOnlyInodeStore mInodeStore;
  MetricsMaster mMetricsMaster;
  List<Metric> mMetrics;
  long mWorkerId1;
  long mWorkerId2;
  String mJournalFolder;
  String mUnderFS;
  InodeStore.Factory mInodeStoreFactory = (ignored) -> new HeapInodeStore();
  public Clock mClock;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Rule
  public TemporaryFolder mUfsPath = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

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
        }
      }, Configuration.modifiableGlobal());

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.MASTER_TTL_CHECK, HeartbeatContext.MASTER_LOST_FILES_DETECTION);

  // Set ttl interval to 0 so that there is no delay in detecting expired files.
  @ClassRule
  public static TtlIntervalRule sTtlIntervalRule = new TtlIntervalRule(0);

  @Parameterized.Parameters
  public static Iterable<InodeStore.Factory> parameters() throws Exception {
    String dir =
        AlluxioTestDirectory.createTemporaryDirectory("inode-store-test").getAbsolutePath();
    return Arrays.asList(
        lockManager -> new HeapInodeStore(),
        lockManager -> new RocksInodeStore(dir),
        lockManager -> new CachingInodeStore(new RocksInodeStore(dir), lockManager));
  }

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    if (mClock == null) {
      mClock = Mockito.mock(Clock.class);
      Mockito.doAnswer(invocation -> Clock.systemUTC().millis()).when(mClock).millis();
    }
    GroupMappingServiceTestUtils.resetCache();
    MetricsSystem.clearAllMetrics();
    // This makes sure that the mount point of the UFS corresponding to the Alluxio root ("/")
    // doesn't exist by default (helps loadRootTest).
    mUnderFS = Configuration.getString(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    mNestedFileContext = CreateFileContext.mergeFrom(
        CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB)
            .setWriteType(WritePType.MUST_CACHE)
            .setRecursive(true));
    mJournalFolder = mTestFolder.newFolder().getAbsolutePath();
    startServices();
  }

  /**
   * Resets global state after each test run.
   */
  @After
  public void after() throws Exception {
    stopServices();
    Configuration.reloadProperties();
  }

  // Helper method to check if expected entries were deleted.
  void checkPersistedDirectoriesDeleted(int levels, AlluxioURI ufsMount,
                                        List<AlluxioURI> except) throws Exception {
    checkPersistedDirectoryDeletedLevel(levels, new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL),
        ufsMount.join(DIR_TOP_LEVEL), except);
  }

  void checkPersistedDirectoryDeletedLevel(
      int levels, AlluxioURI alluxioURI, AlluxioURI ufsURI, List<AlluxioURI> except)
      throws Exception {
    if (levels <= 0) {
      return;
    }
    if (except.contains(alluxioURI)) {
      assertTrue(Files.exists(Paths.get(ufsURI.getPath())));
      assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(alluxioURI));
    } else {
      assertFalse(Files.exists(Paths.get(ufsURI.getPath())));
      assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(alluxioURI));
    }
    for (int i = 0; i < DIR_WIDTH; ++i) {
      // Files can always be deleted.
      assertFalse(Files.exists(Paths.get(ufsURI.join(FILE_PREFIX + i).getPath())));
      assertEquals(IdUtils.INVALID_FILE_ID,
          mFileSystemMaster.getFileId(alluxioURI.join(FILE_PREFIX + i)));

      checkPersistedDirectoryDeletedLevel(levels - 1, alluxioURI.join(DIR_PREFIX + i),
          ufsURI.join(DIR_PREFIX + i), except);
    }
  }

  // Helper method to construct a directory tree in the UFS.
  AlluxioURI createPersistedDirectories(int levels) throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    // Create top-level directory in UFS.
    Files.createDirectory(Paths.get(ufsMount.join(DIR_TOP_LEVEL).getPath()));
    createPersistedDirectoryLevel(levels, ufsMount.join(DIR_TOP_LEVEL));
    return ufsMount;
  }

  void createPersistedDirectoryLevel(int levels, AlluxioURI ufsTop) throws Exception {
    if (levels <= 0) {
      return;
    }
    // Create files and nested directories in UFS.
    for (int i = 0; i < DIR_WIDTH; ++i) {
      Files.createFile(Paths.get(ufsTop.join(FILE_PREFIX + i).getPath()));
      Files.createDirectories(Paths.get(ufsTop.join(DIR_PREFIX + i).getPath()));
      createPersistedDirectoryLevel(levels - 1, ufsTop.join(DIR_PREFIX + i));
    }
  }

  // Helper method to load a tree from the UFS.
  void loadPersistedDirectories(int levels) throws Exception {
    // load persisted ufs entries to alluxio
    mFileSystemMaster.listStatus(new AlluxioURI(MOUNT_URI), ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS)));
    loadPersistedDirectoryLevel(levels, new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL));
  }

  void loadPersistedDirectoryLevel(int levels, AlluxioURI alluxioTop) throws Exception {
    if (levels <= 0) {
      return;
    }

    mFileSystemMaster.listStatus(alluxioTop, ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS)));

    for (int i = 0; i < DIR_WIDTH; ++i) {
      loadPersistedDirectoryLevel(levels - 1, alluxioTop.join(DIR_PREFIX + i));
    }
  }

  // Helper method to mount UFS tree.
  void mountPersistedDirectories(AlluxioURI ufsMount) throws Exception {
    mFileSystemMaster.createDirectory(new AlluxioURI(MOUNT_PARENT_URI),
        CreateDirectoryContext.defaults());
    mFileSystemMaster.mount(new AlluxioURI(MOUNT_URI), ufsMount,
        MountContext.defaults());
  }

  /**
   * Creates a temporary UFS folder. The ufsPath must be a relative path since it's a temporary dir
   * created by mTestFolder.
   *
   * @param ufsPath the UFS path of the temp dir needed to created
   * @return the AlluxioURI of the temp dir
   */
  AlluxioURI createTempUfsDir(String ufsPath) throws IOException {
    String path = mTestFolder.newFolder(ufsPath.split("/")).getPath();
    return new AlluxioURI("file", null, path);
  }

  /**
   * Creates a file in a temporary UFS folder.
   *
   * @param ufsPath the UFS path of the temp file to create
   * @return the AlluxioURI of the temp file
   */
  AlluxioURI createTempUfsFile(String ufsPath) throws IOException {
    String path = mTestFolder.newFile(ufsPath).getPath();
    return new AlluxioURI(path);
  }

  long createFileWithSingleBlock(AlluxioURI uri) throws Exception {
    return createFileWithSingleBlock(uri, mNestedFileContext);
  }

  public long createFileWithSingleBlock(AlluxioURI uri, CreateFileContext createFileContext)
      throws Exception {
    FileInfo info = mFileSystemMaster.createFile(uri, createFileContext);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(uri);
    // write the file
    WritePType writeType = createFileContext.getOptions().getWriteType();
    if (info.getUfsPath() != null && (
        writeType == WritePType.CACHE_THROUGH || writeType == WritePType.THROUGH
            || writeType == WritePType.ASYNC_THROUGH)) {
      Files.createFile(Paths.get(info.getUfsPath()));
    }
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB,
        Constants.MEDIUM_MEM, Constants.MEDIUM_MEM, blockId, Constants.KB);
    CompleteFileContext context =
        CompleteFileContext.mergeFrom(CompleteFilePOptions.newBuilder().setUfsLength(Constants.KB));
    mFileSystemMaster.completeFile(uri, context);
    return blockId;
  }

  long countPaths() throws Exception {
    return mInodeTree.getInodeCount();
  }

  /**
   * Asserts that the map is null or empty.
   * @param m the map to check
   */
  static void assertNullOrEmpty(Map m) {
    assertTrue(m == null || m.isEmpty());
  }

  void startServices() throws Exception {
    mRegistry = new MasterRegistry();
    mJournalSystem = JournalTestUtils.createJournalSystem(mJournalFolder);
    CoreMasterContext masterContext = MasterTestUtils.testMasterContext(mJournalSystem,
        new TestUserState(TEST_USER, Configuration.global()), HeapBlockMetaStore::new,
        mInodeStoreFactory);
    mMetricsMaster = new MetricsMasterFactory().create(mRegistry, masterContext);
    mRegistry.add(MetricsMaster.class, mMetricsMaster);
    mMetrics = Lists.newArrayList();
    mBlockMaster = new BlockMasterFactory().create(mRegistry, masterContext);
    mExecutorService = Executors
        .newFixedThreadPool(4, ThreadFactoryUtils.build("DefaultFileSystemMasterTest-%d", true));
    mFileSystemMaster = new DefaultFileSystemMaster(mBlockMaster, masterContext,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService), mClock);
    mInodeStore = mFileSystemMaster.getInodeStore();
    mInodeTree = mFileSystemMaster.getInodeTree();
    mRegistry.add(FileSystemMaster.class, mFileSystemMaster);
    mJournalSystem.start();
    mJournalSystem.gainPrimacy();
    mRegistry.start(true);

    // set up workers
    mWorkerId1 = mBlockMaster.getWorkerId(
        new WorkerNetAddress().setHost("localhost").setRpcPort(80).setDataPort(81).setWebPort(82));
    mBlockMaster.workerRegister(mWorkerId1,
        Arrays.asList(Constants.MEDIUM_MEM, Constants.MEDIUM_SSD),
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.MB,
            Constants.MEDIUM_SSD, (long) Constants.MB),
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB,
            Constants.MEDIUM_SSD, (long) Constants.KB), ImmutableMap.of(),
        new HashMap<String, StorageList>(), RegisterWorkerPOptions.getDefaultInstance());
    mWorkerId2 = mBlockMaster.getWorkerId(
        new WorkerNetAddress().setHost("remote").setRpcPort(80).setDataPort(81).setWebPort(82));
    mBlockMaster.workerRegister(mWorkerId2,
        Arrays.asList(Constants.MEDIUM_MEM, Constants.MEDIUM_SSD),
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.MB,
            Constants.MEDIUM_SSD, (long) Constants.MB),
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB,
            Constants.MEDIUM_SSD, (long) Constants.KB),
        ImmutableMap.of(), new HashMap<String, StorageList>(),
        RegisterWorkerPOptions.getDefaultInstance());
  }

  public void stopServices() throws Exception {
    mRegistry.stop();
    mJournalSystem.stop();
    mFileSystemMaster.close();
    mFileSystemMaster.stop();
  }
}
