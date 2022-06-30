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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.AuthenticatedClientUserResource;
import alluxio.AuthenticatedUserRule;
import alluxio.ConfigurationRule;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.conf.PropertyKey;
import alluxio.conf.Configuration;
import alluxio.exception.AccessControlException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.UnexpectedAlluxioException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.grpc.Command;
import alluxio.grpc.CommandType;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.ListStatusPartialPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.MountPOptions;
import alluxio.grpc.RegisterWorkerPOptions;
import alluxio.grpc.SetAclAction;
import alluxio.grpc.SetAclPOptions;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.StorageList;
import alluxio.grpc.WritePType;
import alluxio.grpc.XAttrPropagationStrategy;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.CoreMasterContext;
import alluxio.master.MasterRegistry;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.block.BlockMasterFactory;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.FreeContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.contexts.RenameContext;
import alluxio.master.file.contexts.ScheduleAsyncPersistenceContext;
import alluxio.master.file.contexts.SetAclContext;
import alluxio.master.file.contexts.SetAttributeContext;
import alluxio.master.file.contexts.WorkerHeartbeatContext;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.meta.TtlIntervalRule;
import alluxio.master.journal.JournalSystem;
import alluxio.master.journal.JournalTestUtils;
import alluxio.master.metastore.InodeStore;
import alluxio.master.journal.JournalType;
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
import alluxio.security.authorization.AclEntry;
import alluxio.security.authorization.Mode;
import alluxio.security.user.TestUserState;
import alluxio.util.FileSystemOptions;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.FileUtils;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.FileSystemCommand;
import alluxio.wire.UfsInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link FileSystemMaster}.
 */
@RunWith(Parameterized.class)
public final class FileSystemMasterTest {
  private static final Logger LOG = LoggerFactory.getLogger(FileSystemMasterTest.class);

  private static final AlluxioURI NESTED_BASE_URI = new AlluxioURI("/nested");
  private static final AlluxioURI NESTED_URI = new AlluxioURI("/nested/test");
  private static final AlluxioURI NESTED_FILE_URI = new AlluxioURI("/nested/test/file");
  private static final AlluxioURI NESTED_FILE2_URI = new AlluxioURI("/nested/test/file2");
  private static final AlluxioURI NESTED_DIR_URI = new AlluxioURI("/nested/test/dir");
  private static final AlluxioURI NESTED_DIR_FILE_URI = new AlluxioURI("/nested/test/dir/file");
  private static final AlluxioURI NESTED_TEST_FILE_URI = new AlluxioURI("/nested/test_file");
  private static final AlluxioURI ROOT_URI = new AlluxioURI("/");
  private static final AlluxioURI ROOT_FILE_URI = new AlluxioURI("/file");
  private static final AlluxioURI ROOT_AFILE_URI = new AlluxioURI("/afile");
  private static final AlluxioURI TEST_URI = new AlluxioURI("/test");
  private static final String TEST_USER = "test";
  private static final GetStatusContext GET_STATUS_CONTEXT = GetStatusContext.defaults();

  // Constants for tests on persisted directories.
  private static final String DIR_PREFIX = "dir";
  private static final String DIR_TOP_LEVEL = "top";
  private static final String FILE_PREFIX = "file";
  private static final String MOUNT_PARENT_URI = "/mnt";
  private static final String MOUNT_URI = "/mnt/local";
  private static final int DIR_WIDTH = 2;

  private CreateFileContext mNestedFileContext;
  private MasterRegistry mRegistry;
  private JournalSystem mJournalSystem;
  private BlockMaster mBlockMaster;
  private ExecutorService mExecutorService;
  private DefaultFileSystemMaster mFileSystemMaster;
  private InodeTree mInodeTree;
  private ReadOnlyInodeStore mInodeStore;
  private InodeStore.Factory mInodeStoreFactory;
  private MetricsMaster mMetricsMaster;
  private List<Metric> mMetrics;
  private long mWorkerId1;
  private long mWorkerId2;

  private String mJournalFolder;
  private String mUnderFS;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

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

  @Parameters
  public static Iterable<InodeStore.Factory> parameters() throws Exception {
    String dir =
        AlluxioTestDirectory.createTemporaryDirectory("inode-store-test").getAbsolutePath();
    return Arrays.asList(
        lockManager -> new HeapInodeStore(),
        lockManager -> new RocksInodeStore(dir),
        lockManager -> new CachingInodeStore(new RocksInodeStore(dir), lockManager));
  }

  public FileSystemMasterTest(InodeStore.Factory factory) {
    mInodeStoreFactory = factory;
  }

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
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

  @Test
  public void createPathWithWhiteSpaces() throws Exception {
    String[] paths = new String[]{
        "/ ",
        "/  ",
        "/ path",
        "/path ",
        "/pa th",
        "/ pa th ",
        "/pa/ th",
        "/pa / th",
        "/ pa / th ",
    };
    for (String path : paths) {
      AlluxioURI uri = new AlluxioURI(path);
      long id = mFileSystemMaster.createFile(uri, CreateFileContext.mergeFrom(
          CreateFilePOptions.newBuilder().setRecursive(true))).getFileId();
      Assert.assertEquals(id, mFileSystemMaster.getFileId(uri));
      mFileSystemMaster.delete(uri, DeleteContext.defaults());
      id = mFileSystemMaster.createDirectory(uri, CreateDirectoryContext
          .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
      Assert.assertEquals(id, mFileSystemMaster.getFileId(uri));
    }
  }

  @Test
  public void createFileMustCacheThenCacheThrough() throws Exception {
    File file = mTestFolder.newFile();
    AlluxioURI path = new AlluxioURI("/test");
    mFileSystemMaster.createFile(path,
        CreateFileContext.defaults().setWriteType(WriteType.MUST_CACHE));

    mThrown.expect(FileAlreadyExistsException.class);
    mFileSystemMaster.createFile(path,
        CreateFileContext.defaults().setWriteType(WriteType.MUST_CACHE));
  }

  @Test
  public void createFileUsesOperationTime() throws Exception {
    AlluxioURI path = new AlluxioURI("/test");
    mFileSystemMaster.createFile(path, CreateFileContext.defaults().setOperationTimeMs(100));
    FileInfo info = mFileSystemMaster.getFileInfo(path, GetStatusContext.defaults());
    assertEquals(100, info.getLastModificationTimeMs());
    assertEquals(100, info.getLastAccessTimeMs());
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method.
   */
  @Test
  public void deleteFile() throws Exception {
    // cannot delete root
    try {
      mFileSystemMaster.delete(ROOT_URI,
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
      fail("Should not have been able to delete the root");
    } catch (InvalidPathException e) {
      assertEquals(ExceptionMessage.DELETE_ROOT_DIRECTORY.getMessage(), e.getMessage());
    }

    // delete the file
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.delete(NESTED_FILE_URI, DeleteContext.defaults());

    try {
      mBlockMaster.getBlockInfo(blockId);
      fail("Expected blockInfo to fail");
    } catch (BlockInfoException e) {
      // expected
    }

    // Update the heartbeat of removedBlockId received from worker 1.
    Command heartbeat1 = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat1);
    assertFalse(mBlockMaster.isBlockLost(blockId));

    // verify the file is deleted
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));

    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());
    // Create ufs file.
    Files.createDirectory(Paths.get(ufsMount.join("dir1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file1").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());

    AlluxioURI uri = new AlluxioURI("/mnt/local/dir1");
    mFileSystemMaster.listStatus(uri, ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS)));
    mFileSystemMaster.delete(new AlluxioURI("/mnt/local/dir1/file1"),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setAlluxioOnly(true)));

    // ufs file still exists
    assertTrue(Files.exists(Paths.get(ufsMount.join("dir1").join("file1").getPath())));
    // verify the file is deleted
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local/dir1/file1"), GetStatusContext
        .mergeFrom(GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)));
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method with a
   * non-empty directory.
   */
  @Test
  public void deleteNonemptyDirectory() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    String dirName = mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).getName();
    try {
      mFileSystemMaster.delete(NESTED_URI, DeleteContext.defaults());
      fail("Deleting a non-empty directory without setting recursive should fail");
    } catch (DirectoryNotEmptyException e) {
      String expectedMessage =
          ExceptionMessage.DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE.getMessage(dirName);
      assertEquals(expectedMessage, e.getMessage());
    }

    // Now delete with recursive set to true.
    mFileSystemMaster.delete(NESTED_URI,
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a directory.
   */
  @Test
  public void deleteDir() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    // delete the dir
    mFileSystemMaster.delete(NESTED_URI,
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));

    // verify the dir is deleted
    assertEquals(-1, mFileSystemMaster.getFileId(NESTED_URI));

    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());
    // Create ufs file.
    Files.createDirectory(Paths.get(ufsMount.join("dir1").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());
    // load the dir1 to alluxio
    mFileSystemMaster.listStatus(new AlluxioURI("/mnt/local"), ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS)));
    mFileSystemMaster.delete(new AlluxioURI("/mnt/local/dir1"), DeleteContext
        .mergeFrom(DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(true)));
    // ufs directory still exists
    assertTrue(Files.exists(Paths.get(ufsMount.join("dir1").getPath())));
    // verify the directory is deleted
    Files.delete(Paths.get(ufsMount.join("dir1").getPath()));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/mnt/local/dir1")));
  }

  @Test
  public void deleteRecursiveClearsInnerInodesAndEdges() throws Exception {
    createFileWithSingleBlock(new AlluxioURI("/a/b/c/d/e"));
    createFileWithSingleBlock(new AlluxioURI("/a/b/x/y/z"));
    mFileSystemMaster.delete(new AlluxioURI("/a/b"),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
    assertEquals(1, mInodeStore.allEdges().size());
    assertEquals(2, mInodeStore.allInodes().size());
  }

  @Test
  public void deleteDirRecursiveWithPermissions() throws Exception {
    // userA has permissions to delete directory and nested file
    createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      mFileSystemMaster.delete(NESTED_URI,
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
    }
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_URI));
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
  }

  @Test
  public void deleteDirRecursiveWithReadOnlyCheck() throws Exception {
    AlluxioURI rootPath = new AlluxioURI("/mnt/");
    mFileSystemMaster.createDirectory(rootPath, CreateDirectoryContext.defaults());
    // Create ufs file.
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    Files.createDirectory(Paths.get(ufsMount.join("dir1").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
            MountContext.mergeFrom(MountPOptions.newBuilder().setReadOnly(true)));
    mThrown.expect(AccessControlException.class);
    // Will throw AccessControlException because /mnt/local is a readonly mount point
    mFileSystemMaster.delete(rootPath,
            DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
  }

  @Test
  public void deleteDirRecursiveWithInsufficientPermissions() throws Exception {
    // userA has permissions to delete directory but not one of the nested files
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0700).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE2_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      mFileSystemMaster.delete(NESTED_URI,
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
      fail("Deleting a directory w/ insufficient permission on child should fail");
    } catch (FailedPreconditionException e) {
      assertTrue(e.getMessage().contains("/nested/test/file (Permission denied"));
      assertTrue(e.getMessage().contains("/nested/test (Directory not empty)"));
    }
    // Then the nested file and the dir will be left
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
    // File with permission will be deleted
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE2_URI));
  }

  @Test
  public void deleteDirRecursiveNoPermOnFile() throws Exception {
    // The structure looks like below
    // /nested
    // /nested/test/file
    // /nested/test/file2
    // /nested/test2/file
    // /nested/test2/file2
    // /nested/test2/dir
    // /nested/test2/dir/file
    // userA has no permission on /nested/test/file
    // So deleting the root will fail on:
    // /nested/, /nested/test/, /nested/test/file
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/file"));
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/file2"));
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/dir/file"));

    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0700).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE2_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/file2"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/dir/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));

    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      mFileSystemMaster.delete(new AlluxioURI("/nested"),
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
      fail("Deleting a directory w/ insufficient permission on child should fail");
    } catch (FailedPreconditionException e) {
      assertTrue(e.getMessage().contains("/nested/test/file (Permission denied"));
      assertTrue(e.getMessage().contains("/nested/test (Directory not empty)"));
    }
    // The existing files/dirs will be: /, /nested/, /nested/test/, /nested/test/file
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(ROOT_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested")));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
    // The other files should be deleted successfully
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE2_URI));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/file")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/file2")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/dir")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/dir/file")));
  }

  @Test
  public void deleteDirRecursiveNoPermOnFileDiffOrder() throws Exception {
    // The structure looks like below
    // /nested
    // /nested/test/file
    // /nested/test/file2
    // /nested/test2/file
    // /nested/test2/file2
    // /nested/test2/dir
    // /nested/test2/dir/file
    // userA has no permission on /nested/test/file2
    // So deleting the root will fail on:
    // /nested/, /nested/test/, /nested/test/file2
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/file"));
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/file2"));
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/dir/file"));

    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE2_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0700).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/file2"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/dir/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));

    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      mFileSystemMaster.delete(new AlluxioURI("/nested"),
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
      fail("Deleting a directory w/ insufficient permission on child should fail");
    } catch (FailedPreconditionException e) {
      assertTrue(e.getMessage().contains("/nested/test/file2 (Permission denied"));
      assertTrue(e.getMessage().contains("/nested/test (Directory not empty)"));
    }
    // The existing files/dirs will be: /, /nested/, /nested/test/, /nested/test/file
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(ROOT_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested")));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE2_URI));
    // The other files should be deleted successfully
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/file")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/file2")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/dir")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/dir/file")));
  }

  @Test
  public void deleteNestedDirRecursiveNoPermOnFile() throws Exception {
    // The structure looks like below
    // /nested
    // /nested/nested
    // /nested/nested/test/file
    // /nested/nested/test/file2
    // userA has no permission on /nested/nested/test/file
    // So deleting the root will fail on:
    // /nested/, /nested/nested, /nested/nested/test/, /nested/nested/test/file
    createFileWithSingleBlock(new AlluxioURI("/nested/nested/test/file"));
    createFileWithSingleBlock(new AlluxioURI("/nested/nested/test/file2"));

    mFileSystemMaster.setAttribute(new AlluxioURI("/nested"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/nested"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/nested/test"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/nested/test/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0700).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/nested/test/file2"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));

    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      mFileSystemMaster.delete(new AlluxioURI("/nested"),
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
      fail("Deleting a directory w/ insufficient permission on child should fail");
    } catch (FailedPreconditionException e) {
      assertTrue(e.getMessage().contains("/nested/nested/test/file (Permission denied"));
      assertTrue(e.getMessage().contains("/nested/nested/test (Directory not empty)"));
      assertTrue(e.getMessage().contains("/nested/nested (Directory not empty)"));
      assertTrue(e.getMessage().contains("/nested (Directory not empty)"));
    }
    // The existing files/dirs will be: /, /nested/, /nested/nested/,
    // /nested/nested/test/, /nested/test/file
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(ROOT_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested")));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/nested")));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/nested/test")));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/nested/test/file")));
    // The other files should be deleted successfully
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/nested/test/file2")));
  }

  @Test
  public void deleteDirRecursiveNoPermOnDir() throws Exception {
    // The structure looks like below
    // /nested/
    //
    // /nested/test/
    // /nested/test/file
    // /nested/test/file2
    // /nested/test/dir
    // /nested/test/dir/file
    //
    // /nested/test2/
    // /nested/test2/file
    // /nested/test2/file2
    // /nested/test2/dir
    // /nested/test2/dir/file
    // userA has no permission on /nested/test/file
    // So deleting the root will fail on:
    // /nested/, /nested/test/, /nested/test/file
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_FILE_URI);
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/file"));
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/file2"));
    createFileWithSingleBlock(new AlluxioURI("/nested/test2/dir/file"));

    // No permission on the dir, therefore all the files will be kept
    // although the user has permission over those nested files
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0700).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(NESTED_FILE2_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(NESTED_DIR_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test/dir/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    // The user has permission over everything under this dir, so everything will be removed
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/file2"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    mFileSystemMaster.setAttribute(new AlluxioURI("/nested/test2/dir/file"), SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));

    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      mFileSystemMaster.delete(new AlluxioURI("/nested"),
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
      fail("Deleting a directory w/ insufficient permission on child should fail");
    } catch (FailedPreconditionException e) {
      assertTrue(e.getMessage().contains("/nested/test (Permission denied"));
      assertTrue(e.getMessage().contains("/nested (Directory not empty)"));
    }
    // The existing files/dirs will be: /, /nested/, /nested/test/
    // and everything under /nested/test/
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(ROOT_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested")));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test/file2")));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_DIR_URI));
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_DIR_FILE_URI));
    // The other files should be deleted successfully
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/file")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/file2")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/dir")));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(new AlluxioURI("/nested/test2/dir/file")));
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a directory with persistent entries with a sync check.
   */
  @Test
  public void deleteSyncedPersistedDirectoryWithCheck() throws Exception {
    deleteSyncedPersistedDirectory(1, false);
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a directory with persistent entries without a sync check.
   */
  @Test
  public void deleteSyncedPersistedDirectoryWithoutCheck() throws Exception {
    deleteSyncedPersistedDirectory(1, true);
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a multi-level directory with persistent entries with a sync check.
   */
  @Test
  public void deleteSyncedPersistedMultilevelDirectoryWithCheck() throws Exception {
    deleteSyncedPersistedDirectory(3, false);
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a multi-level directory with persistent entries without a sync check.
   */
  @Test
  public void deleteSyncedPersistedMultilevelDirectoryWithoutCheck() throws Exception {
    deleteSyncedPersistedDirectory(3, true);
  }

  // Helper method for deleteSynctedPersisted* tests.
  private void deleteSyncedPersistedDirectory(int levels, boolean unchecked) throws Exception {
    AlluxioURI ufsMount = createPersistedDirectories(levels);
    mountPersistedDirectories(ufsMount);
    loadPersistedDirectories(levels);
    // delete top-level directory
    mFileSystemMaster.delete(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)
            .setAlluxioOnly(false).setUnchecked(unchecked)));
    checkPersistedDirectoriesDeleted(levels, ufsMount, Collections.EMPTY_LIST);
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a directory with un-synced persistent entries with a sync check.
   */
  @Test
  public void deleteUnsyncedPersistedDirectoryWithCheck() throws Exception {
    AlluxioURI ufsMount = createPersistedDirectories(1);
    mountPersistedDirectories(ufsMount);
    loadPersistedDirectories(1);
    // Add a file to the UFS.
    Files.createFile(
        Paths.get(ufsMount.join(DIR_TOP_LEVEL).join(FILE_PREFIX + (DIR_WIDTH)).getPath()));
    // delete top-level directory
    try {
      mFileSystemMaster.delete(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL),
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)
              .setAlluxioOnly(false).setUnchecked(false)));
      fail();
    } catch (IOException e) {
      // Expected
    }
    // Check all that could be deleted.
    List<AlluxioURI> except = new ArrayList<>();
    except.add(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL));
    checkPersistedDirectoriesDeleted(1, ufsMount, except);
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a directory with un-synced persistent entries without a sync check.
   */
  @Test
  public void deleteUnsyncedPersistedDirectoryWithoutCheck() throws Exception {
    AlluxioURI ufsMount = createPersistedDirectories(1);
    mountPersistedDirectories(ufsMount);
    loadPersistedDirectories(1);
    // Add a file to the UFS.
    Files.createFile(
        Paths.get(ufsMount.join(DIR_TOP_LEVEL).join(FILE_PREFIX + (DIR_WIDTH)).getPath()));
    // delete top-level directory
    mFileSystemMaster.delete(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL), DeleteContext.mergeFrom(
        DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(false).setUnchecked(true)));
    checkPersistedDirectoriesDeleted(1, ufsMount, Collections.EMPTY_LIST);
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a multi-level directory with un-synced persistent entries with a sync check.
   */
  @Test
  public void deleteUnsyncedPersistedMultilevelDirectoryWithCheck() throws Exception {
    AlluxioURI ufsMount = createPersistedDirectories(3);
    mountPersistedDirectories(ufsMount);
    loadPersistedDirectories(3);
    // Add a file to the UFS down the tree.
    Files.createFile(Paths.get(ufsMount.join(DIR_TOP_LEVEL).join(DIR_PREFIX + 0)
        .join(FILE_PREFIX + (DIR_WIDTH)).getPath()));
    mThrown.expect(IOException.class);
    // delete top-level directory
    mFileSystemMaster.delete(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL), DeleteContext.mergeFrom(
        DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(false).setUnchecked(false)));
    // Check all that could be deleted.
    List<AlluxioURI> except = new ArrayList<>();
    except.add(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL));
    except.add(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL).join(DIR_PREFIX + 0));
    checkPersistedDirectoriesDeleted(3, ufsMount, except);
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, DeleteContext)} method for
   * a multi-level directory with un-synced persistent entries without a sync check.
   */
  @Test
  public void deleteUnsyncedPersistedMultilevelDirectoryWithoutCheck() throws Exception {
    AlluxioURI ufsMount = createPersistedDirectories(3);
    mountPersistedDirectories(ufsMount);
    loadPersistedDirectories(3);
    // Add a file to the UFS down the tree.
    Files.createFile(Paths.get(ufsMount.join(DIR_TOP_LEVEL).join(DIR_PREFIX + 0)
        .join(FILE_PREFIX + (DIR_WIDTH)).getPath()));
    // delete top-level directory
    mFileSystemMaster.delete(new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL), DeleteContext.mergeFrom(
        DeletePOptions.newBuilder().setRecursive(true).setAlluxioOnly(false).setUnchecked(true)));
    checkPersistedDirectoriesDeleted(3, ufsMount, Collections.EMPTY_LIST);
  }

  // Helper method to check if expected entries were deleted.
  private void checkPersistedDirectoriesDeleted(int levels, AlluxioURI ufsMount,
      List<AlluxioURI> except) throws Exception {
    checkPersistedDirectoryDeletedLevel(levels, new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL),
        ufsMount.join(DIR_TOP_LEVEL), except);
  }

  private void checkPersistedDirectoryDeletedLevel(int levels, AlluxioURI alluxioURI,
      AlluxioURI ufsURI, List<AlluxioURI> except) throws Exception {
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
  private AlluxioURI createPersistedDirectories(int levels) throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    // Create top-level directory in UFS.
    Files.createDirectory(Paths.get(ufsMount.join(DIR_TOP_LEVEL).getPath()));
    createPersistedDirectoryLevel(levels, ufsMount.join(DIR_TOP_LEVEL));
    return ufsMount;
  }

  private void createPersistedDirectoryLevel(int levels, AlluxioURI ufsTop) throws Exception {
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
  private void loadPersistedDirectories(int levels) throws Exception {
    // load persisted ufs entries to alluxio
    mFileSystemMaster.listStatus(new AlluxioURI(MOUNT_URI), ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS)));
    loadPersistedDirectoryLevel(levels, new AlluxioURI(MOUNT_URI).join(DIR_TOP_LEVEL));
  }

  private void loadPersistedDirectoryLevel(int levels, AlluxioURI alluxioTop) throws Exception {
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
  private void mountPersistedDirectories(AlluxioURI ufsMount) throws Exception {
    mFileSystemMaster.createDirectory(new AlluxioURI(MOUNT_PARENT_URI),
        CreateDirectoryContext.defaults());
    mFileSystemMaster.mount(new AlluxioURI(MOUNT_URI), ufsMount,
        MountContext.defaults());
  }

  /**
   * Tests the {@link FileSystemMaster#getNewBlockIdForFile(AlluxioURI)} method.
   */
  @Test
  public void getNewBlockIdForFile() throws Exception {
    mFileSystemMaster.createFile(NESTED_FILE_URI, mNestedFileContext);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(NESTED_FILE_URI);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT);
    assertEquals(Lists.newArrayList(blockId), fileInfo.getBlockIds());
  }

  @Test
  public void getPath() throws Exception {
    AlluxioURI rootUri = new AlluxioURI("/");
    long rootId = mFileSystemMaster.getFileId(rootUri);
    assertEquals(rootUri, mFileSystemMaster.getPath(rootId));

    // get non-existent id
    try {
      mFileSystemMaster.getPath(rootId + 1234);
      fail("getPath() for a non-existent id should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  /**
   * Tests the {@link FileSystemMaster#getPersistenceState(long)} method.
   */
  @Test
  public void getPersistenceState() throws Exception {
    AlluxioURI rootUri = new AlluxioURI("/");
    long rootId = mFileSystemMaster.getFileId(rootUri);
    assertEquals(PersistenceState.PERSISTED, mFileSystemMaster.getPersistenceState(rootId));

    // get non-existent id
    try {
      mFileSystemMaster.getPersistenceState(rootId + 1234);
      fail("getPath() for a non-existent id should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  /**
   * Tests the {@link FileSystemMaster#getFileId(AlluxioURI)} method.
   */
  @Test
  public void getFileId() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);

    // These URIs exist.
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(ROOT_URI));
    assertEquals(ROOT_URI, mFileSystemMaster.getPath(mFileSystemMaster.getFileId(ROOT_URI)));

    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_URI));
    assertEquals(NESTED_URI,
        mFileSystemMaster.getPath(mFileSystemMaster.getFileId(NESTED_URI)));

    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
    assertEquals(NESTED_FILE_URI,
        mFileSystemMaster.getPath(mFileSystemMaster.getFileId(NESTED_FILE_URI)));

    // These URIs do not exist.
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(ROOT_FILE_URI));
    assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(TEST_URI));
    assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(NESTED_FILE_URI.join("DNE")));
  }

  /**
   * Tests the {@link FileSystemMaster#getFileInfo(AlluxioURI, GetStatusContext)} method.
   */
  @Test
  public void getFileInfo() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    long fileId;
    FileInfo info;

    fileId = mFileSystemMaster.getFileId(ROOT_URI);
    info = mFileSystemMaster.getFileInfo(fileId);
    assertEquals(ROOT_URI.getPath(), info.getPath());
    assertEquals(ROOT_URI.getPath(),
        mFileSystemMaster.getFileInfo(ROOT_URI, GET_STATUS_CONTEXT).getPath());

    fileId = mFileSystemMaster.getFileId(NESTED_URI);
    info = mFileSystemMaster.getFileInfo(fileId);
    assertEquals(NESTED_URI.getPath(), info.getPath());
    assertEquals(NESTED_URI.getPath(),
        mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).getPath());

    fileId = mFileSystemMaster.getFileId(NESTED_FILE_URI);
    info = mFileSystemMaster.getFileInfo(fileId);
    assertEquals(NESTED_FILE_URI.getPath(), info.getPath());
    assertEquals(NESTED_FILE_URI.getPath(),
        mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).getPath());

    // Test non-existent id.
    try {
      mFileSystemMaster.getFileInfo(fileId + 1234);
      fail("getFileInfo() for a non-existent id should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }

    // Test non-existent URIs.
    try {
      mFileSystemMaster.getFileInfo(ROOT_FILE_URI, GET_STATUS_CONTEXT);
      fail("getFileInfo() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
    try {
      mFileSystemMaster.getFileInfo(TEST_URI, GET_STATUS_CONTEXT);
      fail("getFileInfo() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
    try {
      mFileSystemMaster.getFileInfo(NESTED_URI.join("DNE"), GET_STATUS_CONTEXT);
      fail("getFileInfo() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  @Test
  public void getFileInfoWithLoadMetadata() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());

    // Create ufs file.
    Files.createFile(Paths.get(ufsMount.join("file").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());

    // 3 directories exist.
    assertEquals(3, countPaths());

    // getFileInfo should load metadata automatically.
    AlluxioURI uri = new AlluxioURI("/mnt/local/file");
    assertEquals(uri.getPath(),
        mFileSystemMaster.getFileInfo(uri, GET_STATUS_CONTEXT).getPath());

    // getFileInfo should have loaded another file, so now 4 paths exist.
    assertEquals(4, countPaths());
  }

  @Test
  public void getFileIdWithLoadMetadata() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());

    // Create ufs file.
    Files.createFile(Paths.get(ufsMount.join("file").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());

    // 3 directories exist.
    assertEquals(3, countPaths());

    // getFileId should load metadata automatically.
    AlluxioURI uri = new AlluxioURI("/mnt/local/file");
    assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(uri));

    // getFileId should have loaded another file, so now 4 paths exist.
    assertEquals(4, countPaths());
  }

  private long countPaths() throws Exception {
    return mInodeTree.getInodeCount();
  }

  @Test
  public void listStatusWithLoadMetadataNever() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());

    // Create ufs file.
    Files.createDirectory(Paths.get(ufsMount.join("dir1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file2").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());

    // 3 directories exist.
    assertEquals(3, countPaths());

    // getFileId should load metadata automatically.
    AlluxioURI uri = new AlluxioURI("/mnt/local/dir1");
    try {
      mFileSystemMaster.listStatus(uri, ListStatusContext
          .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)));
      fail("Exception expected");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }

    assertEquals(3, countPaths());
  }

  @Test
  public void listStatusWithLoadMetadataOnce() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());

    // Create ufs file.
    Files.createDirectory(Paths.get(ufsMount.join("dir1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file2").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());

    // 3 directories exist.
    assertEquals(3, countPaths());

    // getFileId should load metadata automatically.
    AlluxioURI uri = new AlluxioURI("/mnt/local/dir1");
    List<FileInfo> fileInfoList =
        mFileSystemMaster.listStatus(uri, ListStatusContext.defaults());
    Set<String> paths = new HashSet<>();
    for (FileInfo fileInfo : fileInfoList) {
      paths.add(fileInfo.getPath());
    }
    assertEquals(2, paths.size());
    assertTrue(paths.contains("/mnt/local/dir1/file1"));
    assertTrue(paths.contains("/mnt/local/dir1/file2"));
    // listStatus should have loaded another 3 files (dir1, dir1/file1, dir1/file2), so now 6
    // paths exist.
    assertEquals(6, countPaths());
  }

  @Test
  public void listStatusWithLoadMetadataAlways() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());

    // Create ufs file.
    Files.createDirectory(Paths.get(ufsMount.join("dir1").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());

    // 3 directories exist.
    assertEquals(3, countPaths());

    // getFileId should load metadata automatically.
    AlluxioURI uri = new AlluxioURI("/mnt/local/dir1");
    List<FileInfo> fileInfoList =
        mFileSystemMaster.listStatus(uri, ListStatusContext.defaults());
    assertEquals(0, fileInfoList.size());
    // listStatus should have loaded another files (dir1), so now 4 paths exist.
    assertEquals(4, countPaths());

    // Add two files.
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file2").getPath()));

    fileInfoList =
        mFileSystemMaster.listStatus(uri, ListStatusContext.defaults());
    assertEquals(0, fileInfoList.size());
    // No file is loaded since dir1 has been loaded once.
    assertEquals(4, countPaths());

    fileInfoList = mFileSystemMaster.listStatus(uri, ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS)));
    Set<String> paths = new HashSet<>();
    for (FileInfo fileInfo : fileInfoList) {
      paths.add(fileInfo.getPath());
    }
    assertEquals(2, paths.size());
    assertTrue(paths.contains("/mnt/local/dir1/file1"));
    assertTrue(paths.contains("/mnt/local/dir1/file2"));
    // listStatus should have loaded another 2 files (dir1/file1, dir1/file2), so now 6
    // paths exist.
    assertEquals(6, countPaths());
  }

  /**
   * Tests listing status on a non-persisted directory.
   */
  @Test
  public void listStatusWithLoadMetadataNonPersistedDir() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());

    // Create ufs file.
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());

    // 3 directories exist.
    assertEquals(3, countPaths());

    // Create a drectory in alluxio which is not persisted.
    AlluxioURI folder = new AlluxioURI("/mnt/local/folder");
    mFileSystemMaster.createDirectory(folder, CreateDirectoryContext.defaults());

    assertFalse(
        mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local/folder"), GET_STATUS_CONTEXT)
            .isPersisted());

    // Create files in ufs.
    Files.createDirectory(Paths.get(ufsMount.join("folder").getPath()));
    Files.createFile(Paths.get(ufsMount.join("folder").join("file1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("folder").join("file2").getPath()));

    // getStatus won't mark folder as persisted.
    assertFalse(
        mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local/folder"), GET_STATUS_CONTEXT)
            .isPersisted());

    List<FileInfo> fileInfoList =
        mFileSystemMaster.listStatus(folder, ListStatusContext.defaults());
    assertEquals(2, fileInfoList.size());
    // listStatus should have loaded files (folder, folder/file1, folder/file2), so now 6 paths
    // exist.
    assertEquals(6, countPaths());

    Set<String> paths = new HashSet<>();
    for (FileInfo f : fileInfoList) {
      paths.add(f.getPath());
    }
    assertEquals(2, paths.size());
    assertTrue(paths.contains("/mnt/local/folder/file1"));
    assertTrue(paths.contains("/mnt/local/folder/file2"));

    assertTrue(
        mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local/folder"), GET_STATUS_CONTEXT)
            .isPersisted());
  }

  @Test
  public void listStatus() throws Exception {
    final int files = 10;
    List<FileInfo> infos;
    List<String> filenames;

    // Test files in root directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(ROOT_URI.join("file" + String.format("%05d", i)));
    }
    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)));
    assertEquals(files, infos.size());
    // Copy out filenames to use List contains.
    filenames = new ArrayList<>();
    for (FileInfo info : infos) {
      filenames.add(info.getPath());
    }
    // Compare all filenames.
    for (int i = 0; i < files; i++) {
      assertTrue(
          filenames.contains(ROOT_URI.join("file" + String.format("%05d", i)).toString()));
    }

    // Test single file.
    createFileWithSingleBlock(ROOT_FILE_URI);
    infos = mFileSystemMaster.listStatus(ROOT_FILE_URI, ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)));
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.getPath(), infos.get(0).getPath());

    // Test files in nested directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(NESTED_URI.join("file" + String.format("%05d", i)));
    }
    infos = mFileSystemMaster.listStatus(NESTED_URI, ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)));
    assertEquals(files, infos.size());
    // Copy out filenames to use List contains.
    filenames = new ArrayList<>();
    for (FileInfo info : infos) {
      filenames.add(info.getPath());
    }
    // Compare all filenames.
    for (int i = 0; i < files; i++) {
      assertTrue(
          filenames.contains(NESTED_URI.join("file" + String.format("%05d", i)).toString()));
    }

    // Test non-existent URIs.
    try {
      mFileSystemMaster.listStatus(NESTED_URI.join("DNE"), ListStatusContext
          .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)));
      fail("listStatus() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  private ListStatusContext genListStatusPrefix(String prefix, boolean recursive) {
    return ListStatusContext.mergeFrom(
            ListStatusPartialPOptions.newBuilder().setPrefix(prefix).setOptions(
                ListStatusPOptions.newBuilder()
                    .setLoadMetadataType(LoadMetadataPType.NEVER)
                    .setRecursive(recursive)));
  }

  private ListStatusContext genListStatusStartAfter(String startAfter, boolean recursive) {
    return ListStatusContext.mergeFrom(
            ListStatusPartialPOptions.newBuilder().setStartAfter(startAfter).setOptions(
                ListStatusPOptions.newBuilder()
        .setLoadMetadataType(LoadMetadataPType.NEVER)
        .setRecursive(recursive)));
  }

  private ListStatusContext genListStatusPrefixStartAfter(
      String prefix, String startAfter, boolean recursive) {
    return ListStatusContext.mergeFrom(
        ListStatusPartialPOptions.newBuilder().setPrefix(prefix)
            .setStartAfter(startAfter).setOptions(ListStatusPOptions.newBuilder()
                .setLoadMetadataType(LoadMetadataPType.NEVER)
                .setRecursive(recursive)));
  }

  private ListStatusContext genListStatusPartial(
      int batchSize, long offset, boolean recursive, String prefix, String startAfter) {
    return ListStatusContext.mergeFrom(
        ListStatusPartialPOptions.newBuilder().setStartAfter(startAfter)
            .setBatchSize(batchSize).setOffset(offset)
            .setPrefix(prefix).setOptions(
                ListStatusPOptions.newBuilder()
                    .setLoadMetadataType(LoadMetadataPType.NEVER)
                    .setRecursive(recursive)));
  }

  @Test
  public void listStatusPartialDelete() throws Exception {
    long offset = 0;
    List<FileInfo> infos;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);

    // List two file from NESTED_URI, getting its offset
    ListStatusContext context = genListStatusPartial(2, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(2, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(1).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(1).getFileId();

    // The next file should be NESTED_FILE_URI2
    context = genListStatusPartial(2, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(1, infos.size());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());

    // Now delete NESTED_FILE_URI, so the offset is no longer valid
    mFileSystemMaster.delete(NESTED_FILE_URI,
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));

    // Should throw an inode does not exist exception
    final long throwOffset = offset;
    assertThrows(FileDoesNotExistException.class, () -> mFileSystemMaster.listStatus(
        NESTED_URI, genListStatusPartial(1, throwOffset, true, "", "")));

    // Insert a new file with the same name
    createFileWithSingleBlock(NESTED_FILE_URI);

    // An exception should still be thrown because it has a new inode id
    assertThrows(FileDoesNotExistException.class, () -> mFileSystemMaster.listStatus(
        NESTED_URI, genListStatusPartial(1, throwOffset, true, "", "")));
  }

  @Test
  public void listStatusPartialRename() throws Exception {
    long offset = 0;
    List<FileInfo> infos;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);

    ListStatusContext context = genListStatusPartial(2, offset, true, "", "");
    // List two file from NESTED_URI, getting its offset
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(2, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(1).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(1).getFileId();

    // The next file should still be NESTED_FILE_URI2
    context = genListStatusPartial(2, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(1, infos.size());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());

    AlluxioURI renameTo = new AlluxioURI(NESTED_FILE_URI + "1");
    // Now rename NESTED_FILE_URI, but in the same directory
    mFileSystemMaster.rename(NESTED_FILE_URI, renameTo,
        RenameContext.defaults());

    // The next file should still be NESTED_FILE_URI2
    context = genListStatusPartial(2, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(1, infos.size());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());

    // Now rename the renamed file into a different directory
    mFileSystemMaster.rename(renameTo, new AlluxioURI("/moveHere"),
        RenameContext.defaults());
    // Should throw an exception since we are no longer in the same path as NESTED_URI
    final long finalOffset = offset;
    assertThrows(FileDoesNotExistException.class,
        () -> mFileSystemMaster.listStatus(NESTED_URI, genListStatusPartial(
            1, finalOffset, true, "", "")));
  }

  @Test
  public void listStatusStartAfter() throws Exception {
    List<FileInfo> infos;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(ROOT_AFILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);
    createFileWithSingleBlock(NESTED_TEST_FILE_URI);

    // list without recursion, and start after "/file",
    // the results should be sorted by name
    ListStatusContext context = genListStatusStartAfter(
        ROOT_FILE_URI.getPath(), false);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(3, context.getTotalListings());
    assertFalse(context.isTruncated());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());

    // list with recursion, and start after "/file",
    // the results should be sorted by name
    context = genListStatusStartAfter(
        ROOT_FILE_URI.getPath(), true);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(6, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_URI.toString(), infos.get(1).getPath());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(2).getPath());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(3).getPath());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(4).getPath());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(5).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list "/nested/test" with recursion, and start after "/file",
    // the results should be sorted by name
    context = genListStatusStartAfter("/file", true);
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list "/nested/test" with recursion, and start after "/dir",
    // the results should be sorted by name
    context = genListStatusStartAfter("/di", true);
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(3, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(1).getPath());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(2).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list "/nested/test" with recursion, and start after "/dir",
    // the results should be sorted by name
    context = genListStatusStartAfter("/dir", true);
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(2, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(1).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());
  }

  @Test
  public void listStatusPrefixNestedStartAfter() throws Exception {
    List<FileInfo> infos;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(TEST_URI);
    createFileWithSingleBlock(ROOT_AFILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);
    createFileWithSingleBlock(NESTED_TEST_FILE_URI);

    // list without recursion, with prefix "/file" and startAfter "/fi"
    ListStatusContext context = genListStatusPrefixStartAfter(
        ROOT_FILE_URI.getPath(), "/fi", false);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.toString(), infos.get(0).getPath());
    assertEquals(4, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list without recursion, with prefix "/file" and startAfter "/file"
    context = genListStatusPrefixStartAfter(
        ROOT_FILE_URI.getPath(), ROOT_FILE_URI.getPath(), false);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(0, infos.size());
    assertEquals(4, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list with recursion, with prefix "/fi" and startAfter "/file"
    context = genListStatusPrefixStartAfter(
        "/fi", ROOT_FILE_URI.getPath(), false);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(0, infos.size());
    assertEquals(4, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list with recursion, with prefix "/fi" and startAfter "/file"
    context = genListStatusPrefixStartAfter(
        "/fi", ROOT_FILE_URI.getPath(), true);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(0, infos.size());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list with recursion, with prefix "/ne" and startAfter "/file"
    context = genListStatusPrefixStartAfter(
        "/ne", ROOT_FILE_URI.getPath(), true);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(6, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_URI.toString(), infos.get(1).getPath());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(2).getPath());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(3).getPath());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(4).getPath());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(5).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list with recursion, with prefix "/ne" and startAfter "/nested/test"
    context = genListStatusPrefixStartAfter(
        "/ne", "/nested/test", true);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(4, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(1).getPath());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(2).getPath());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(3).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());
  }

  @Test
  public void listStatusPartialPrefixNestedStartAfter() throws Exception {
    List<FileInfo> infos;
    long offset;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(TEST_URI);
    createFileWithSingleBlock(ROOT_AFILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);
    createFileWithSingleBlock(NESTED_TEST_FILE_URI);

    // list without recursion with prefix "/file" and start after "/fi",
    // the results should be sorted by name
    ListStatusContext context = genListStatusPartial(
        1, 0, false, ROOT_FILE_URI.getPath(), "/fi");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.toString(), infos.get(0).getPath());
    assertEquals(4, context.getTotalListings());
    assertFalse(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, false, ROOT_FILE_URI.getPath(), "/fi");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(4, context.getTotalListings());
    assertFalse(context.isTruncated());
    assertEquals(0, infos.size());

    // list with recursion from "/nested/test/" with prefix "/file" and start after "/fi",
    // the results should be sorted by name
    context = genListStatusPartial(
        1, 0, true, ROOT_FILE_URI.getPath(), "/fi");
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertTrue(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, ROOT_FILE_URI.getPath(), "/fi");
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, ROOT_FILE_URI.getPath(), "/fi");
    infos = mFileSystemMaster.listStatus(NESTED_URI, context);
    assertEquals(0, infos.size());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());

    // list with recursion from "/" with prefix "/nest" and start after "/nested/d",
    // the results should be sorted by name
    context = genListStatusPartial(
        1, 0, true, NESTED_BASE_URI.getPath(), "/nested/test/d");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertTrue(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "/nested/test/d");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    long prevOffset = offset;
    offset = infos.get(0).getFileId();

    // the start after parameter should be ignored if there is an offset != 0
    // so the result should be the same as the previous call
    context = genListStatusPartial(
        1, prevOffset, true, NESTED_BASE_URI.getPath(), "random-value");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "/nested/test/d");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "/nested/test/d");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "/nested/test/d");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(0, infos.size());
  }

  @Test
  public void listStatusPrefixNested() throws Exception {
    List<FileInfo> infos;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(ROOT_AFILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);

    // list one at a time without recursion, and prefix "/file",
    // the results should be sorted by name
    ListStatusContext context = genListStatusPrefix(
        ROOT_FILE_URI.getPath(), false);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(3, context.getTotalListings());

    // list without recursion, with a prefix that is longer than the result
    context = genListStatusPrefix(
        NESTED_FILE_URI.getPath(), true);
    infos = mFileSystemMaster.listStatus(NESTED_BASE_URI, context);
    assertEquals(0, infos.size());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());

    // list one at a time without recursion, and prefix "/nested",
    // the results should be sorted by name
    context = genListStatusPrefix(
        NESTED_BASE_URI.getPath(), false);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(3, context.getTotalListings());

    // start listing without recursion from "/nested", with a prefix of "/test/file"
    context = genListStatusPrefix("/test/file", false);
    infos = mFileSystemMaster.listStatus(NESTED_BASE_URI, context);
    assertEquals(0, infos.size());
    assertFalse(context.isTruncated());
    assertEquals(1, context.getTotalListings());

    // list one at a time with recursion, and prefix "/nested",
    // the results should be sorted by name
    context = genListStatusPrefix(
        NESTED_BASE_URI.getPath(), true);
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(5, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_URI.toString(), infos.get(1).getPath());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(2).getPath());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(3).getPath());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(4).getPath());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());

    // start listing with recursion from "/nested", with a prefix of "/test/file"
    context = genListStatusPrefix("/test/file", true);
    infos = mFileSystemMaster.listStatus(NESTED_BASE_URI, context);
    assertEquals(2, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(1).getPath());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
  }

  @Test
  public void listStatusPartialPrefixNested() throws Exception {
    long offset;
    List<FileInfo> infos;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(ROOT_AFILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_TEST_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);

    // list one at a time without recursion, and prefix "/file",
    // the results should be sorted by name
    ListStatusContext context = genListStatusPartial(
        1, 0, false, ROOT_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(3, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, false, ROOT_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(3, context.getTotalListings());
    assertEquals(0, infos.size());

    // list one at a time without recursion, and prefix "/nested",
    // the results should be sorted by name
    context = genListStatusPartial(
        1, 0, false, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertEquals(3, context.getTotalListings());
    assertFalse(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, false, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(3, context.getTotalListings());
    assertEquals(0, infos.size());

    // list one at a time without recursion from "/nested", and prefix "/test",
    // the results should be sorted by name
    context = genListStatusPartial(
        1, 0, false, "/test", "");
    infos = mFileSystemMaster.listStatus(NESTED_BASE_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_URI.toString(), infos.get(0).getPath());
    assertEquals(2, context.getTotalListings());
    assertTrue(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, false, "/test", "");
    infos = mFileSystemMaster.listStatus(NESTED_BASE_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(2, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, false, "/test", "");
    infos = mFileSystemMaster.listStatus(NESTED_BASE_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(2, context.getTotalListings());
    assertEquals(0, infos.size());

    // list one at a time with recursion with prefix "/file",
    // the results should be sorted by name,
    // and returned in a depth first manner
    context = genListStatusPartial(
        1, 0, true, ROOT_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, ROOT_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(0, infos.size());

    // list one at a time with recursion with prefix "/nested",
    // the results should be sorted by name,
    context = genListStatusPartial(
        1, 0, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    // give an invalid nested prefix during the partial listing
    long finalOffset = offset;
    assertThrows(InvalidPathException.class, () ->
        mFileSystemMaster.listStatus(ROOT_URI, genListStatusPartial(
            1, finalOffset, true, ROOT_FILE_URI.getPath(), "")));

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertTrue(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertTrue(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(0).getPath());
    assertEquals(-1, context.getTotalListings());
    assertFalse(context.isTruncated());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_BASE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(0, infos.size());

    // list one at a time with recursion with prefix "/nested/test/file",
    // the results should be sorted by name,
    // and returned in a depth first manner
    context = genListStatusPartial(
        1, 0, true, NESTED_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, NESTED_FILE_URI.getPath(), "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(0, infos.size());
  }

  @Test
  public void listStatusPartialNested() throws Exception {
    long offset;
    List<FileInfo> infos;

    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);
    createFileWithSingleBlock(NESTED_TEST_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE2_URI);
    createFileWithSingleBlock(NESTED_DIR_URI);

    // list one at a time without recursion, the results should be sorted by name
    ListStatusContext context = genListStatusPartial(
        1, 0, false, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(2, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, false, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(2, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, false, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(2, context.getTotalListings());
    assertEquals(0, infos.size());

    // list one at a time with recursion, the results should be sorted by name,
    context = genListStatusPartial(
        1, 0, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(ROOT_FILE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_BASE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_DIR_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_FILE2_URI.toString(), infos.get(0).getPath());
    assertTrue(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertEquals(1, infos.size());
    assertEquals(NESTED_TEST_FILE_URI.toString(), infos.get(0).getPath());
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    offset = infos.get(0).getFileId();

    context = genListStatusPartial(
        1, offset, true, "", "");
    infos = mFileSystemMaster.listStatus(ROOT_URI, context);
    assertFalse(context.isTruncated());
    assertEquals(-1, context.getTotalListings());
    assertEquals(0, infos.size());
  }

  @Test
  public void listStatusPartial() throws Exception {
    final int files = 13;
    final int batchSize = 5;
    List<FileInfo> infos;
    List<String> filenames;

    // Test files in root directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(ROOT_URI.join("file" + String.format("%05d", i)));
    }

    long offset = 0;
    for (int i = 0; i < files; i += batchSize) {
      ListStatusContext context = genListStatusPartial(batchSize, offset,
          false, "", "");
      infos = mFileSystemMaster.listStatus(ROOT_URI, context);
      assertEquals(files, context.getTotalListings());
      if (i + infos.size() == files) {
        assertFalse(context.isTruncated());
      } else {
        assertTrue(context.isTruncated());
      }
      // Copy out filenames to use List contains.
      filenames = infos.stream().map(FileInfo::getPath).collect(
          Collectors.toCollection(ArrayList::new));
      // Compare all filenames.
      for (int j = i; j < Math.min(i + batchSize, files); j++) {
        assertTrue(
            filenames.contains(ROOT_URI.join("file" + String.format("%05d", j))
                .toString()));
      }
      assertEquals(Math.min(i + batchSize, files) - i, filenames.size());
      // start from the offset
      offset = infos.get(infos.size() - 1).getFileId();
    }

    // test a non-existing file
    assertThrows(FileDoesNotExistException.class, () -> mFileSystemMaster.listStatus(
        new AlluxioURI("/doesNotExist"), genListStatusPartial(
            batchSize, 0, false, "", "")));
  }

  @Test
  public void listStatusPartialBatchRecursive() throws Exception {
    final int files = 13;
    final int batchSize = 5;
    final int depthSize = 5;
    List<FileInfo> infos;
    // we will start a listing from each depth
    // so for each depth there will be an arraylist of files that are at that depth or greater
    ArrayList<ArrayList<String>> filenames = new ArrayList<>(depthSize);
    for (int i = 0; i < depthSize; i++) {
      filenames.add(new ArrayList<>());
    }

    // First add number files in each depth
    ArrayList<String> parent = new ArrayList<>();
    for (int j = 0; j < depthSize; j++) {
      // The directory listing will be at the start of those with smaller depth
      for (int k = 0; k < j; k++) {
        filenames.get(k).add(parent.stream().skip(1).reduce("/", String::concat) + "nxt");
      }
      for (int i = 0; i < files; i++) {
        AlluxioURI nxt = new AlluxioURI(parent.stream().reduce(
            "/", String::concat) + "file" + String.format("%05d", i));
        // the file will be in the listing of every path that has an equal or smaller depth
        for (int k = 0; k <= j; k++) {
          filenames.get(k).add(nxt.getPath());
        }
        createFileWithSingleBlock(nxt);
      }
      parent.add("nxt/");
    }

    // Start a partial listing from each depth
    StringBuilder parentPath = new StringBuilder();
    for (int i = 0; i < depthSize; i++) {
      long offset = 0;
      ArrayList<String> myFileNames = filenames.get(i);
      // do the partial listing for each batch at this depth and be sure all files are listed
      for (int j = 0; j < myFileNames.size(); j += batchSize) {
        ListStatusContext context = genListStatusPartial(
            batchSize, offset, true, "", "");
        infos = mFileSystemMaster.listStatus(new AlluxioURI("/" + parentPath), context);
        assertEquals(Math.min(myFileNames.size() - j, batchSize), infos.size());
        assertEquals(-1, context.getTotalListings());
        if (j + infos.size() == myFileNames.size()) {
          assertFalse(context.isTruncated());
        } else {
          assertTrue(context.isTruncated());
        }
        for (int k = 0; k < infos.size(); k++) {
          assertEquals(myFileNames.get(k + j), infos.get(k).getPath());
        }
        offset = infos.get(infos.size() - 1).getFileId();
      }
      // there should be no more files to list
      ListStatusContext context = genListStatusPartial(batchSize, offset, true, "", "");
      infos = mFileSystemMaster.listStatus(new AlluxioURI("/" + parentPath), context);
      assertEquals(0, infos.size());
      assertFalse(context.isTruncated());
      parentPath.append("nxt/");
    }
  }

  @Test
  public void listStatusPartialBatch() throws Exception {
    final int files = 13;
    final int batchSize = 5;
    final int depthSize = 5;
    List<FileInfo> infos;
    List<String> filenames = new ArrayList<>();

    StringBuilder parent = new StringBuilder();
    for (int j = 0; j < depthSize; j++) {
      // Test files in root directory.
      for (int i = 0; i < files; i++) {
        AlluxioURI nxt = new AlluxioURI(parent + "/file" + String.format("%05d", i));
        createFileWithSingleBlock(nxt);
      }
      parent.append("/nxt");
    }

    // go through each file without recursion
    parent = new StringBuilder();
    for (int j = 0; j < depthSize; j++) {
      long offset = 0;
      // the number of remaining files to list for this directory
      int remain;
      long listingCount;
      if (j == depthSize - 1) {
        remain = files;
        listingCount = files;
      } else {
        // +1 for the nested directory
        remain = files + 1;
        listingCount = files + 1;
      }
      for (int i = 0; i < files; i += batchSize) {
        ListStatusContext context = genListStatusPartial(
            batchSize, offset, false, "", "");
        infos = mFileSystemMaster.listStatus(new AlluxioURI("/" + parent), context);
        assertEquals(listingCount, context.getTotalListings());
        if (remain > batchSize) {
          assertTrue(context.isTruncated());
        } else {
          assertFalse(context.isTruncated());
        }
        // Copy out filenames to use List contains.
        filenames = infos.stream().map(FileInfo::getPath).collect(
            Collectors.toCollection(ArrayList::new));
        // check all the remaining files are listed
        assertEquals(Math.min(batchSize, remain), filenames.size());
        for (int k = i; k < Math.min(files, i + batchSize); k++) {
          assertTrue(filenames.contains("/" + parent + "file" + String.format("%05d", k)));
        }
        offset = infos.get(infos.size() - 1).getFileId();
        remain -= batchSize;
      }
      // listing of the nested directory
      if (j != depthSize - 1) {
        assertEquals("/" + parent + "nxt", filenames.get(filenames.size() - 1));
      }
      // all files should have been listed
      ListStatusContext context = genListStatusPartial(
          batchSize, offset, false, "", "");
      infos = mFileSystemMaster.listStatus(new AlluxioURI("/" + parent), context);
      assertFalse(context.isTruncated());
      assertEquals(0, infos.size());

      parent.append("nxt/");
    }
  }

  @Test
  public void listStatusRecursive() throws Exception {
    final int files = 10;
    List<FileInfo> infos;
    List<String> filenames;

    // Test files in root directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(ROOT_URI.join("file" + String.format("%05d", i)));
    }
    // Test files in nested directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(NESTED_URI.join("file" + String.format("%05d", i)));
    }

    // Test recursive listStatus
    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).setRecursive(true)));
    // 10 files in each directory, 2 levels of directories
    assertEquals(files + files + 2, infos.size());

    filenames = new ArrayList<>();
    for (FileInfo info : infos) {
      filenames.add(info.getPath());
    }
    for (int i = 0; i < files; i++) {
      assertTrue(
          filenames.contains(ROOT_URI.join("file" + String.format("%05d", i)).toString()));
    }
    for (int i = 0; i < files; i++) {
      assertTrue(
          filenames.contains(NESTED_URI.join("file" + String.format("%05d", i)).toString()));
    }
  }

  @Test
  public void listStatusPermissions() throws Exception {
    List<FileInfo> infos;
    // create a single file
    createFileWithSingleBlock(ROOT_URI.join("file"));
    // create a nested file
    createFileWithSingleBlock(NESTED_URI);
    // list the files
    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).setRecursive(true)));
    assertEquals(3, infos.size());
    assertEquals(ROOT_URI.join("file").getPath(), infos.get(0).getPath());
    assertEquals(NESTED_BASE_URI.getPath(), infos.get(1).getPath());
    assertEquals(NESTED_URI.getPath(), infos.get(2).getPath());

    // change the permissions of all the files and directories
    mFileSystemMaster.setAttribute(ROOT_URI, SetAttributeContext.mergeFrom(SetAttributePOptions
        .newBuilder().setMode(new Mode((short) 0400).toProto()).setRecursive(true)));
    // allow us to list the root directory
    mFileSystemMaster.setAttribute(ROOT_URI, SetAttributeContext.mergeFrom(SetAttributePOptions
        .newBuilder().setMode(new Mode((short) 0555).toProto()).setRecursive(false)));

    // we should be able to list everything except the files inside the nested directory
    try (Closeable r = new AuthenticatedUserRule("test_user1", Configuration.global())
        .toResource()) {
      // Test recursive listStatus
      infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
          .newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).setRecursive(true)));
      assertEquals(2, infos.size());
      assertEquals(ROOT_URI.join("file").getPath(), infos.get(0).getPath());
      assertEquals(NESTED_BASE_URI.getPath(), infos.get(1).getPath());
    }
  }

  @Test
  public void listStatusRecursivePermissions() throws Exception {
    final int files = 10;
    List<FileInfo> infos;

    // Test files in root directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(ROOT_URI.join("file" + String.format("%05d", i)));
    }
    // Test files in nested directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(NESTED_URI.join("file" + String.format("%05d", i)));
    }
    // Test recursive listStatus without permissions
    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).setRecursive(true)));
    // 10 files in each directory, 2 levels of directories
    assertEquals(2 * (files + 1), infos.size());

    // Test with permissions
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext.mergeFrom(SetAttributePOptions
        .newBuilder().setMode(new Mode((short) 0400).toProto()).setRecursive(true)));
    try (Closeable r = new AuthenticatedUserRule("test_user1", Configuration.global())
        .toResource()) {
      // Test recursive listStatus
      infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
          .newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).setRecursive(true)));

      // 10 files in the root directory, 2 level of directories
      assertEquals(files + 2, infos.size());
    }
  }

  @Test
  public void listStatusRecursiveLoadMetadata() throws Exception {
    final int files = 10;
    List<FileInfo> infos;

    // Test files in root directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(ROOT_URI.join("file" + String.format("%05d", i)));
    }

    FileUtils.createFile(Paths.get(mUnderFS).resolve("ufsfile1").toString());
    // Test interaction between recursive and loadMetadata
    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE).setRecursive(false)));

    assertEquals(files + 1  , infos.size());

    FileUtils.createFile(Paths.get(mUnderFS).resolve("ufsfile2").toString());
    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE).setRecursive(false)));
    assertEquals(files + 1  , infos.size());

    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).setRecursive(false)));
    assertEquals(files + 2, infos.size());

    // Test files in nested directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(NESTED_URI.join("file" + String.format("%05d", i)));
    }

    FileUtils.createFile(Paths.get(mUnderFS).resolve("nested/test/ufsnestedfile1").toString());
    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE).setRecursive(true)));
    // 2 sets of files, 2 files inserted at root, 2 directories nested and test,
    // 1 file ufsnestedfile1
    assertEquals(files + files + 2 + 2 + 1, infos.size());

    FileUtils.createFile(Paths.get(mUnderFS).resolve("nested/test/ufsnestedfile2").toString());
    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE).setRecursive(true)));
    assertEquals(files + files + 2 + 2 + 1, infos.size());

    infos = mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
        .newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS).setRecursive(true)));
    assertEquals(files + files + 2 + 2 + 2, infos.size());
  }

  @Test
  public void getFileBlockInfoList() throws Exception {
    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);

    List<FileBlockInfo> blockInfo;

    blockInfo = mFileSystemMaster.getFileBlockInfoList(ROOT_FILE_URI);
    assertEquals(1, blockInfo.size());

    blockInfo = mFileSystemMaster.getFileBlockInfoList(NESTED_FILE_URI);
    assertEquals(1, blockInfo.size());

    // Test directory URI.
    try {
      mFileSystemMaster.getFileBlockInfoList(NESTED_URI);
      fail("getFileBlockInfoList() for a directory URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }

    // Test non-existent URI.
    try {
      mFileSystemMaster.getFileBlockInfoList(TEST_URI);
      fail("getFileBlockInfoList() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  @Test
  public void mountUnmount() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryContext.defaults());

    // Alluxio mount point should not exist before mounting.
    try {
      mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local"), GET_STATUS_CONTEXT);
      fail("getFileInfo() for a non-existent URI (before mounting) should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }

    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount,
        MountContext.defaults());
    // Alluxio mount point should exist after mounting.
    assertNotNull(
        mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local"), GET_STATUS_CONTEXT));

    mFileSystemMaster.unmount(new AlluxioURI("/mnt/local"));

    // Alluxio mount point should not exist after unmounting.
    try {
      mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local"), GET_STATUS_CONTEXT);
      fail("getFileInfo() for a non-existent URI (after mounting) should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  @Test
  public void setDefaultAclforFile() throws Exception {
    SetAclContext context = SetAclContext.defaults();
    createFileWithSingleBlock(NESTED_FILE_URI);

    Set<String> newEntries = Sets.newHashSet("default:user::rwx",
        "default:group::rwx", "default:other::r-x");

    mThrown.expect(UnsupportedOperationException.class);
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);
  }

  @Test
  public void setDefaultAcl() throws Exception {
    SetAclContext context = SetAclContext.defaults();
    createFileWithSingleBlock(NESTED_FILE_URI);
    Set<String> entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertEquals(0, entries.size());

    // replace
    Set<String> newEntries = Sets.newHashSet("default:user::rwx",
        "default:group::rwx", "default:other::r-x");
    mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.REPLACE,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertEquals(newEntries, entries);

    // replace
    newEntries = Sets.newHashSet("default:user::rw-", "default:group::r--", "default:other::r--");
    mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.REPLACE,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);
    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertEquals(newEntries, entries);

    // modify existing
    newEntries = Sets.newHashSet("default:user::rwx", "default:group::rw-", "default:other::r-x");
    mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertEquals(newEntries, entries);

    // modify add
    Set<String> oldEntries = new HashSet<>(entries);
    newEntries = Sets.newHashSet("default:user:usera:---", "default:group:groupa:--x");
    mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertTrue(entries.containsAll(oldEntries));
    assertTrue(entries.containsAll(newEntries));
    assertTrue(entries.contains("default:mask::rwx"));

    // modify existing and add
    newEntries = Sets.newHashSet("default:user:usera:---", "default:group:groupa:--x",
        "default:other::r-x");
    mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertTrue(entries.containsAll(newEntries));

    // remove default
    mFileSystemMaster
        .setAcl(NESTED_URI, SetAclAction.REMOVE_DEFAULT, Collections.emptyList(), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertEquals(0, entries.size());

    // remove
    newEntries =
        Sets.newHashSet("default:user:usera:---", "default:user:userb:rwx",
            "default:group:groupa:--x", "default:group:groupb:-wx");
    mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);
    oldEntries = new HashSet<>(entries);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    assertTrue(entries.containsAll(oldEntries));

    Set<String> deleteEntries = Sets.newHashSet("default:user:userb:rwx",
        "default:group:groupa:--x");
    mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.REMOVE,
        deleteEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).convertDefaultAclToStringEntries());
    Set<String> remainingEntries = new HashSet<>(newEntries);
    assertTrue(remainingEntries.removeAll(deleteEntries));
    assertTrue(entries.containsAll(remainingEntries));

    final Set<String> finalEntries = entries;
    assertTrue(deleteEntries.stream().noneMatch(finalEntries::contains));
  }

  @Test
  public void setAcl() throws Exception {
    SetAclContext context = SetAclContext.defaults();
    createFileWithSingleBlock(NESTED_FILE_URI);

    Set<String> entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertEquals(3, entries.size());

    // replace
    Set<String> newEntries = Sets.newHashSet("user::rwx", "group::rwx", "other::rwx");
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.REPLACE,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertEquals(newEntries, entries);

    // replace
    newEntries = Sets.newHashSet("user::rw-", "group::r--", "other::r--");
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.REPLACE,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertEquals(newEntries, entries);

    // modify existing
    newEntries = Sets.newHashSet("user::rwx", "group::r--", "other::r-x");
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertEquals(newEntries, entries);

    // modify add
    Set<String> oldEntries = new HashSet<>(entries);
    newEntries = Sets.newHashSet("user:usera:---", "group:groupa:--x");
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertTrue(entries.containsAll(oldEntries));
    assertTrue(entries.containsAll(newEntries));
    // check if the mask got updated correctly
    assertTrue(entries.contains("mask::r-x"));

    // modify existing and add
    newEntries = Sets.newHashSet("user:usera:---", "group:groupa:--x", "other::r-x");
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertTrue(entries.containsAll(newEntries));

    // remove all
    mFileSystemMaster
        .setAcl(NESTED_FILE_URI, SetAclAction.REMOVE_ALL, Collections.emptyList(), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertEquals(3, entries.size());

    // remove
    newEntries =
        Sets.newHashSet("user:usera:---", "user:userb:rwx", "group:groupa:--x", "group:groupb:-wx");
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.MODIFY,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);
    oldEntries = new HashSet<>(entries);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertTrue(entries.containsAll(oldEntries));

    Set<String> deleteEntries = Sets.newHashSet("user:userb:rwx", "group:groupa:--x");
    mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.REMOVE,
        deleteEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    Set<String> remainingEntries = new HashSet<>(newEntries);
    assertTrue(remainingEntries.removeAll(deleteEntries));
    assertTrue(entries.containsAll(remainingEntries));

    final Set<String> finalEntries = entries;
    assertTrue(deleteEntries.stream().noneMatch(finalEntries::contains));
  }

  @Test
  public void setRecursiveAcl() throws Exception {
    final int files = 10;
    SetAclContext context =
        SetAclContext.mergeFrom(SetAclPOptions.newBuilder().setRecursive(true));

    // Test files in root directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(ROOT_URI.join("file" + String.format("%05d", i)));
    }
    // Test files in nested directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(NESTED_URI.join("file" + String.format("%05d", i)));
    }

    // Test files in nested directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(NESTED_DIR_URI.join("file" + String.format("%05d", i)));
    }

    // replace
    Set<String> newEntries = Sets.newHashSet("user::rw-", "group::r-x", "other::-wx");
    mFileSystemMaster.setAcl(ROOT_URI, SetAclAction.REPLACE,
        newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()), context);

    List<FileInfo> infos =
        mFileSystemMaster.listStatus(ROOT_URI, ListStatusContext.mergeFrom(ListStatusPOptions
            .newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE).setRecursive(true)));
    assertEquals(files * 3 + 3, infos.size());
    for (FileInfo info : infos) {
      assertEquals(newEntries, Sets.newHashSet(info.convertAclToStringEntries()));
    }
  }

  @Test
  public void inheritExtendedDefaultAcl() throws Exception {
    AlluxioURI dir = new AlluxioURI("/dir");
    mFileSystemMaster.createDirectory(dir, CreateDirectoryContext.defaults());
    String aclString = "default:user:foo:-w-";
    mFileSystemMaster.setAcl(dir, SetAclAction.MODIFY,
        Arrays.asList(AclEntry.fromCliString(aclString)), SetAclContext.defaults());
    AlluxioURI inner = new AlluxioURI("/dir/inner");
    mFileSystemMaster.createDirectory(inner, CreateDirectoryContext.defaults());
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(inner, GetStatusContext.defaults());
    List<String> accessEntries = fileInfo.getAcl().toStringEntries();
    assertTrue(accessEntries.toString(), accessEntries.contains("user:foo:-w-"));
    List<String> defaultEntries = fileInfo.getDefaultAcl().toStringEntries();
    assertTrue(defaultEntries.toString(), defaultEntries.contains(aclString));
  }

  @Test
  public void inheritNonExtendedDefaultAcl() throws Exception {
    AlluxioURI dir = new AlluxioURI("/dir");
    mFileSystemMaster.createDirectory(dir, CreateDirectoryContext.defaults());
    String aclString = "default:user::-w-";
    mFileSystemMaster.setAcl(dir, SetAclAction.MODIFY,
        Arrays.asList(AclEntry.fromCliString(aclString)), SetAclContext.defaults());
    AlluxioURI inner = new AlluxioURI("/dir/inner");
    mFileSystemMaster.createDirectory(inner, CreateDirectoryContext.defaults());
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(inner, GetStatusContext.defaults());
    List<String> accessEntries = fileInfo.getAcl().toStringEntries();
    assertTrue(accessEntries.toString(), accessEntries.contains("user::-w-"));
    List<String> defaultEntries = fileInfo.getDefaultAcl().toStringEntries();
    assertTrue(defaultEntries.toString(), defaultEntries.contains(aclString));
  }

  @Test
  public void setAclWithoutOwner() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    Set<String> entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertEquals(3, entries.size());

    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      Set<String> newEntries = Sets.newHashSet("user::rwx", "group::rwx", "other::rwx");
      mThrown.expect(AccessControlException.class);
      mFileSystemMaster.setAcl(NESTED_FILE_URI, SetAclAction.REPLACE,
          newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()),
          SetAclContext.defaults());
    }
  }

  @Test
  public void setAclNestedWithoutOwner() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext.mergeFrom(SetAttributePOptions
        .newBuilder().setMode(new Mode((short) 0777).toProto()).setOwner("userA")));
    Set<String> entries = Sets.newHashSet(mFileSystemMaster
        .getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).convertAclToStringEntries());
    assertEquals(3, entries.size());
    // recursive setAcl should fail if one of the child is not owned by the user
    mThrown.expect(AccessControlException.class);
    try (AuthenticatedClientUserResource userA = new AuthenticatedClientUserResource("userA",
        Configuration.global())) {
      Set<String> newEntries = Sets.newHashSet("user::rwx", "group::rwx", "other::rwx");
      mFileSystemMaster.setAcl(NESTED_URI, SetAclAction.REPLACE,
          newEntries.stream().map(AclEntry::fromCliString).collect(Collectors.toList()),
          SetAclContext.mergeFrom(SetAclPOptions.newBuilder().setRecursive(true)));
      entries = Sets.newHashSet(mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT)
          .convertAclToStringEntries());
      assertEquals(newEntries, entries);
    }
  }

  @Test
  public void removeExtendedAclMask() throws Exception {
    mFileSystemMaster.createDirectory(NESTED_URI, CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
    AclEntry newAcl = AclEntry.fromCliString("user:newuser:rwx");
    // Add an ACL
    addAcl(NESTED_URI, newAcl);
    assertThat(getInfo(NESTED_URI).getAcl().getEntries(), hasItem(newAcl));

    // Attempt to remove the ACL mask
    AclEntry maskEntry = AclEntry.fromCliString("mask::rwx");
    assertThat(getInfo(NESTED_URI).getAcl().getEntries(), hasItem(maskEntry));
    try {
      removeAcl(NESTED_URI, maskEntry);
      fail("Expected removing the mask from an extended ACL to fail");
    } catch (IOException e) {
      assertThat(e.getMessage(), containsString("mask"));
    }

    // Remove the extended ACL
    removeAcl(NESTED_URI, newAcl);
    // Now we can add and remove a mask
    addAcl(NESTED_URI, maskEntry);
    removeAcl(NESTED_URI, maskEntry);
  }

  @Test
  public void removeExtendedDefaultAclMask() throws Exception {
    mFileSystemMaster.createDirectory(NESTED_URI, CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
    AclEntry newAcl = AclEntry.fromCliString("default:user:newuser:rwx");
    // Add an ACL
    addAcl(NESTED_URI, newAcl);
    assertThat(getInfo(NESTED_URI).getDefaultAcl().getEntries(), hasItem(newAcl));

    // Attempt to remove the ACL mask
    AclEntry maskEntry = AclEntry.fromCliString("default:mask::rwx");
    assertThat(getInfo(NESTED_URI).getDefaultAcl().getEntries(), hasItem(maskEntry));
    try {
      removeAcl(NESTED_URI, maskEntry);
      fail("Expected removing the mask from an extended ACL to fail");
    } catch (IOException e) {
      assertThat(e.getMessage(), containsString("mask"));
    }

    // Remove the extended ACL
    removeAcl(NESTED_URI, newAcl);
    // Now we can add and remove a mask
    addAcl(NESTED_URI, maskEntry);
    removeAcl(NESTED_URI, maskEntry);
  }

  private void addAcl(AlluxioURI uri, AclEntry acl) throws Exception {
    mFileSystemMaster.setAcl(uri, SetAclAction.MODIFY, Arrays.asList(acl),
        SetAclContext.defaults());
  }

  private void removeAcl(AlluxioURI uri, AclEntry acl) throws Exception {
    mFileSystemMaster.setAcl(uri, SetAclAction.REMOVE, Arrays.asList(acl),
        SetAclContext.defaults());
  }

  private FileInfo getInfo(AlluxioURI uri) throws Exception {
    return mFileSystemMaster.getFileInfo(uri, GetStatusContext.defaults());
  }

  /**
   * Tests that an exception is in the
   * {@link FileSystemMaster#createFile(AlluxioURI, CreateFileContext)} with a
   * TTL set in the {@link CreateFileContext} after the TTL check was done once.
   */
  @Test
  public void ttlFileDelete() throws Exception {
    CreateFileContext context = CreateFileContext.defaults();
    context.getOptions().setBlockSizeBytes(Constants.KB);
    context.getOptions().setRecursive(true);
    context.getOptions().setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0));
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, context).getFileId();
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    assertEquals(fileInfo.getFileId(), fileId);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * Tests that TTL delete of a file is not forgotten across restarts.
   */
  @Test
  public void ttlFileDeleteReplay() throws Exception {
    CreateFileContext context = CreateFileContext.defaults();
    context.getOptions().setBlockSizeBytes(Constants.KB);
    context.getOptions().setRecursive(true);
    context.getOptions().setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0));
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, context).getFileId();

    // Simulate restart.
    stopServices();
    startServices();

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    assertEquals(fileInfo.getFileId(), fileId);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * Tests that an exception is in the
   * {@literal FileSystemMaster#createDirectory(AlluxioURI, CreateDirectoryOptions)}
   * with a TTL set in the {@link CreateDirectoryContext} after the TTL check was done once.
   */
  @Test
  public void ttlDirectoryDelete() throws Exception {
    CreateDirectoryContext context =
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0)));
    long dirId = mFileSystemMaster.createDirectory(NESTED_DIR_URI, context);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(dirId);
    assertEquals(fileInfo.getFileId(), dirId);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(dirId);
  }

  /**
   * Tests that TTL delete of a directory is not forgotten across restarts.
   */
  @Test
  public void ttlDirectoryDeleteReplay() throws Exception {
    CreateDirectoryContext context =
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0)));
    long dirId = mFileSystemMaster.createDirectory(NESTED_DIR_URI, context);

    // Simulate restart.
    stopServices();
    startServices();

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(dirId);
    assertEquals(fileInfo.getFileId(), dirId);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(dirId);
  }

  /**
   * Tests that file information is still present after it has been freed after the TTL has been set
   * to 0.
   */
  @Test
  public void ttlFileFree() throws Exception {
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());
    // Set ttl & operation.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext.mergeFrom(
        SetAttributePOptions.newBuilder().setCommonOptions(FileSystemOptions
            .commonDefaults(Configuration.global()).toBuilder().setTtl(0)
            .setTtlAction(alluxio.grpc.TtlAction.FREE))));
    Command heartbeat = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests that TTL free of a file is not forgotten across restarts.
   */
  @Test
  public void ttlFileFreeReplay() throws Exception {
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());
    // Set ttl & operation.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext.mergeFrom(
        SetAttributePOptions.newBuilder().setCommonOptions(FileSystemOptions
            .commonDefaults(Configuration.global()).toBuilder().setTtl(0)
            .setTtlAction(alluxio.grpc.TtlAction.FREE))));
    // Simulate restart.
    stopServices();
    startServices();

    Command heartbeat = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.<String, StorageList>of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests that file information is still present after it has been freed after the parent
   * directory's TTL has been set to 0.
   */
  @Test
  public void ttlDirectoryFree() throws Exception {
    CreateDirectoryContext directoryContext = CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true));
    mFileSystemMaster.createDirectory(NESTED_URI, directoryContext);
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());
    // Set ttl & operation.
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext.mergeFrom(
        SetAttributePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setTtl(0).setTtlAction(alluxio.grpc.TtlAction.FREE))));
    Command heartbeat = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.<String, StorageList>of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests that TTL free of a directory is not forgotten across restarts.
   */
  @Test
  public void ttlDirectoryFreeReplay() throws Exception {
    CreateDirectoryContext directoryContext = CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true));
    mFileSystemMaster.createDirectory(NESTED_URI, directoryContext);
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());
    // Set ttl & operation.
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext.mergeFrom(
        SetAttributePOptions.newBuilder().setCommonOptions(FileSystemOptions
            .commonDefaults(Configuration.global()).toBuilder().setTtl(0)
            .setTtlAction(alluxio.grpc.TtlAction.FREE))));

    // Simulate restart.
    stopServices();
    startServices();

    Command heartbeat = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests that an exception is thrown when trying to get information about a file after it
   * has been deleted because of a TTL of 0.
   */
  @Test
  public void setTtlForFileWithNoTtl() throws Exception {
    CreateFileContext context = CreateFileContext.mergeFrom(
        CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB).setRecursive(true));
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, context).getFileId();
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // Since no TTL is set, the file should not be deleted.
    assertEquals(fileId,
        mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).getFileId());

    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // TTL is set to 0, the file should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * Tests that an exception is thrown when trying to get information about a Directory after
   * it has been deleted because of a TTL of 0.
   */
  @Test
  public void setTtlForDirectoryWithNoTtl() throws Exception {
    CreateDirectoryContext directoryContext = CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true));
    mFileSystemMaster.createDirectory(NESTED_URI, directoryContext);
    mFileSystemMaster.createDirectory(NESTED_DIR_URI, directoryContext);
    CreateFileContext createFileContext = CreateFileContext.mergeFrom(
        CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB).setRecursive(true));
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, createFileContext).getFileId();
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // Since no TTL is set, the file should not be deleted.
    assertEquals(fileId,
        mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).getFileId());
    // Set ttl.
    mFileSystemMaster.setAttribute(NESTED_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // TTL is set to 0, the file and directory should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT);
    mFileSystemMaster.getFileInfo(NESTED_DIR_URI, GET_STATUS_CONTEXT);
    mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT);
  }

  /**
   * Tests that an exception is thrown when trying to get information about a file after it
   * has been deleted after the TTL has been set to 0.
   */
  @Test
  public void setSmallerTtlForFileWithTtl() throws Exception {
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(Constants.HOUR_MS))
        .setBlockSizeBytes(Constants.KB).setRecursive(true));
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, context).getFileId();
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // Since TTL is 1 hour, the file won't be deleted during last TTL check.
    assertEquals(fileId,
        mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).getFileId());

    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // TTL is reset to 0, the file should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * Tests that an exception is thrown when trying to get information about a Directory after
   * it has been deleted after the TTL has been set to 0.
   */
  @Test
  public void setSmallerTtlForDirectoryWithTtl() throws Exception {
    CreateDirectoryContext directoryContext =
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(Constants.HOUR_MS))
            .setRecursive(true));
    mFileSystemMaster.createDirectory(NESTED_URI, directoryContext);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    assertTrue(
        mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).getName() != null);
    mFileSystemMaster.setAttribute(NESTED_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // TTL is reset to 0, the file should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT);
  }

  /**
   * Tests that a file has not been deleted after the TTL has been reset to a valid value.
   */
  @Test
  public void setLargerTtlForFileWithTtl() throws Exception {
    mFileSystemMaster.createDirectory(NESTED_URI, CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))
        .setBlockSizeBytes(Constants.KB).setRecursive(true));
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, context).getFileId();
    assertEquals(fileId,
        mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).getFileId());

    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setTtl(Constants.HOUR_MS))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // TTL is reset to 1 hour, the file should not be deleted during last TTL check.
    assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).getFileId());
  }

  /**
   * Tests that a directory has not been deleted after the TTL has been reset to a valid value.
   */
  @Test
  public void setLargerTtlForDirectoryWithTtl() throws Exception {
    mFileSystemMaster.createDirectory(new AlluxioURI("/nested"),
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
    mFileSystemMaster.createDirectory(NESTED_URI,
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))
            .setRecursive(true)));
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setTtl(Constants.HOUR_MS))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // TTL is reset to 1 hour, the directory should not be deleted during last TTL check.
    assertEquals(NESTED_URI.getName(),
        mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).getName());
  }

  /**
   * Tests that the original TTL is removed after setting it to {@link Constants#NO_TTL} for a file.
   */
  @Test
  public void setNoTtlForFileWithTtl() throws Exception {
    mFileSystemMaster.createDirectory(NESTED_URI, CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))
        .setBlockSizeBytes(Constants.KB).setRecursive(true));
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, context).getFileId();
    // After setting TTL to NO_TTL, the original TTL will be removed, and the file will not be
    // deleted during next TTL check.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setTtl(Constants.NO_TTL))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).getFileId());
  }

  /**
   * Tests that the original TTL is removed after setting it to {@link Constants#NO_TTL} for
   * a directory.
   */
  @Test
  public void setNoTtlForDirectoryWithTtl() throws Exception {
    mFileSystemMaster.createDirectory(new AlluxioURI("/nested"),
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)));
    mFileSystemMaster.createDirectory(NESTED_URI,
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(0))
            .setRecursive(true)));
    // After setting TTL to NO_TTL, the original TTL will be removed, and the file will not be
    // deleted during next TTL check.
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setTtl(Constants.NO_TTL))));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    assertEquals(NESTED_URI.getName(),
        mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).getName());
  }

  /**
   * Tests the {@link FileSystemMaster#setAttribute(AlluxioURI, SetAttributeContext)} method and
   * that an exception is thrown when trying to set a TTL for a directory.
   */
  @Test
  public void setAttribute() throws Exception {
    mFileSystemMaster.createFile(NESTED_FILE_URI, mNestedFileContext);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT);
    assertFalse(fileInfo.isPinned());
    assertEquals(Constants.NO_TTL, fileInfo.getTtl());

    // No State.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeContext.defaults());
    fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT);
    assertFalse(fileInfo.isPinned());
    assertEquals(Constants.NO_TTL, fileInfo.getTtl());

    // Just set pinned flag.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setPinned(true)));
    fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT);
    assertTrue(fileInfo.isPinned());
    assertEquals(Constants.NO_TTL, fileInfo.getTtl());

    // Both pinned flag and ttl value.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setPinned(false)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setTtl(1))));
    fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT);
    assertFalse(fileInfo.isPinned());
    assertEquals(1, fileInfo.getTtl());

    mFileSystemMaster.setAttribute(NESTED_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
            .setTtl(1))));
  }

  /**
   * Tests the permission bits are 0777 for directories and 0666 for files with UMASK 000.
   */
  @Test
  public void permission() throws Exception {
    mFileSystemMaster.createFile(NESTED_FILE_URI, mNestedFileContext);
    assertEquals(0777,
        mFileSystemMaster.getFileInfo(NESTED_URI, GET_STATUS_CONTEXT).getMode());
    assertEquals(0666,
        mFileSystemMaster.getFileInfo(NESTED_FILE_URI, GET_STATUS_CONTEXT).getMode());
  }

  /**
   * Tests that a file is fully written to memory.
   */
  @Test
  public void isFullyInMemory() throws Exception {
    // add nested file
    mFileSystemMaster.createFile(NESTED_FILE_URI, mNestedFileContext);
    // add in-memory block
    long blockId = mFileSystemMaster.getNewBlockIdForFile(NESTED_FILE_URI);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB,
        Constants.MEDIUM_MEM, Constants.MEDIUM_MEM, blockId, Constants.KB);
    // add SSD block
    blockId = mFileSystemMaster.getNewBlockIdForFile(NESTED_FILE_URI);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB, Constants.MEDIUM_SSD,
        Constants.MEDIUM_SSD, blockId, Constants.KB);
    mFileSystemMaster.completeFile(NESTED_FILE_URI, CompleteFileContext.defaults());

    // Create 2 files in memory.
    createFileWithSingleBlock(ROOT_FILE_URI);
    AlluxioURI nestedMemUri = NESTED_URI.join("mem_file");
    createFileWithSingleBlock(nestedMemUri);
    assertEquals(2, mFileSystemMaster.getInMemoryFiles().size());
    assertTrue(mFileSystemMaster.getInMemoryFiles().contains(ROOT_FILE_URI));
    assertTrue(mFileSystemMaster.getInMemoryFiles().contains(nestedMemUri));
  }

  /**
   * Tests the {@link FileSystemMaster#rename(AlluxioURI, AlluxioURI, RenameContext)} method.
   */
  @Test
  public void rename() throws Exception {
    mFileSystemMaster.createFile(NESTED_FILE_URI, mNestedFileContext);

    // try to rename a file to root
    try {
      mFileSystemMaster.rename(NESTED_FILE_URI, ROOT_URI, RenameContext.defaults());
      fail("Renaming to root should fail.");
    } catch (InvalidPathException e) {
      assertEquals(ExceptionMessage.RENAME_CANNOT_BE_TO_ROOT.getMessage(), e.getMessage());
    }

    // move root to another path
    try {
      mFileSystemMaster.rename(ROOT_URI, TEST_URI, RenameContext.defaults());
      fail("Should not be able to rename root");
    } catch (InvalidPathException e) {
      assertEquals(ExceptionMessage.ROOT_CANNOT_BE_RENAMED.getMessage(), e.getMessage());
    }

    // move to existing path
    try {
      mFileSystemMaster.rename(NESTED_FILE_URI, NESTED_URI, RenameContext.defaults());
      fail("Should not be able to overwrite existing file.");
    } catch (FileAlreadyExistsException e) {
      assertEquals(String
          .format("Cannot rename because destination already exists. src: %s dst: %s",
              NESTED_FILE_URI.getPath(), NESTED_URI.getPath()), e.getMessage());
    }

    // move a nested file to a root file
    mFileSystemMaster.rename(NESTED_FILE_URI, TEST_URI, RenameContext.defaults());
    assertEquals(mFileSystemMaster.getFileInfo(TEST_URI, GET_STATUS_CONTEXT).getPath(),
        TEST_URI.getPath());

    // move a file where the dst is lexicographically earlier than the source
    AlluxioURI newDst = new AlluxioURI("/abc_test");
    mFileSystemMaster.rename(TEST_URI, newDst, RenameContext.defaults());
    assertEquals(mFileSystemMaster.getFileInfo(newDst, GET_STATUS_CONTEXT).getPath(),
        newDst.getPath());
  }

  /**
   * Tests that an exception is thrown when trying to create a file in a non-existing directory
   * without setting the {@code recursive} flag.
   */
  @Test
  public void renameUnderNonexistingDir() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/nested/test"));
    CreateFileContext context = CreateFileContext
        .mergeFrom(CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB));
    mFileSystemMaster.createFile(TEST_URI, context);

    // nested dir
    mFileSystemMaster.rename(TEST_URI, NESTED_FILE_URI, RenameContext.defaults());
  }

  @Test
  public void renameToNonExistentParent() throws Exception {
    CreateFileContext context = CreateFileContext.mergeFrom(
        CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB).setRecursive(true));
    mFileSystemMaster.createFile(NESTED_URI, context);

    try {
      mFileSystemMaster.rename(NESTED_URI, new AlluxioURI("/testDNE/b"), RenameContext.defaults());
      fail("Rename to a non-existent parent path should not succeed.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  /**
   * Tests that an exception is thrown when trying to rename a file to a prefix of the original
   * file.
   */
  @Test
  public void renameToSubpath() throws Exception {
    mFileSystemMaster.createFile(NESTED_URI, mNestedFileContext);
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Traversal failed for path /nested/test/file. "
        + "Component 2(test) is a file, not a directory");
    mFileSystemMaster.rename(NESTED_URI, NESTED_FILE_URI, RenameContext.defaults());
  }

  /**
   * Tests {@link FileSystemMaster#free} on persisted file.
   */
  @Test
  public void free() throws Exception {
    mNestedFileContext.setWriteType(WriteType.CACHE_THROUGH);
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // free the file
    mFileSystemMaster.free(NESTED_FILE_URI, FreeContext.mergeFrom(FreePOptions.newBuilder()
        .setForced(false).setRecursive(false)));
    // Update the heartbeat of removedBlockId received from worker 1.
    Command heartbeat2 = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat2);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests {@link FileSystemMaster#free} on non-persisted file.
   */
  @Test
  public void freeNonPersistedFile() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    mThrown.expect(UnexpectedAlluxioException.class);
    mThrown.expectMessage(ExceptionMessage.CANNOT_FREE_NON_PERSISTED_FILE
        .getMessage(NESTED_FILE_URI.getPath()));
    // cannot free a non-persisted file
    mFileSystemMaster.free(NESTED_FILE_URI, FreeContext.defaults());
  }

  /**
   * Tests {@link FileSystemMaster#free} on pinned file when forced flag is false.
   */
  @Test
  public void freePinnedFileWithoutForce() throws Exception {
    mNestedFileContext.setWriteType(WriteType.CACHE_THROUGH);
    createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setPinned(true)));
    mThrown.expect(UnexpectedAlluxioException.class);
    mThrown.expectMessage(ExceptionMessage.CANNOT_FREE_PINNED_FILE
        .getMessage(NESTED_FILE_URI.getPath()));
    // cannot free a pinned file without "forced"
    mFileSystemMaster.free(NESTED_FILE_URI, FreeContext.defaults());
  }

  /**
   * Tests {@link FileSystemMaster#free} on pinned file when forced flag is true.
   */
  @Test
  public void freePinnedFileWithForce() throws Exception {
    mNestedFileContext.setWriteType(WriteType.CACHE_THROUGH);
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setPinned(true)));

    assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // free the file
    mFileSystemMaster.free(NESTED_FILE_URI,
        FreeContext.mergeFrom(FreePOptions.newBuilder().setForced(true)));
    // Update the heartbeat of removedBlockId received from worker 1.
    Command heartbeat = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests the {@link FileSystemMaster#free} method with a directory but recursive to false.
   */
  @Test
  public void freeDirNonRecursive() throws Exception {
    mNestedFileContext.setWriteType(WriteType.CACHE_THROUGH);
    createFileWithSingleBlock(NESTED_FILE_URI);
    mThrown.expect(UnexpectedAlluxioException.class);
    mThrown.expectMessage(ExceptionMessage.CANNOT_FREE_NON_EMPTY_DIR.getMessage(NESTED_URI));
    // cannot free directory with recursive argument to false
    mFileSystemMaster.free(NESTED_FILE_URI.getParent(),
        FreeContext.mergeFrom(FreePOptions.newBuilder().setRecursive(false)));
  }

  /**
   * Tests the {@link FileSystemMaster#free} method with a directory.
   */
  @Test
  public void freeDir() throws Exception {
    mNestedFileContext.setWriteType(WriteType.CACHE_THROUGH);
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // free the dir
    mFileSystemMaster.free(NESTED_FILE_URI.getParent(),
        FreeContext.mergeFrom(FreePOptions.newBuilder().setForced(true).setRecursive(true)));
    // Update the heartbeat of removedBlockId received from worker 1.
    Command heartbeat3 = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat3);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests the {@link FileSystemMaster#free} method with a directory with a file non-persisted.
   */
  @Test
  public void freeDirWithNonPersistedFile() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    mThrown.expect(UnexpectedAlluxioException.class);
    mThrown.expectMessage(ExceptionMessage.CANNOT_FREE_NON_PERSISTED_FILE
        .getMessage(NESTED_FILE_URI.getPath()));
    // cannot free the parent dir of a non-persisted file
    mFileSystemMaster.free(NESTED_FILE_URI.getParent(),
        FreeContext.mergeFrom(FreePOptions.newBuilder().setForced(false).setRecursive(true)));
  }

  /**
   * Tests the {@link FileSystemMaster#free} method with a directory with a file pinned when
   * forced flag is false.
   */
  @Test
  public void freeDirWithPinnedFileAndNotForced() throws Exception {
    mNestedFileContext.setWriteType(WriteType.CACHE_THROUGH);
    createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setPinned(true)));
    mThrown.expect(UnexpectedAlluxioException.class);
    mThrown.expectMessage(ExceptionMessage.CANNOT_FREE_PINNED_FILE
        .getMessage(NESTED_FILE_URI.getPath()));
    // cannot free the parent dir of a pinned file without "forced"
    mFileSystemMaster.free(NESTED_FILE_URI.getParent(),
        FreeContext.mergeFrom(FreePOptions.newBuilder().setForced(false).setRecursive(true)));
  }

  /**
   * Tests the {@link FileSystemMaster#free} method with a directory with a file pinned when
   * forced flag is true.
   */
  @Test
  public void freeDirWithPinnedFileAndForced() throws Exception {
    mNestedFileContext.setWriteType(WriteType.CACHE_THROUGH);
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setPinned(true)));
    // free the parent dir of a pinned file with "forced"
    mFileSystemMaster.free(NESTED_FILE_URI.getParent(),
        FreeContext.mergeFrom(FreePOptions.newBuilder().setForced(true).setRecursive(true)));
    // Update the heartbeat of removedBlockId received from worker 1.
    Command heartbeat = mBlockMaster.workerHeartbeat(mWorkerId1, null,
        ImmutableMap.of(Constants.MEDIUM_MEM, (long) Constants.KB), ImmutableList.of(blockId),
        ImmutableMap.of(), ImmutableMap.of(), mMetrics);
    // Verify the muted Free command on worker1.
    assertEquals(Command.newBuilder().setCommandType(CommandType.Nothing).build(), heartbeat);
    assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests the {@link FileSystemMaster#mount(AlluxioURI, AlluxioURI, MountContext)} method.
   */
  @Test
  public void mount() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountContext.defaults());
  }

  /**
   * Tests mounting an existing dir.
   */
  @Test
  public void mountExistingDir() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    mFileSystemMaster.createDirectory(alluxioURI, CreateDirectoryContext.defaults());
    mThrown.expect(InvalidPathException.class);
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountContext.defaults());
  }

  /**
   * Tests mounting to an Alluxio path whose parent dir does not exist.
   */
  @Test
  public void mountNonExistingParentDir() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/non-existing/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountContext.defaults());
  }

  /**
   * Tests a readOnly mount for the create directory op.
   */
  @Test
  public void mountReadOnlyCreateDirectory() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI,
        MountContext.mergeFrom(MountPOptions.newBuilder().setReadOnly(true)));

    mThrown.expect(AccessControlException.class);
    AlluxioURI path = new AlluxioURI("/hello/dir1");
    mFileSystemMaster.createDirectory(path, CreateDirectoryContext.defaults());
  }

  /**
   * Tests a readOnly mount for the create file op.
   */
  @Test
  public void mountReadOnlyCreateFile() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI,
        MountContext.mergeFrom(MountPOptions.newBuilder().setReadOnly(true)));

    mThrown.expect(AccessControlException.class);
    AlluxioURI path = new AlluxioURI("/hello/file1");
    mFileSystemMaster.createFile(path, CreateFileContext.defaults());
  }

  /**
   * Tests a readOnly mount for the delete op.
   */
  @Test
  public void mountReadOnlyDeleteFile() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    createTempUfsFile("ufs/hello/file1");
    mFileSystemMaster.mount(alluxioURI, ufsURI,
        MountContext.mergeFrom(MountPOptions.newBuilder().setReadOnly(true)));

    mThrown.expect(AccessControlException.class);
    AlluxioURI path = new AlluxioURI("/hello/file1");
    mFileSystemMaster.delete(path, DeleteContext.defaults());
  }

  /**
   * Tests a readOnly mount for the rename op.
   */
  @Test
  public void mountReadOnlyRenameFile() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    createTempUfsFile("ufs/hello/file1");
    mFileSystemMaster.mount(alluxioURI, ufsURI,
        MountContext.mergeFrom(MountPOptions.newBuilder().setReadOnly(true)));

    mThrown.expect(AccessControlException.class);
    AlluxioURI src = new AlluxioURI("/hello/file1");
    AlluxioURI dst = new AlluxioURI("/hello/file2");
    mFileSystemMaster.rename(src, dst, RenameContext.defaults());
  }

  /**
   * Tests a readOnly mount for the set attribute op.
   */
  @Test
  public void mountReadOnlySetAttribute() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    createTempUfsFile("ufs/hello/file1");
    mFileSystemMaster.mount(alluxioURI, ufsURI,
        MountContext.mergeFrom(MountPOptions.newBuilder().setReadOnly(true)));

    mThrown.expect(AccessControlException.class);
    AlluxioURI path = new AlluxioURI("/hello/file1");
    mFileSystemMaster.setAttribute(path,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setOwner("owner")));
  }

  /**
   * Tests mounting a shadow Alluxio dir.
   */
  @Test
  public void mountShadowDir() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello/shadow");

    mFileSystemMaster.mount(alluxioURI, ufsURI.getParent(),
        MountContext.defaults());
    AlluxioURI shadowAlluxioURI = new AlluxioURI("/hello/shadow");
    AlluxioURI notShadowAlluxioURI = new AlluxioURI("/hello/notshadow");
    AlluxioURI shadowUfsURI = createTempUfsDir("ufs/hi");
    AlluxioURI notShadowUfsURI = createTempUfsDir("ufs/notshadowhi");
    mFileSystemMaster.mount(notShadowAlluxioURI, notShadowUfsURI,
        MountContext.defaults());
    mThrown.expect(IOException.class);
    mFileSystemMaster.mount(shadowAlluxioURI, shadowUfsURI,
        MountContext.defaults());
  }

  /**
   * Tests mounting a prefix UFS dir.
   */
  @Test
  public void mountPrefixUfsDir() throws Exception {
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello/shadow");
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountContext.defaults());
    AlluxioURI preUfsURI = ufsURI.getParent();
    AlluxioURI anotherAlluxioURI = new AlluxioURI("/hi");
    mThrown.expect(InvalidPathException.class);
    mFileSystemMaster.mount(anotherAlluxioURI, preUfsURI, MountContext.defaults());
  }

  /**
   * Tests mounting a suffix UFS dir.
   */
  @Test
  public void mountSuffixUfsDir() throws Exception {
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello/shadow");
    AlluxioURI preUfsURI = ufsURI.getParent();
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    mFileSystemMaster.mount(alluxioURI, preUfsURI, MountContext.defaults());
    AlluxioURI anotherAlluxioURI = new AlluxioURI("/hi");
    mThrown.expect(InvalidPathException.class);
    mFileSystemMaster.mount(anotherAlluxioURI, ufsURI, MountContext.defaults());
  }

  /**
   * Tests unmount operation.
   */
  @Test
  public void unmount() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountContext.defaults());
    mFileSystemMaster.createDirectory(alluxioURI.join("dir"), CreateDirectoryContext
        .defaults().setWriteType(WriteType.CACHE_THROUGH));
    mFileSystemMaster.unmount(alluxioURI);
    // after unmount, ufs path under previous mount point should still exist
    File file = new File(ufsURI.join("dir").getPath());
    assertTrue(file.exists());
    // after unmount, alluxio path under previous mount point should not exist
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(alluxioURI.join("dir"), GET_STATUS_CONTEXT);
  }

  /**
   * Tests unmount operation failed when unmounting root.
   */
  @Test
  public void unmountRootWithException() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mFileSystemMaster.unmount(new AlluxioURI("/"));
  }

  /**
   * Tests unmount operation failed when unmounting non-mount point.
   */
  @Test
  public void unmountNonMountPointWithException() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountContext.defaults());
    AlluxioURI dirURI = alluxioURI.join("dir");
    mFileSystemMaster.createDirectory(dirURI, CreateDirectoryContext
        .defaults().setWriteType(WriteType.MUST_CACHE));
    mThrown.expect(InvalidPathException.class);
    mFileSystemMaster.unmount(dirURI);
  }

  /**
   * Tests unmount operation failed when unmounting non-existing dir.
   */
  @Test
  public void unmountNonExistingPathWithException() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.unmount(new AlluxioURI("/FileNotExists"));
  }

  /**
   * Creates a temporary UFS folder. The ufsPath must be a relative path since it's a temporary dir
   * created by mTestFolder.
   *
   * @param ufsPath the UFS path of the temp dir needed to created
   * @return the AlluxioURI of the temp dir
   */
  private AlluxioURI createTempUfsDir(String ufsPath) throws IOException {
    String path = mTestFolder.newFolder(ufsPath.split("/")).getPath();
    return new AlluxioURI("file", null, path);
  }

  /**
   * Creates a file in a temporary UFS folder.
   *
   * @param ufsPath the UFS path of the temp file to create
   * @return the AlluxioURI of the temp file
   */
  private AlluxioURI createTempUfsFile(String ufsPath) throws IOException {
    String path = mTestFolder.newFile(ufsPath).getPath();
    return new AlluxioURI(path);
  }

  /**
   * Tests the {@link DefaultFileSystemMaster#stop()} method.
   */
  @Test
  public void stop() throws Exception {
    mRegistry.stop();
    assertTrue(mExecutorService.isShutdown());
    assertTrue(mExecutorService.isTerminated());
  }

  /**
   * Tests the {@link FileSystemMaster#workerHeartbeat} method.
   */
  @Test
  public void workerHeartbeat() throws Exception {
    long blockId = createFileWithSingleBlock(ROOT_FILE_URI);

    long fileId = mFileSystemMaster.getFileId(ROOT_FILE_URI);
    mFileSystemMaster.scheduleAsyncPersistence(ROOT_FILE_URI,
        ScheduleAsyncPersistenceContext.defaults());

    FileSystemCommand command = mFileSystemMaster
        .workerHeartbeat(mWorkerId1, Lists.newArrayList(fileId), WorkerHeartbeatContext.defaults());
    assertEquals(alluxio.wire.CommandType.PERSIST, command.getCommandType());
    assertEquals(0, command.getCommandOptions().getPersistOptions().getFilesToPersist().size());
  }

  /**
   * Tests that lost files can successfully be detected.
   */
  @Test
  public void lostFilesDetection() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    long fileId = mFileSystemMaster.getFileId(NESTED_FILE_URI);

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    mBlockMaster.reportLostBlocks(fileInfo.getBlockIds());

    assertEquals(PersistenceState.NOT_PERSISTED.name(), fileInfo.getPersistenceState());
    // Check with getPersistenceState.
    assertEquals(PersistenceState.NOT_PERSISTED,
        mFileSystemMaster.getPersistenceState(fileId));

    // run the detector
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_LOST_FILES_DETECTION);

    fileInfo = mFileSystemMaster.getFileInfo(fileId);
    assertEquals(PersistenceState.LOST.name(), fileInfo.getPersistenceState());
    // Check with getPersistenceState.
    assertEquals(PersistenceState.LOST, mFileSystemMaster.getPersistenceState(fileId));
  }

  @Test
  public void getUfsInfo() throws Exception {
    FileInfo alluxioRootInfo =
        mFileSystemMaster.getFileInfo(new AlluxioURI("alluxio://"), GET_STATUS_CONTEXT);
    UfsInfo ufsRootInfo = mFileSystemMaster.getUfsInfo(alluxioRootInfo.getMountId());
    assertEquals(mUnderFS, ufsRootInfo.getUri().getPath());
    assertTrue(ufsRootInfo.getMountOptions().getPropertiesMap().isEmpty());
  }

  @Test
  public void getUfsInfoNotExist() throws Exception {
    UfsInfo noSuchUfsInfo = mFileSystemMaster.getUfsInfo(100L);
    assertNull(noSuchUfsInfo.getUri());
    assertNull(noSuchUfsInfo.getMountOptions());
  }

  /**
   * Tests that setting the ufs fingerprint persists across restarts.
   */
  @Test
  public void setUfsFingerprintReplay() throws Exception {
    String fingerprint = "FINGERPRINT";
    createFileWithSingleBlock(NESTED_FILE_URI);

    ((DefaultFileSystemMaster) mFileSystemMaster).setAttribute(NESTED_FILE_URI,
        SetAttributeContext.defaults().setUfsFingerprint(fingerprint));

    // Simulate restart.
    stopServices();
    startServices();

    assertEquals(fingerprint,
        mFileSystemMaster.getFileInfo(NESTED_FILE_URI,
            GetStatusContext.defaults()).getUfsFingerprint());
  }

  @Test
  public void ignoreInvalidFiles() throws Exception {
    FileUtils.createDir(Paths.get(mUnderFS, "test").toString());
    FileUtils.createFile(Paths.get(mUnderFS, "test", "a?b=C").toString());
    FileUtils.createFile(Paths.get(mUnderFS, "test", "valid").toString());
    List<FileInfo> listing = mFileSystemMaster.listStatus(new AlluxioURI("/test"),
        ListStatusContext.mergeFrom(ListStatusPOptions.newBuilder()
            .setLoadMetadataType(LoadMetadataPType.ALWAYS).setRecursive(true)));
    assertEquals(1, listing.size());
    assertEquals("valid", listing.get(0).getName());
  }

  @Test
  public void propagatePersisted() throws Exception {
    AlluxioURI nestedFile = new AlluxioURI("/nested1/nested2/file");
    AlluxioURI parent1 = new AlluxioURI("/nested1/");
    AlluxioURI parent2 = new AlluxioURI("/nested1/nested2/");

    createFileWithSingleBlock(nestedFile);

    // Nothing is persisted yet.
    assertEquals(PersistenceState.NOT_PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(nestedFile, GetStatusContext.defaults())
            .getPersistenceState());
    assertEquals(PersistenceState.NOT_PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(parent1,
            GetStatusContext.defaults()).getPersistenceState());
    assertEquals(PersistenceState.NOT_PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(parent2,
            GetStatusContext.defaults()).getPersistenceState());

    // Persist the file.
    mFileSystemMaster.setAttribute(nestedFile,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder().setPersisted(true)));

    // Everything component should be persisted.
    assertEquals(PersistenceState.PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(nestedFile, GetStatusContext.defaults())
            .getPersistenceState());
    assertEquals(PersistenceState.PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(parent1,
            GetStatusContext.defaults()).getPersistenceState());
    assertEquals(PersistenceState.PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(parent2,
            GetStatusContext.defaults()).getPersistenceState());

    // Simulate restart.
    stopServices();
    startServices();

    // Everything component should be persisted.
    assertEquals(PersistenceState.PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(nestedFile, GetStatusContext.defaults())
            .getPersistenceState());
    assertEquals(PersistenceState.PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(parent1,
            GetStatusContext.defaults()).getPersistenceState());
    assertEquals(PersistenceState.PERSISTED.toString(),
        mFileSystemMaster.getFileInfo(parent2,
            GetStatusContext.defaults()).getPersistenceState());
  }

  /**
   * Tests using SetAttribute to apply xAttr to a file, using the TRUNCATE update strategy.
   */
  @Test
  public void setAttributeXAttrTruncate() throws Exception {
    // Create file with xAttr to overwrite
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setBlockSizeBytes(Constants.KB)
        .putXattr("foo", ByteString.copyFrom("foo", StandardCharsets.UTF_8))
        .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
        .setRecursive(true));
    FileInfo fileInfo = mFileSystemMaster.createFile(ROOT_FILE_URI, context);
    assertEquals(new String(fileInfo.getXAttr().get("foo"), StandardCharsets.UTF_8), "foo");

    // Replace the xAttr map
    mFileSystemMaster.setAttribute(ROOT_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .putXattr("bar", ByteString.copyFrom("bar", StandardCharsets.UTF_8))
            .setXattrUpdateStrategy(alluxio.proto.journal.File.XAttrUpdateStrategy.TRUNCATE)));
    FileInfo updatedFileInfo = mFileSystemMaster.getFileInfo(ROOT_FILE_URI, GET_STATUS_CONTEXT);
    assertEquals(updatedFileInfo.getXAttr().size(), 1);
    assertEquals(new String(updatedFileInfo.getXAttr().get("bar"), StandardCharsets.UTF_8),
        "bar");
  }

  /**
   * Tests using SetAttribute to apply xAttr to a file, using the UNION_REPLACE update strategy.
   */
  @Test
  public void setAttributeXAttrUnionReplace() throws Exception {
    // Create file with xAttr to overwrite
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setBlockSizeBytes(Constants.KB)
        .putXattr("foo", ByteString.copyFrom("foo", StandardCharsets.UTF_8))
        .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
        .setRecursive(true));
    FileInfo fileInfo = mFileSystemMaster.createFile(ROOT_FILE_URI, context);
    assertEquals(new String(fileInfo.getXAttr().get("foo"), StandardCharsets.UTF_8), "foo");

    // Combine the xAttr maps, replacing existing keys
    mFileSystemMaster.setAttribute(ROOT_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .putXattr("foo", ByteString.copyFrom("baz", StandardCharsets.UTF_8))
            .putXattr("bar", ByteString.copyFrom("bar", StandardCharsets.UTF_8))
            .setXattrUpdateStrategy(
                alluxio.proto.journal.File.XAttrUpdateStrategy.UNION_REPLACE)));
    FileInfo updatedFileInfo = mFileSystemMaster.getFileInfo(ROOT_FILE_URI, GET_STATUS_CONTEXT);
    assertEquals(updatedFileInfo.getXAttr().size(), 2);
    assertEquals(new String(updatedFileInfo.getXAttr().get("foo"), StandardCharsets.UTF_8),
        "baz");
    assertEquals(new String(updatedFileInfo.getXAttr().get("bar"), StandardCharsets.UTF_8),
        "bar");
  }

  /**
   * Tests using SetAttribute to apply xAttr to a file, using the UNION_PRESERVE update strategy.
   */
  @Test
  public void setAttributeXAttrUnionPreserve() throws Exception {
    // Create file with xAttr to overwrite
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setBlockSizeBytes(Constants.KB)
        .putXattr("foo", ByteString.copyFrom("foo", StandardCharsets.UTF_8))
        .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
        .setRecursive(true));
    FileInfo fileInfo = mFileSystemMaster.createFile(ROOT_FILE_URI, context);
    assertEquals(new String(fileInfo.getXAttr().get("foo"), StandardCharsets.UTF_8), "foo");

    // Combine the xAttr maps, preserving existing keys
    mFileSystemMaster.setAttribute(ROOT_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .putXattr("foo", ByteString.copyFrom("baz", StandardCharsets.UTF_8))
            .putXattr("bar", ByteString.copyFrom("bar", StandardCharsets.UTF_8))
            .setXattrUpdateStrategy(
                alluxio.proto.journal.File.XAttrUpdateStrategy.UNION_PRESERVE)));
    FileInfo updatedFileInfo = mFileSystemMaster.getFileInfo(ROOT_FILE_URI, GET_STATUS_CONTEXT);
    assertEquals(updatedFileInfo.getXAttr().size(), 2);
    assertEquals(new String(updatedFileInfo.getXAttr().get("foo"), StandardCharsets.UTF_8),
        "foo");
    assertEquals(new String(updatedFileInfo.getXAttr().get("bar"), StandardCharsets.UTF_8),
        "bar");
  }

  /**
   * Tests using SetAttribute to apply xAttr to a file, using the DELETE_KEYS update strategy.
   */
  @Test
  public void setAttributeXAttrDeleteKeys() throws Exception {
    // Create file with xAttr to delete
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setBlockSizeBytes(Constants.KB)
        .putXattr("foo", ByteString.copyFrom("foo", StandardCharsets.UTF_8))
        .setXattrPropStrat(XAttrPropagationStrategy.LEAF_NODE)
        .setRecursive(true));
    FileInfo fileInfo = mFileSystemMaster.createFile(ROOT_FILE_URI, context);
    assertEquals(new String(fileInfo.getXAttr().get("foo"), StandardCharsets.UTF_8), "foo");

    // Delete a non-existing key
    mFileSystemMaster.setAttribute(ROOT_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .putXattr("bar", ByteString.copyFrom("", StandardCharsets.UTF_8))
            .setXattrUpdateStrategy(alluxio.proto.journal.File.XAttrUpdateStrategy.DELETE_KEYS)));
    FileInfo updatedFileInfo = mFileSystemMaster.getFileInfo(ROOT_FILE_URI, GET_STATUS_CONTEXT);
    assertEquals(updatedFileInfo.getXAttr().size(), 1);

    // Delete an existing key
    mFileSystemMaster.setAttribute(ROOT_FILE_URI,
        SetAttributeContext.mergeFrom(SetAttributePOptions.newBuilder()
            .putXattr("foo", ByteString.copyFrom("", StandardCharsets.UTF_8))
            .setXattrUpdateStrategy(alluxio.proto.journal.File.XAttrUpdateStrategy.DELETE_KEYS)));
    updatedFileInfo = mFileSystemMaster.getFileInfo(ROOT_FILE_URI, GET_STATUS_CONTEXT);
    assertNullOrEmpty(updatedFileInfo.getXAttr());
  }

  /**
   * Tests setting xAttr when creating a file in a nested directory, and
   * propagates the xAttr back to the parent directories.
   */
  @Test
  public void createFileXAttrPropagateNewPaths() throws Exception {
    createFileXAttr(XAttrPropagationStrategy.NEW_PATHS);
  }

  /**
   * Tests setting xAttr when creating a file in a nested directory, and
   * only assigns xAttr to the leaf node.
   */
  @Test
  public void createFileXAttrPropagateLeaf() throws Exception {
    createFileXAttr(XAttrPropagationStrategy.LEAF_NODE);
  }

  private void createFileXAttr(XAttrPropagationStrategy propStrat) throws Exception {
    // Set xAttr upon creating the file
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setBlockSizeBytes(Constants.KB)
        .putXattr("foo", ByteString.copyFrom("bar", StandardCharsets.UTF_8))
        .setRecursive(true)
        .setXattrPropStrat(propStrat));
    FileInfo fileInfo = mFileSystemMaster.createFile(NESTED_FILE_URI, context);
    assertEquals(new String(fileInfo.getXAttr().get("foo"), StandardCharsets.UTF_8), "bar");

    checkParentXAttrTags(NESTED_FILE_URI, propStrat);
  }

  /**
   * Tests setting xAttr when creating a nested subdirectory, and
   * propagates the xAttr back to the parent directories.
   */
  @Test
  public void createDirXAttrPropagateNewPaths() throws Exception {
    createDirXAttr(XAttrPropagationStrategy.NEW_PATHS);
  }

  /**
   * Tests setting xAttr when creating a nested subdirectory, and
   * only assigns xAttr to the leaf node.
   */
  @Test
  public void createDirXAttrPropagateLeaf() throws Exception {
    createDirXAttr(XAttrPropagationStrategy.LEAF_NODE);
  }

  private void createDirXAttr(XAttrPropagationStrategy propStrat) throws Exception {
    // Set xAttr upon creating the directory
    CreateDirectoryContext directoryContext = CreateDirectoryContext.mergeFrom(
        CreateDirectoryPOptions.newBuilder()
            .putXattr("foo", ByteString.copyFrom("bar", StandardCharsets.UTF_8))
            .setXattrPropStrat(propStrat)
            .setRecursive(true));
    long dirId = mFileSystemMaster.createDirectory(NESTED_DIR_URI, directoryContext);
    assertEquals(new String(mFileSystemMaster.getFileInfo(dirId).getXAttr().get("foo"),
        StandardCharsets.UTF_8), "bar");

    checkParentXAttrTags(NESTED_DIR_URI, propStrat);
  }

  private void checkParentXAttrTags(AlluxioURI uri, XAttrPropagationStrategy propStrat)
      throws Exception {
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(uri, GET_STATUS_CONTEXT);
    Map<String, byte[]> xAttrs = fileInfo.getXAttr();
    assertNotEquals(xAttrs, null);

    switch (propStrat) {
      case NEW_PATHS:
        // Verify that the parent directories have matching xAttr
        for (int i = 1; uri.getLeadingPath(i + 1) != null; i++) {
          for (Map.Entry<String, byte[]> entry : xAttrs.entrySet()) {
            assertArrayEquals(mFileSystemMaster.getFileInfo(new AlluxioURI(uri.getLeadingPath(i)),
                GET_STATUS_CONTEXT).getXAttr().get(entry.getKey()), xAttrs.get(entry.getKey()));
          }
        }
        break;
      case LEAF_NODE:
        // Verify that the parent directories have no xAttr
        for (int i = 1; uri.getLeadingPath(i + 1) != null; i++) {
          assertNullOrEmpty(mFileSystemMaster.getFileInfo(new AlluxioURI(uri.getLeadingPath(i)),
              GET_STATUS_CONTEXT).getXAttr());
        }
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Invalid XAttrPropagationStrategy: %s", propStrat));
    }
  }

  private long createFileWithSingleBlock(AlluxioURI uri) throws Exception {
    mFileSystemMaster.createFile(uri, mNestedFileContext);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(uri);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB,
        Constants.MEDIUM_MEM, Constants.MEDIUM_MEM, blockId, Constants.KB);
    CompleteFileContext context =
        CompleteFileContext.mergeFrom(CompleteFilePOptions.newBuilder().setUfsLength(Constants.KB));
    mFileSystemMaster.completeFile(uri, context);
    return blockId;
  }

  private void startServices() throws Exception {
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
        .newFixedThreadPool(4, ThreadFactoryUtils.build(
            "DefaultFileSystemMasterTest-%d", true));
    mFileSystemMaster = new DefaultFileSystemMaster(mBlockMaster, masterContext,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));
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

  private void stopServices() throws Exception {
    mRegistry.stop();
    mJournalSystem.stop();
    mFileSystemMaster.close();
    mFileSystemMaster.stop();
  }

  /**
   * Asserts that the map is null or empty.
   * @param m the map to check
   */
  private static void assertNullOrEmpty(Map m) {
    assertTrue(m == null || m.isEmpty());
  }
}
