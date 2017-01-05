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

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.Configuration;
import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.collections.ConcurrentHashSet;
import alluxio.exception.BlockInfoException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.meta.TtlIntervalRule;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.LoadMetadataOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.master.journal.JournalFactory;
import alluxio.security.GroupMappingServiceTestUtils;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.thrift.Command;
import alluxio.thrift.CommandType;
import alluxio.thrift.FileSystemCommand;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.TtlAction;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Unit tests for {@link FileSystemMaster}.
 */
public final class FileSystemMasterTest {
  private static final AlluxioURI NESTED_URI = new AlluxioURI("/nested/test");
  private static final AlluxioURI NESTED_FILE_URI = new AlluxioURI("/nested/test/file");
  private static final AlluxioURI NESTED_DIR_URI = new AlluxioURI("/nested/test/dir");
  private static final AlluxioURI ROOT_URI = new AlluxioURI("/");
  private static final AlluxioURI ROOT_FILE_URI = new AlluxioURI("/file");
  private static final AlluxioURI TEST_URI = new AlluxioURI("/test");
  private static final String TEST_USER = "test";
  private static CreateFileOptions sNestedFileOptions;

  private BlockMaster mBlockMaster;
  private ExecutorService mExecutorService;
  private FileSystemMaster mFileSystemMaster;
  private long mWorkerId1;
  private long mWorkerId2;

  private String mUnderFS;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER);

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.MASTER_TTL_CHECK, HeartbeatContext.MASTER_LOST_FILES_DETECTION);

  // Set ttl interval to 0 so that there is no delay in detecting expired files.
  @ClassRule
  public static TtlIntervalRule sTtlIntervalRule = new TtlIntervalRule(0);

  /**
   * Sets up the dependencies before a single test runs.
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    sNestedFileOptions =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setRecursive(true);
  }

  /**
   * Sets up the dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    LoginUserTestUtils.resetLoginUser();
    GroupMappingServiceTestUtils.resetCache();
    Configuration.set(PropertyKey.SECURITY_LOGIN_USERNAME, TEST_USER);
    // Set umask "000" to make default directory permission 0777 and default file permission 0666.
    Configuration.set(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_UMASK, "000");
    // This makes sure that the mount point of the UFS corresponding to the Alluxio root ("/")
    // doesn't exist by default (helps loadRootTest).
    mUnderFS = PathUtils.concatPath(mTestFolder.newFolder().getAbsolutePath(), "underFs");
    Configuration.set(PropertyKey.UNDERFS_ADDRESS, mUnderFS);

    JournalFactory journalFactory =
        new JournalFactory.ReadWrite(mTestFolder.newFolder().getAbsolutePath());
    mBlockMaster = new BlockMaster(journalFactory);
    mExecutorService =
        Executors.newFixedThreadPool(2, ThreadFactoryUtils.build("FileSystemMasterTest-%d", true));
    mFileSystemMaster = new FileSystemMaster(mBlockMaster, journalFactory,
        ExecutorServiceFactories.constantExecutorServiceFactory(mExecutorService));

    mBlockMaster.start(true);
    mFileSystemMaster.start(true);

    // set up workers
    mWorkerId1 = mBlockMaster.getWorkerId(
        new WorkerNetAddress().setHost("localhost").setRpcPort(80).setDataPort(81).setWebPort(82));
    mBlockMaster.workerRegister(mWorkerId1, Arrays.asList("MEM", "SSD"),
        ImmutableMap.of("MEM", (long) Constants.MB, "SSD", (long) Constants.MB),
        ImmutableMap.of("MEM", (long) Constants.KB, "SSD", (long) Constants.KB),
        new HashMap<String, List<Long>>());
    mWorkerId2 = mBlockMaster.getWorkerId(
        new WorkerNetAddress().setHost("remote").setRpcPort(80).setDataPort(81).setWebPort(82));
    mBlockMaster.workerRegister(mWorkerId2, Arrays.asList("MEM", "SSD"),
        ImmutableMap.of("MEM", (long) Constants.MB, "SSD", (long) Constants.MB),
        ImmutableMap.of("MEM", (long) Constants.KB, "SSD", (long) Constants.KB),
        new HashMap<String, List<Long>>());
  }

  /**
   * Resets global state after each test run.
   */
  @After
  public void after() throws Exception {
    mFileSystemMaster.stop();
    mBlockMaster.stop();
    ConfigurationTestUtils.resetConfiguration();
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, boolean)} method.
   */
  @Test
  public void deleteFile() throws Exception {
    // cannot delete root
    try {
      mFileSystemMaster.delete(ROOT_URI, true);
      Assert.fail("Should not have been able to delete the root");
    } catch (InvalidPathException e) {
      Assert.assertEquals(ExceptionMessage.DELETE_ROOT_DIRECTORY.getMessage(), e.getMessage());
    }

    // delete the file
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    mFileSystemMaster.delete(NESTED_FILE_URI, false);

    mThrown.expect(BlockInfoException.class);
    mBlockMaster.getBlockInfo(blockId);

    // Update the heartbeat of removedBlockId received from worker 1
    Command heartBeat1 =
        mBlockMaster.workerHeartbeat(mWorkerId1, ImmutableMap.of("MEM", (long) Constants.KB),
            ImmutableList.of(blockId), ImmutableMap.<String, List<Long>>of());
    // Verify the muted Free command on worker1
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat1);
    Assert.assertFalse(mBlockMaster.getLostBlocks().contains(blockId));

    // verify the file is deleted
    Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, boolean)} method with a non-empty
   * directory.
   */
  @Test
  public void deleteNonemptyDirectory() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    String dirName = mFileSystemMaster.getFileInfo(NESTED_URI).getName();
    try {
      mFileSystemMaster.delete(NESTED_URI, false);
      Assert.fail("Deleting a non-empty directory without setting recursive should fail");
    } catch (DirectoryNotEmptyException e) {
      String expectedMessage =
          ExceptionMessage.DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE.getMessage(dirName);
      Assert.assertEquals(expectedMessage, e.getMessage());
    }

    // Now delete with recursive set to true
    mFileSystemMaster.delete(NESTED_URI, true);
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, boolean)} method for a directory.
   */
  @Test
  public void deleteDir() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    // delete the dir
    mFileSystemMaster.delete(NESTED_URI, true);

    // verify the dir is deleted
    Assert.assertEquals(-1, mFileSystemMaster.getFileId(NESTED_URI));
  }

  /**
   * Tests the {@link FileSystemMaster#getNewBlockIdForFile(AlluxioURI)} method.
   */
  @Test
  public void getNewBlockIdForFile() throws Exception {
    mFileSystemMaster.createFile(NESTED_FILE_URI, sNestedFileOptions);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(NESTED_FILE_URI);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI);
    Assert.assertEquals(Lists.newArrayList(blockId), fileInfo.getBlockIds());
  }

  @Test
  public void getPath() throws Exception {
    AlluxioURI rootUri = new AlluxioURI("/");
    long rootId = mFileSystemMaster.getFileId(rootUri);
    Assert.assertEquals(rootUri, mFileSystemMaster.getPath(rootId));

    // get non-existent id
    try {
      mFileSystemMaster.getPath(rootId + 1234);
      Assert.fail("getPath() for a non-existent id should fail.");
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
    Assert.assertEquals(PersistenceState.PERSISTED, mFileSystemMaster.getPersistenceState(rootId));

    // get non-existent id
    try {
      mFileSystemMaster.getPersistenceState(rootId + 1234);
      Assert.fail("getPath() for a non-existent id should fail.");
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
    Assert.assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(ROOT_URI));
    Assert.assertEquals(ROOT_URI, mFileSystemMaster.getPath(mFileSystemMaster.getFileId(ROOT_URI)));

    Assert.assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_URI));
    Assert.assertEquals(NESTED_URI,
        mFileSystemMaster.getPath(mFileSystemMaster.getFileId(NESTED_URI)));

    Assert.assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
    Assert.assertEquals(NESTED_FILE_URI,
        mFileSystemMaster.getPath(mFileSystemMaster.getFileId(NESTED_FILE_URI)));

    // These URIs do not exist.
    Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(ROOT_FILE_URI));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(TEST_URI));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID,
        mFileSystemMaster.getFileId(NESTED_FILE_URI.join("DNE")));
  }

  /**
   * Tests the {@link FileSystemMaster#getFileInfo(AlluxioURI)} method.
   */
  @Test
  public void getFileInfo() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    long fileId;
    FileInfo info;

    fileId = mFileSystemMaster.getFileId(ROOT_URI);
    info = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(ROOT_URI.getPath(), info.getPath());
    Assert.assertEquals(ROOT_URI.getPath(), mFileSystemMaster.getFileInfo(ROOT_URI).getPath());

    fileId = mFileSystemMaster.getFileId(NESTED_URI);
    info = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(NESTED_URI.getPath(), info.getPath());
    Assert.assertEquals(NESTED_URI.getPath(), mFileSystemMaster.getFileInfo(NESTED_URI).getPath());

    fileId = mFileSystemMaster.getFileId(NESTED_FILE_URI);
    info = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(NESTED_FILE_URI.getPath(), info.getPath());
    Assert.assertEquals(NESTED_FILE_URI.getPath(),
        mFileSystemMaster.getFileInfo(NESTED_FILE_URI).getPath());

    // Test non-existent id.
    try {
      mFileSystemMaster.getFileInfo(fileId + 1234);
      Assert.fail("getFileInfo() for a non-existent id should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }

    // Test non-existent URIs.
    try {
      mFileSystemMaster.getFileInfo(ROOT_FILE_URI);
      Assert.fail("getFileInfo() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
    try {
      mFileSystemMaster.getFileInfo(TEST_URI);
      Assert.fail("getFileInfo() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
    try {
      mFileSystemMaster.getFileInfo(NESTED_URI.join("DNE"));
      Assert.fail("getFileInfo() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  @Test
  public void getFileInfoWithLoadMetadata() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryOptions.defaults());

    // Create ufs file
    Files.createFile(Paths.get(ufsMount.join("file").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount, MountOptions.defaults());

    // 3 directories exist.
    Assert.assertEquals(3, mFileSystemMaster.getNumberOfPaths());

    // getFileInfo should load metadata automatically.
    AlluxioURI uri = new AlluxioURI("/mnt/local/file");
    Assert.assertEquals(uri.getPath(), mFileSystemMaster.getFileInfo(uri).getPath());

    // getFileInfo should have loaded another file, so now 4 paths exist.
    Assert.assertEquals(4, mFileSystemMaster.getNumberOfPaths());
  }

  @Test
  public void getFileIdWithLoadMetadata() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryOptions.defaults());

    // Create ufs file
    Files.createFile(Paths.get(ufsMount.join("file").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount, MountOptions.defaults());

    // 3 directories exist.
    Assert.assertEquals(3, mFileSystemMaster.getNumberOfPaths());

    // getFileId should load metadata automatically.
    AlluxioURI uri = new AlluxioURI("/mnt/local/file");
    Assert.assertNotEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(uri));

    // getFileId should have loaded another file, so now 4 paths exist.
    Assert.assertEquals(4, mFileSystemMaster.getNumberOfPaths());
  }

  @Test
  public void listStatusWithLoadMetadataNever() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryOptions.defaults());

    // Create ufs file
    Files.createDirectory(Paths.get(ufsMount.join("dir1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file2").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount, MountOptions.defaults());

    // 3 directories exist.
    Assert.assertEquals(3, mFileSystemMaster.getNumberOfPaths());

    // getFileId should load metadata automatically.
    AlluxioURI uri = new AlluxioURI("/mnt/local/dir1");
    List<FileInfo> fileInfoList = mFileSystemMaster.listStatus(uri,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never));
    Assert.assertEquals(0, fileInfoList.size());
    Assert.assertEquals(4, mFileSystemMaster.getNumberOfPaths());
  }

  @Test
  public void listStatusWithLoadMetadataOnce() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryOptions.defaults());

    // Create ufs file
    Files.createDirectory(Paths.get(ufsMount.join("dir1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file2").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount, MountOptions.defaults());

    // 3 directories exist.
    Assert.assertEquals(3, mFileSystemMaster.getNumberOfPaths());

    // getFileId should load metadata automatically.
    AlluxioURI uri = new AlluxioURI("/mnt/local/dir1");
    List<FileInfo> fileInfoList = mFileSystemMaster.listStatus(uri, ListStatusOptions.defaults());
    Set<String> paths = new HashSet<>();
    for (FileInfo fileInfo : fileInfoList) {
      paths.add(fileInfo.getPath());
    }
    Assert.assertEquals(2, paths.size());
    Assert.assertTrue(paths.contains("/mnt/local/dir1/file1"));
    Assert.assertTrue(paths.contains("/mnt/local/dir1/file2"));
    // listStatus should have loaded another 3 files (dir1, dir1/file1, dir1/file2), so now 6
    // paths exist.
    Assert.assertEquals(6, mFileSystemMaster.getNumberOfPaths());
  }

  @Test
  public void listStatusWithLoadMetadataAlways() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryOptions.defaults());

    // Create ufs file
    Files.createDirectory(Paths.get(ufsMount.join("dir1").getPath()));
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount, MountOptions.defaults());

    // 3 directories exist.
    Assert.assertEquals(3, mFileSystemMaster.getNumberOfPaths());

    // getFileId should load metadata automatically.
    AlluxioURI uri = new AlluxioURI("/mnt/local/dir1");
    List<FileInfo> fileInfoList = mFileSystemMaster.listStatus(uri, ListStatusOptions.defaults());
    Assert.assertEquals(0, fileInfoList.size());
    // listStatus should have loaded another files (dir1), so now 4 paths exist.
    Assert.assertEquals(4, mFileSystemMaster.getNumberOfPaths());

    // Add two files.
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("dir1").join("file2").getPath()));

    fileInfoList = mFileSystemMaster.listStatus(uri, ListStatusOptions.defaults());
    Assert.assertEquals(0, fileInfoList.size());
    // No file is loaded since dir1 has been loaded once.
    Assert.assertEquals(4, mFileSystemMaster.getNumberOfPaths());

    fileInfoList = mFileSystemMaster.listStatus(uri,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Always));
    Set<String> paths = new HashSet<>();
    for (FileInfo fileInfo : fileInfoList) {
      paths.add(fileInfo.getPath());
    }
    Assert.assertEquals(2, paths.size());
    Assert.assertTrue(paths.contains("/mnt/local/dir1/file1"));
    Assert.assertTrue(paths.contains("/mnt/local/dir1/file2"));
    // listStatus should have loaded another 2 files (dir1/file1, dir1/file2), so now 6
    // paths exist.
    Assert.assertEquals(6, mFileSystemMaster.getNumberOfPaths());
  }

  /**
   * Tests listing status on a non-persisted directory.
   */
  @Test
  public void listStatusWithLoadMetadataNonPersistedDir() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryOptions.defaults());

    // Create ufs file
    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount, MountOptions.defaults());

    // 3 directories exist.
    Assert.assertEquals(3, mFileSystemMaster.getNumberOfPaths());

    // Create a drectory in alluxio which is not persisted.
    AlluxioURI folder = new AlluxioURI("/mnt/local/folder");
    mFileSystemMaster.createDirectory(folder, CreateDirectoryOptions.defaults());

    Assert.assertFalse(
        mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local/folder")).isPersisted());

    // Create files in ufs.
    Files.createDirectory(Paths.get(ufsMount.join("folder").getPath()));
    Files.createFile(Paths.get(ufsMount.join("folder").join("file1").getPath()));
    Files.createFile(Paths.get(ufsMount.join("folder").join("file2").getPath()));

    // getStatus won't mark folder as persisted.
    Assert.assertFalse(
        mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local/folder")).isPersisted());

    List<FileInfo> fileInfoList =
        mFileSystemMaster.listStatus(folder, ListStatusOptions.defaults());
    Assert.assertEquals(2, fileInfoList.size());
    // listStatus should have loaded files (folder, folder/file1, folder/file2), so now 6 paths
    // exist.
    Assert.assertEquals(6, mFileSystemMaster.getNumberOfPaths());

    Set<String> paths = new HashSet<>();
    for (FileInfo f : fileInfoList) {
      paths.add(f.getPath());
    }
    Assert.assertEquals(2, paths.size());
    Assert.assertTrue(paths.contains("/mnt/local/folder/file1"));
    Assert.assertTrue(paths.contains("/mnt/local/folder/file2"));

    Assert.assertTrue(
        mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local/folder")).isPersisted());
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
    infos = mFileSystemMaster.listStatus(ROOT_URI,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never));
    Assert.assertEquals(files, infos.size());
    // Copy out filenames to use List contains.
    filenames = new ArrayList<>();
    for (FileInfo info : infos) {
      filenames.add(info.getPath());
    }
    // Compare all filenames.
    for (int i = 0; i < files; i++) {
      Assert.assertTrue(
          filenames.contains(ROOT_URI.join("file" + String.format("%05d", i)).toString()));
    }

    // Test single file.
    createFileWithSingleBlock(ROOT_FILE_URI);
    infos = mFileSystemMaster.listStatus(ROOT_FILE_URI,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never));
    Assert.assertEquals(1, infos.size());
    Assert.assertEquals(ROOT_FILE_URI.getPath(), infos.get(0).getPath());

    // Test files in nested directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(NESTED_URI.join("file" + String.format("%05d", i)));
    }
    infos = mFileSystemMaster.listStatus(NESTED_URI,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never));
    Assert.assertEquals(files, infos.size());
    // Copy out filenames to use List contains.
    filenames = new ArrayList<>();
    for (FileInfo info : infos) {
      filenames.add(info.getPath());
    }
    // Compare all filenames.
    for (int i = 0; i < files; i++) {
      Assert.assertTrue(
          filenames.contains(NESTED_URI.join("file" + String.format("%05d", i)).toString()));
    }

    // Test non-existent URIs.
    try {
      mFileSystemMaster.listStatus(NESTED_URI.join("DNE"),
          ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never));
      Assert.fail("listStatus() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  @Test
  public void getFileBlockInfoList() throws Exception {
    createFileWithSingleBlock(ROOT_FILE_URI);
    createFileWithSingleBlock(NESTED_FILE_URI);

    List<FileBlockInfo> blockInfo;

    blockInfo = mFileSystemMaster.getFileBlockInfoList(ROOT_FILE_URI);
    Assert.assertEquals(1, blockInfo.size());

    blockInfo = mFileSystemMaster.getFileBlockInfoList(NESTED_FILE_URI);
    Assert.assertEquals(1, blockInfo.size());

    // Test directory URI.
    try {
      mFileSystemMaster.getFileBlockInfoList(NESTED_URI);
      Assert.fail("getFileBlockInfoList() for a directory URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }

    // Test non-existent URI.
    try {
      mFileSystemMaster.getFileBlockInfoList(TEST_URI);
      Assert.fail("getFileBlockInfoList() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  @Test
  public void mountUnmount() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryOptions.defaults());

    // Alluxio mount point should not exist before mounting.
    try {
      mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local"));
      Assert.fail("getFileInfo() for a non-existent URI (before mounting) should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }

    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount, MountOptions.defaults());
    // Alluxio mount point should exist after mounting.
    Assert.assertNotNull(mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local")));

    mFileSystemMaster.unmount(new AlluxioURI("/mnt/local"));

    // Alluxio mount point should not exist after unmounting.
    try {
      mFileSystemMaster.getFileInfo(new AlluxioURI("/mnt/local"));
      Assert.fail("getFileInfo() for a non-existent URI (before mounting) should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  @Test
  public void loadMetadata() throws Exception {
    AlluxioURI ufsMount = new AlluxioURI(mTestFolder.newFolder().getAbsolutePath());
    mFileSystemMaster.createDirectory(new AlluxioURI("/mnt/"), CreateDirectoryOptions.defaults());

    // Create ufs file
    Files.createFile(Paths.get(ufsMount.join("file").getPath()));

    // Created nested file
    Files.createDirectory(Paths.get(ufsMount.join("nested").getPath()));
    Files.createFile(Paths.get(ufsMount.join("nested").join("file").getPath()));

    mFileSystemMaster.mount(new AlluxioURI("/mnt/local"), ufsMount, MountOptions.defaults());

    // Test simple file.
    AlluxioURI uri = new AlluxioURI("/mnt/local/file");
    mFileSystemMaster.loadMetadata(uri, LoadMetadataOptions.defaults().setCreateAncestors(false));
    Assert.assertNotNull(mFileSystemMaster.getFileInfo(uri));

    // Test nested file.
    uri = new AlluxioURI("/mnt/local/nested/file");
    try {
      mFileSystemMaster.loadMetadata(uri, LoadMetadataOptions.defaults().setCreateAncestors(false));
      Assert.fail("loadMetadata() without recursive, for a nested file should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }

    // Test the nested file with recursive flag.
    mFileSystemMaster.loadMetadata(uri, LoadMetadataOptions.defaults().setCreateAncestors(true));
    Assert.assertNotNull(mFileSystemMaster.getFileInfo(uri));
  }

  /**
   * Tests that an exception is in the
   * {@link FileSystemMaster#createFile(AlluxioURI, CreateFileOptions)} with a TTL set in the
   * {@link CreateFileOptions} after the TTL check was done once.
   */
  @Test
  public void createFileWithTtl() throws Exception {
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setRecursive(true).setTtl(0);
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, options);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(fileInfo.getFileId(), fileId);

    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * Tests that an exception is thrown when trying to get information about a file after it has been
   * deleted because of a TTL of 0.
   */
  @Test
  public void setTtlForFileWithNoTtl() throws Exception {
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setRecursive(true);
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, options);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // Since no TTL is set, the file should not be deleted.
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(NESTED_FILE_URI).getFileId());

    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeOptions.defaults().setTtl(0));
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
    CreateDirectoryOptions createDirectoryOptions =
            CreateDirectoryOptions.defaults().setRecursive(true);
    mFileSystemMaster.createDirectory(NESTED_URI, createDirectoryOptions);
    mFileSystemMaster.createDirectory(NESTED_DIR_URI, createDirectoryOptions);
    CreateFileOptions createFileOptions =
            CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setRecursive(true);
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, createFileOptions);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // Since no TTL is set, the file should not be deleted.
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(NESTED_FILE_URI).getFileId());
    // Set ttl
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeOptions.defaults().setTtl(0));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // TTL is set to 0, the file and directory should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(NESTED_URI);
    mFileSystemMaster.getFileInfo(NESTED_DIR_URI);
    mFileSystemMaster.getFileInfo(NESTED_FILE_URI);
  }

  /**
   * Tests that an exception is thrown when trying to get information about a file after it has been
   * deleted after the TTL has been set to 0.
   */
  @Test
  public void setSmallerTtlForFileWithTtl() throws Exception {
    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB)
        .setRecursive(true).setTtl(Constants.HOUR_MS);
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, options);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // Since TTL is 1 hour, the file won't be deleted during last TTL check.
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(NESTED_FILE_URI).getFileId());

    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeOptions.defaults().setTtl(0));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // TTL is reset to 0, the file should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * Tests that file information is still present after it has been freed after the TTL has been set
   * to 0.
   */
  @Test
  public void setTtlForFileWithFreeOperationTest() throws Exception {
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    Assert.assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());
    // Set ttl & operation
    SetAttributeOptions options = SetAttributeOptions.defaults();
    options.setTtl(0);
    options.setTtlAction(TtlAction.FREE);
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, options);
    Command heartBeat =
        mBlockMaster.workerHeartbeat(mWorkerId1, ImmutableMap.of("MEM", (long) Constants.KB),
            ImmutableList.of(blockId), ImmutableMap.<String, List<Long>>of());
    // Verify the muted Free command on worker1
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat);
    Assert.assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests that file information is still present after it has been freed after the parent
   * directory's TTL has been set to 0.
   */
  @Test
  public void setTtlForDirectoryWithFreeOperationTest() throws Exception {
    CreateDirectoryOptions createDirectoryOptions =
            CreateDirectoryOptions.defaults().setRecursive(true);
    mFileSystemMaster.createDirectory(NESTED_URI, createDirectoryOptions);
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    Assert.assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());
    // Set ttl & operation
    SetAttributeOptions options = SetAttributeOptions.defaults();
    options.setTtl(0);
    options.setTtlAction(TtlAction.FREE);
    mFileSystemMaster.setAttribute(NESTED_URI, options);
    Command heartBeat =
            mBlockMaster.workerHeartbeat(mWorkerId1, ImmutableMap.of("MEM", (long) Constants.KB),
                    ImmutableList.of(blockId), ImmutableMap.<String, List<Long>>of());
    // Verify the muted Free command on worker1
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat);
    Assert.assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests that a file has not been deleted after the TTL has been reset to a valid value.
   */
  @Test
  public void setLargerTtlForFileWithTtl() throws Exception {
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setRecursive(true).setTtl(0);
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, options);
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(NESTED_FILE_URI).getFileId());

    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeOptions.defaults().setTtl(Constants.HOUR_MS));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    // TTL is reset to 1 hour, the file should not be deleted during last TTL check.
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).getFileId());
  }

  /**
   * Tests that the original TTL is removed after setting it to {@link Constants#NO_TTL} for a file.
   */
  @Test
  public void setNoTtlForFileWithTtl() throws Exception {
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setRecursive(true).setTtl(0);
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, options);
    // After setting TTL to NO_TTL, the original TTL will be removed, and the file will not be
    // deleted during next TTL check.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeOptions.defaults().setTtl(Constants.NO_TTL));
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).getFileId());
  }

  /**
   * Tests the {@link FileSystemMaster#setAttribute(AlluxioURI, SetAttributeOptions)} method and
   * that an exception is thrown when trying to set a TTL for a directory.
   */
  @Test
  public void setAttribute() throws Exception {
    mFileSystemMaster.createFile(NESTED_FILE_URI, sNestedFileOptions);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI);
    Assert.assertFalse(fileInfo.isPinned());
    Assert.assertEquals(Constants.NO_TTL, fileInfo.getTtl());

    // No State.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeOptions.defaults());
    fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI);
    Assert.assertFalse(fileInfo.isPinned());
    Assert.assertEquals(Constants.NO_TTL, fileInfo.getTtl());

    // Just set pinned flag.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeOptions.defaults().setPinned(true));
    fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI);
    Assert.assertTrue(fileInfo.isPinned());
    Assert.assertEquals(Constants.NO_TTL, fileInfo.getTtl());

    // Both pinned flag and ttl value.
    mFileSystemMaster.setAttribute(NESTED_FILE_URI,
        SetAttributeOptions.defaults().setPinned(false).setTtl(1));
    fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI);
    Assert.assertFalse(fileInfo.isPinned());
    Assert.assertEquals(1, fileInfo.getTtl());

    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeOptions.defaults().setTtl(1));
  }

  /**
   * Tests the permission bits are 0777 for directories and 0666 for files with UMASK 000.
   */
  @Test
  public void permission() throws Exception {
    mFileSystemMaster.createFile(NESTED_FILE_URI, sNestedFileOptions);
    Assert.assertEquals(0777, mFileSystemMaster.getFileInfo(NESTED_URI).getMode());
    Assert.assertEquals(0666, mFileSystemMaster.getFileInfo(NESTED_FILE_URI).getMode());
  }

  /**
   * Tests that a file is fully written to memory.
   */
  @Test
  public void isFullyInMemory() throws Exception {
    // add nested file
    mFileSystemMaster.createFile(NESTED_FILE_URI, sNestedFileOptions);
    // add in-memory block
    long blockId = mFileSystemMaster.getNewBlockIdForFile(NESTED_FILE_URI);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB, "MEM", blockId, Constants.KB);
    // add SSD block
    blockId = mFileSystemMaster.getNewBlockIdForFile(NESTED_FILE_URI);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB, "SSD", blockId, Constants.KB);
    mFileSystemMaster.completeFile(NESTED_FILE_URI, CompleteFileOptions.defaults());

    // Create 2 files in memory.
    createFileWithSingleBlock(ROOT_FILE_URI);
    AlluxioURI nestedMemUri = NESTED_URI.join("mem_file");
    createFileWithSingleBlock(nestedMemUri);
    Assert.assertEquals(2, mFileSystemMaster.getInMemoryFiles().size());
    Assert.assertTrue(mFileSystemMaster.getInMemoryFiles().contains(ROOT_FILE_URI));
    Assert.assertTrue(mFileSystemMaster.getInMemoryFiles().contains(nestedMemUri));
  }

  /**
   * Tests the {@link FileSystemMaster#rename(AlluxioURI, AlluxioURI)} method.
   */
  @Test
  public void rename() throws Exception {
    mFileSystemMaster.createFile(NESTED_FILE_URI, sNestedFileOptions);

    // try to rename a file to root
    try {
      mFileSystemMaster.rename(NESTED_FILE_URI, ROOT_URI);
      Assert.fail("Renaming to root should fail.");
    } catch (InvalidPathException e) {
      Assert.assertEquals(ExceptionMessage.RENAME_CANNOT_BE_TO_ROOT.getMessage(), e.getMessage());
    }

    // move root to another path
    try {
      mFileSystemMaster.rename(ROOT_URI, TEST_URI);
      Assert.fail("Should not be able to rename root");
    } catch (InvalidPathException e) {
      Assert.assertEquals(ExceptionMessage.ROOT_CANNOT_BE_RENAMED.getMessage(), e.getMessage());
    }

    // move to existing path
    try {
      mFileSystemMaster.rename(NESTED_FILE_URI, NESTED_URI);
      Assert.fail("Should not be able to overwrite existing file.");
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(NESTED_URI.getPath()),
          e.getMessage());
    }

    // move a nested file to a root file
    mFileSystemMaster.rename(NESTED_FILE_URI, TEST_URI);
    Assert.assertEquals(mFileSystemMaster.getFileInfo(TEST_URI).getPath(), TEST_URI.getPath());

    // move a file where the dst is lexicographically earlier than the source
    AlluxioURI newDst = new AlluxioURI("/abc_test");
    mFileSystemMaster.rename(TEST_URI, newDst);
    Assert.assertEquals(mFileSystemMaster.getFileInfo(newDst).getPath(), newDst.getPath());
  }

  /**
   * Tests that an exception is thrown when trying to create a file in a non-existing directory
   * without setting the {@code recursive} flag.
   */
  @Test
  public void renameUnderNonexistingDir() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mThrown.expectMessage(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/nested/test"));

    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB);
    mFileSystemMaster.createFile(TEST_URI, options);

    // nested dir
    mFileSystemMaster.rename(TEST_URI, NESTED_FILE_URI);
  }

  @Test
  public void renameToNonExistentParent() throws Exception {
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setRecursive(true);
    mFileSystemMaster.createFile(NESTED_URI, options);

    try {
      mFileSystemMaster.rename(NESTED_URI, new AlluxioURI("/testDNE/b"));
      Assert.fail("Rename to a non-existent parent path should not succeed.");
    } catch (FileDoesNotExistException e) {
      // Expected case
    }
  }

  /**
   * Tests that an exception is thrown when trying to rename a file to a prefix of the original
   * file.
   */
  @Test
  public void renameToSubpath() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Traversal failed. Component 2(test) is a file");

    mFileSystemMaster.createFile(NESTED_URI, sNestedFileOptions);
    mFileSystemMaster.rename(NESTED_URI, NESTED_FILE_URI);
  }

  /**
   * Tests the {@link FileSystemMaster#free(AlluxioURI, boolean)} method.
   */
  @Test
  public void free() throws Exception {
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    Assert.assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // cannot free directory with recursive argument to false
    Assert.assertFalse(mFileSystemMaster.free(NESTED_FILE_URI.getParent(), false));

    // free the file
    Assert.assertTrue(mFileSystemMaster.free(NESTED_FILE_URI, false));
    // Update the heartbeat of removedBlockId received from worker 1
    Command heartBeat2 =
        mBlockMaster.workerHeartbeat(mWorkerId1, ImmutableMap.of("MEM", (long) Constants.KB),
            ImmutableList.of(blockId), ImmutableMap.<String, List<Long>>of());
    // Verify the muted Free command on worker1
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat2);
    Assert.assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests the {@link FileSystemMaster#free(AlluxioURI, boolean)} method with a directory.
   */
  @Test
  public void freeDir() throws Exception {
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    Assert.assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // free the dir
    Assert.assertTrue(mFileSystemMaster.free(NESTED_FILE_URI.getParent(), true));
    // Update the heartbeat of removedBlockId received from worker 1
    Command heartBeat3 =
        mBlockMaster.workerHeartbeat(mWorkerId1, ImmutableMap.of("MEM", (long) Constants.KB),
            ImmutableList.of(blockId), ImmutableMap.<String, List<Long>>of());
    // Verify the muted Free command on worker1
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat3);
    Assert.assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests the {@link FileSystemMaster#mount(AlluxioURI, AlluxioURI, MountOptions)} method.
   */
  @Test
  public void mount() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountOptions.defaults());
  }

  /**
   * Tests mounting an existing dir.
   */
  @Test
  public void mountExistingDir() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    mFileSystemMaster.createDirectory(alluxioURI, CreateDirectoryOptions.defaults());
    mThrown.expect(InvalidPathException.class);
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountOptions.defaults());
  }

  /**
   * Tests mounting to an Alluxio path whose parent dir does not exist.
   */
  @Test
  public void mountNonExistingParentDir() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/non-existing/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountOptions.defaults());
  }

  /**
   * Tests mounting a shadow Alluxio dir.
   */
  @Test
  public void mountShadowDir() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountOptions.defaults());
    AlluxioURI shadowAlluxioURI = new AlluxioURI("/hello/shadow");
    AlluxioURI anotherUfsURI = createTempUfsDir("ufs/hi");
    mThrown.expect(InvalidPathException.class);
    mFileSystemMaster.mount(shadowAlluxioURI, anotherUfsURI, MountOptions.defaults());
  }

  /**
   * Tests mounting a prefix UFS dir.
   */
  @Test
  public void mountPrefixUfsDir() throws Exception {
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello/shadow");
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountOptions.defaults());
    AlluxioURI preUfsURI = ufsURI.getParent();
    AlluxioURI anotherAlluxioURI = new AlluxioURI("/hi");
    mThrown.expect(InvalidPathException.class);
    mFileSystemMaster.mount(anotherAlluxioURI, preUfsURI, MountOptions.defaults());
  }

  /**
   * Tests mounting a suffix UFS dir.
   */
  @Test
  public void mountSuffixUfsDir() throws Exception {
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello/shadow");
    AlluxioURI preUfsURI = ufsURI.getParent();
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    mFileSystemMaster.mount(alluxioURI, preUfsURI, MountOptions.defaults());
    AlluxioURI anotherAlluxioURI = new AlluxioURI("/hi");
    mThrown.expect(InvalidPathException.class);
    mFileSystemMaster.mount(anotherAlluxioURI, ufsURI, MountOptions.defaults());
  }

  /**
   * Tests unmounting operation.
   */
  @Test
  public void unmount() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountOptions.defaults());
    AlluxioURI dirURI = new AlluxioURI("dir");
    mFileSystemMaster.createDirectory(new AlluxioURI(alluxioURI, dirURI),
        CreateDirectoryOptions.defaults().setPersisted(true));
    mFileSystemMaster.unmount(alluxioURI);
    AlluxioURI ufsDirURI = new AlluxioURI(ufsURI, dirURI);
    File file = new File(ufsDirURI.toString());
    Assert.assertTrue(file.exists());
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
    return new AlluxioURI(path);
  }

  /**
   * Tests the {@link FileSystemMaster#stop()} method.
   */
  @Test
  public void stop() throws Exception {
    mFileSystemMaster.stop();
    Assert.assertTrue(mExecutorService.isShutdown());
    Assert.assertTrue(mExecutorService.isTerminated());
  }

  /**
   * Tests the {@link FileSystemMaster#workerHeartbeat(long, List)} method.
   */
  @Test
  public void workerHeartbeat() throws Exception {
    long blockId = createFileWithSingleBlock(ROOT_FILE_URI);

    long fileId = mFileSystemMaster.getFileId(ROOT_FILE_URI);
    mFileSystemMaster.scheduleAsyncPersistence(ROOT_FILE_URI);

    FileSystemCommand command =
        mFileSystemMaster.workerHeartbeat(mWorkerId1, Lists.newArrayList(fileId));
    Assert.assertEquals(CommandType.Persist, command.getCommandType());
    Assert.assertEquals(1,
        command.getCommandOptions().getPersistOptions().getPersistFiles().size());
    Assert.assertEquals(fileId,
        command.getCommandOptions().getPersistOptions().getPersistFiles().get(0).getFileId());
    Assert.assertEquals(blockId, (long) command.getCommandOptions().getPersistOptions()
        .getPersistFiles().get(0).getBlockIds().get(0));
  }

  /**
   * Tests that lost files can successfully be detected.
   */
  @Test
  public void lostFilesDetection() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    long fileId = mFileSystemMaster.getFileId(NESTED_FILE_URI);
    mFileSystemMaster.reportLostFile(fileId);

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.name(), fileInfo.getPersistenceState());
    // Check with getPersistenceState
    Assert.assertEquals(PersistenceState.NOT_PERSISTED,
        mFileSystemMaster.getPersistenceState(fileId));

    // run the detector
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_LOST_FILES_DETECTION);

    fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(PersistenceState.LOST.name(), fileInfo.getPersistenceState());
    // Check with getPersistenceState
    Assert.assertEquals(PersistenceState.LOST, mFileSystemMaster.getPersistenceState(fileId));
  }

  /**
   * Tests load metadata logic.
   */
  @Test
  public void testLoadMetadata() throws Exception {
    FileUtils.createDir(Paths.get(mUnderFS).resolve("a").toString());
    mFileSystemMaster.loadMetadata(new AlluxioURI("alluxio:/a"),
        LoadMetadataOptions.defaults().setCreateAncestors(true));
    mFileSystemMaster.loadMetadata(new AlluxioURI("alluxio:/a"),
        LoadMetadataOptions.defaults().setCreateAncestors(true));

    // TODO(peis): Avoid this hack by adding an option in getFileInfo to skip loading metadata.
    try {
      mFileSystemMaster.createDirectory(new AlluxioURI("alluxio:/a"),
          CreateDirectoryOptions.defaults());
      Assert.fail("createDirectory was expected to fail with FileAlreadyExistsException");
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(
          ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(new AlluxioURI("alluxio:/a")),
          e.getMessage());
    }

    FileUtils.createFile(Paths.get(mUnderFS).resolve("a/f1").toString());
    FileUtils.createFile(Paths.get(mUnderFS).resolve("a/f2").toString());

    mFileSystemMaster.loadMetadata(new AlluxioURI("alluxio:/a/f1"),
        LoadMetadataOptions.defaults().setCreateAncestors(true));

    // This should not throw file exists exception those a/f1 is loaded.
    mFileSystemMaster.loadMetadata(new AlluxioURI("alluxio:/a"),
        LoadMetadataOptions.defaults().setCreateAncestors(true).setLoadDirectChildren(true));

    // TODO(peis): Avoid this hack by adding an option in getFileInfo to skip loading metadata.
    try {
      mFileSystemMaster.createFile(new AlluxioURI("alluxio:/a/f2"), CreateFileOptions.defaults());
      Assert.fail("createDirectory was expected to fail with FileAlreadyExistsException");
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(
          ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(new AlluxioURI("alluxio:/a/f2")),
          e.getMessage());
    }

    mFileSystemMaster.loadMetadata(new AlluxioURI("alluxio:/a"),
        LoadMetadataOptions.defaults().setCreateAncestors(true).setLoadDirectChildren(true));
  }

  /**
   * Tests load root metadata. It should not fail.
   */
  @Test
  public void loadRoot() throws Exception {
    mFileSystemMaster.loadMetadata(new AlluxioURI("alluxio:/"), LoadMetadataOptions.defaults());
  }

  private long createFileWithSingleBlock(AlluxioURI uri) throws Exception {
    mFileSystemMaster.createFile(uri, sNestedFileOptions);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(uri);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB, "MEM", blockId, Constants.KB);
    CompleteFileOptions options = CompleteFileOptions.defaults().setUfsLength(Constants.KB);
    mFileSystemMaster.completeFile(uri, options);
    return blockId;
  }

  /**
   * Uses the integer suffix of a path to determine order. Paths without integer suffixes will be
   * ordered last.
   */
  private class IntegerSuffixedPathComparator implements Comparator<FileInfo> {
    @Override
    public int compare(FileInfo o1, FileInfo o2) {
      return extractIntegerSuffix(o1.getName()) - extractIntegerSuffix(o2.getName());
    }

    private int extractIntegerSuffix(String name) {
      Pattern p = Pattern.compile("\\D*(\\d+$)");
      Matcher m = p.matcher(name);
      if (m.matches()) {
        return Integer.parseInt(m.group(1));
      } else {
        return Integer.MAX_VALUE;
      }
    }
  }

  /**
   * Tests concurrent renames within the root do not block on each other.
   */
  @Test
  public void rootConcurrentRename() throws Exception {
    final int numThreads = 100;
    AlluxioURI[] srcs = new AlluxioURI[numThreads];
    AlluxioURI[] dsts = new AlluxioURI[numThreads];

    for (int i = 0; i < numThreads; i++) {
      srcs[i] = new AlluxioURI("/file" + i);
      mFileSystemMaster.createFile(srcs[i], sNestedFileOptions);
      dsts[i] = new AlluxioURI("/renamed" + i);
    }

    int errors = concurrentRename(srcs, dsts);

    Assert.assertEquals("More than 0 errors: " + errors, 0, errors);
    List<FileInfo> files =
        mFileSystemMaster.listStatus(new AlluxioURI("/"), ListStatusOptions.defaults());
    Collections.sort(files, new IntegerSuffixedPathComparator());
    for (int i = 0; i < numThreads; i++) {
      Assert.assertEquals(dsts[i].getName(), files.get(i).getName());
    }
    Assert.assertEquals(numThreads, files.size());
  }

  /**
   * Tests concurrent renames within a folder do not block on each other.
   */
  @Test
  public void folderConcurrentRename() throws Exception {
    final int numThreads = 100;
    AlluxioURI[] srcs = new AlluxioURI[numThreads];
    AlluxioURI[] dsts = new AlluxioURI[numThreads];

    AlluxioURI dir = new AlluxioURI("/dir");

    mFileSystemMaster.createDirectory(dir, CreateDirectoryOptions.defaults());

    for (int i = 0; i < numThreads; i++) {
      srcs[i] = dir.join("/file" + i);
      mFileSystemMaster.createFile(srcs[i], sNestedFileOptions);
      dsts[i] = dir.join("/renamed" + i);
    }
    int errors = concurrentRename(srcs, dsts);

    Assert.assertEquals("More than 0 errors: " + errors, 0, errors);
    List<FileInfo> files =
        mFileSystemMaster.listStatus(dir, ListStatusOptions.defaults());
    Collections.sort(files, new IntegerSuffixedPathComparator());
    for (int i = 0; i < numThreads; i++) {
      Assert.assertEquals(dsts[i].getName(), files.get(i).getName());
    }
    Assert.assertEquals(numThreads, files.size());
  }

  /**
   * Tests that many threads concurrently renaming the same file will only succeed once.
   */
  @Test
  public void sameFileConcurrentRename() throws Exception {
    int numThreads = 10;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    for (int i = 0; i < numThreads; i++) {
      srcs[i] = new AlluxioURI("/file");
      dsts[i] = new AlluxioURI("/renamed" + i);
    }

    // Create the one source file
    mFileSystemMaster.createFile(srcs[0], CreateFileOptions.defaults());

    int errors = concurrentRename(srcs, dsts);

    // We should get an error for all but 1 rename
    Assert.assertEquals(numThreads - 1, errors);

    List<FileInfo> files =
        mFileSystemMaster.listStatus(new AlluxioURI("/"), ListStatusOptions.defaults());

    // Only one renamed file should exist
    Assert.assertEquals(1, files.size());
    Assert.assertTrue(files.get(0).getName().startsWith("renamed"));
  }

  /**
   * Tests that many threads concurrently renaming the same directory will only succeed once.
   */
  @Test
  public void sameDirConcurrentRename() throws Exception {
    int numThreads = 10;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    for (int i = 0; i < numThreads; i++) {
      srcs[i] = new AlluxioURI("/dir");
      dsts[i] = new AlluxioURI("/renamed" + i);
    }

    // Create the one source directory
    mFileSystemMaster.createDirectory(srcs[0], CreateDirectoryOptions.defaults());
    mFileSystemMaster.createFile(new AlluxioURI("/dir/file"), CreateFileOptions.defaults());

    int errors = concurrentRename(srcs, dsts);

    // We should get an error for all but 1 rename
    Assert.assertEquals(numThreads - 1, errors);
    // Only one renamed dir should exist
    List<FileInfo> existingDirs =
        mFileSystemMaster.listStatus(new AlluxioURI("/"), ListStatusOptions.defaults());
    Assert.assertEquals(1, existingDirs.size());
    Assert.assertTrue(existingDirs.get(0).getName().startsWith("renamed"));
    // The directory should contain the file
    List<FileInfo> dirChildren =
        mFileSystemMaster.listStatus(new AlluxioURI(existingDirs.get(0).getPath()),
            ListStatusOptions.defaults());
    Assert.assertEquals(1, dirChildren.size());
  }

  /**
   * Tests renaming files concurrently to the same destination will only succeed once.
   */
  @Test
  public void sameDstConcurrentRename() throws Exception {
    int numThreads = 10;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    for (int i = 0; i < numThreads; i++) {
      srcs[i] = new AlluxioURI("/file" + i);
      mFileSystemMaster.createFile(srcs[i], CreateFileOptions.defaults());
      dsts[i] = new AlluxioURI("/renamed");
    }

    int errors = concurrentRename(srcs, dsts);

    // We should get an error for all but 1 rename.
    Assert.assertEquals(numThreads - 1, errors);

    List<FileInfo> files =
        mFileSystemMaster.listStatus(new AlluxioURI("/"), ListStatusOptions.defaults());
    // Store file names in a set to ensure the names are all unique.
    Set<String> renamedFiles = new HashSet<>();
    Set<String> originalFiles = new HashSet<>();
    for (FileInfo file : files) {
      if (file.getName().startsWith("renamed")) {
        renamedFiles.add(file.getName());
      }
      if (file.getName().startsWith("file")) {
        originalFiles.add(file.getName());
      }
    }
    // One renamed file should exist, and 9 original source files
    Assert.assertEquals(numThreads, files.size());
    Assert.assertEquals(1, renamedFiles.size());
    Assert.assertEquals(numThreads - 1, originalFiles.size());
  }

  /**
   * Tests renaming files concurrently to the same destination from different sources will
   * succeed only once.
   */
  @Test
  public void sameDstDifferentSrcConcurrentRename() throws Exception {
    int numThreads = 10;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    AlluxioURI dir1 = new AlluxioURI("/dir1");
    AlluxioURI dir2 = new AlluxioURI("/dir2");
    mFileSystemMaster.createDirectory(dir1, CreateDirectoryOptions.defaults());
    mFileSystemMaster.createDirectory(dir2, CreateDirectoryOptions.defaults());
    for (int i = 0; i < numThreads; i++) {
      if (i % 2 == 0) {
        srcs[i] = dir1.join("file1_" + i);
      } else {
        srcs[i] = dir2.join("file2_" + i);
      }
      mFileSystemMaster.createFile(srcs[i], CreateFileOptions.defaults());
      dsts[i] = new AlluxioURI("/renamed");
    }

    int errors = concurrentRename(srcs, dsts);

    // We should get an error for all but 1 rename.
    Assert.assertEquals(numThreads - 1, errors);

    List<FileInfo> files =
        mFileSystemMaster.listStatus(new AlluxioURI("/"), ListStatusOptions.defaults());
    // Store file names in a set to ensure the names are all unique.
    Set<String> renamedFiles = new HashSet<>();
    for (FileInfo file : files) {
      if (file.getName().startsWith("renamed")) {
        renamedFiles.add(file.getName());
      }
    }

    Set<String> originalFiles = new HashSet<>();

    List<FileInfo> dir1Files = mFileSystemMaster.listStatus(dir1, ListStatusOptions.defaults());
    for (FileInfo file : dir1Files) {
      if (file.getName().startsWith("file1_")) {
        originalFiles.add(file.getName());
      } else {
        // Should not have any other types of files
        Assert.fail();
      }
    }

    List<FileInfo> dir2Files = mFileSystemMaster.listStatus(dir2, ListStatusOptions.defaults());
    for (FileInfo file : dir2Files) {
      if (file.getName().startsWith("file2_")) {
        originalFiles.add(file.getName());
      } else {
        // Should not have any other types of files
        Assert.fail();
      }
    }

    // One renamed file should exist, and numThreads - 1 original source files
    Assert.assertEquals(numThreads, renamedFiles.size() + originalFiles.size());
    Assert.assertEquals(1, renamedFiles.size());
    Assert.assertEquals(numThreads - 1, originalFiles.size());
  }

  /**
   * Tests renaming files concurrently from one directory to another succeeds.
   */
  @Test
  public void twoDirConcurrentRename() throws Exception {
    int numThreads = 10;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    AlluxioURI dir1 = new AlluxioURI("/dir1");
    AlluxioURI dir2 = new AlluxioURI("/dir2");
    mFileSystemMaster.createDirectory(dir1, CreateDirectoryOptions.defaults());
    mFileSystemMaster.createDirectory(dir2, CreateDirectoryOptions.defaults());
    for (int i = 0; i < numThreads; i++) {
      srcs[i] = dir1.join("file" + i);
      mFileSystemMaster.createFile(srcs[i], CreateFileOptions.defaults());
      dsts[i] = dir2.join("renamed" + i);
    }

    int errors = concurrentRename(srcs, dsts);

    // We should get no errors
    Assert.assertEquals(0, errors);

    List<FileInfo> dir1Files =
        mFileSystemMaster.listStatus(dir1, ListStatusOptions.defaults());
    List<FileInfo> dir2Files =
        mFileSystemMaster.listStatus(dir2, ListStatusOptions.defaults());

    Assert.assertEquals(0, dir1Files.size());
    Assert.assertEquals(numThreads, dir2Files.size());

    Collections.sort(dir2Files, new IntegerSuffixedPathComparator());
    for (int i = 0; i < numThreads; i++) {
      Assert.assertEquals(dsts[i].getName(), dir2Files.get(i).getName());
    }
  }

  /**
   * Tests renaming files concurrently from and to two directories succeeds.
   */
  @Test
  public void acrossDirConcurrentRename() throws Exception {
    int numThreads = 20;
    final AlluxioURI[] srcs = new AlluxioURI[numThreads];
    final AlluxioURI[] dsts = new AlluxioURI[numThreads];
    AlluxioURI dir1 = new AlluxioURI("/dir1");
    AlluxioURI dir2 = new AlluxioURI("/dir2");
    mFileSystemMaster.createDirectory(dir1, CreateDirectoryOptions.defaults());
    mFileSystemMaster.createDirectory(dir2, CreateDirectoryOptions.defaults());
    for (int i = 0; i < numThreads; i++) {
      // Dir1 has even files, dir2 has odd files.
      if (i % 2 == 0) {
        srcs[i] = dir1.join("file" + i);
        dsts[i] = dir2.join("renamed" + i);
      } else {
        srcs[i] = dir2.join("file" + i);
        dsts[i] = dir1.join("renamed" + i);
      }
      mFileSystemMaster.createFile(srcs[i], CreateFileOptions.defaults());
    }

    int errors = concurrentRename(srcs, dsts);

    // We should get no errors.
    Assert.assertEquals(0, errors);

    List<FileInfo> dir1Files =
        mFileSystemMaster.listStatus(dir1, ListStatusOptions.defaults());
    List<FileInfo> dir2Files =
        mFileSystemMaster.listStatus(dir2, ListStatusOptions.defaults());

    Assert.assertEquals(numThreads / 2, dir1Files.size());
    Assert.assertEquals(numThreads / 2, dir2Files.size());

    Collections.sort(dir1Files, new IntegerSuffixedPathComparator());
    for (int i = 1; i < numThreads; i += 2) {
      Assert.assertEquals(dsts[i].getName(), dir1Files.get(i / 2).getName());
    }

    Collections.sort(dir2Files, new IntegerSuffixedPathComparator());
    for (int i = 0; i < numThreads; i += 2) {
      Assert.assertEquals(dsts[i].getName(), dir2Files.get(i / 2).getName());
    }
  }

  /**
   * Helper for renaming a list of paths concurrently. Assumes the srcs are already created and
   * dsts do not exist.
   *
   * @param src list of source paths
   * @param dst list of destination paths
   * @return how many errors occurred
   */
  private int concurrentRename(final AlluxioURI[] src, final AlluxioURI[] dst)
      throws Exception {
    final int numFiles = src.length;
    final CyclicBarrier barrier = new CyclicBarrier(numFiles);
    List<Thread> threads = new ArrayList<>(numFiles);
    // If there are exceptions, we will store them here.
    final ConcurrentHashSet<Throwable> errors = new ConcurrentHashSet<>();
    Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread th, Throwable ex) {
        errors.add(ex);
      }
    };
    for (int i = 0; i < numFiles; i++) {
      final int iteration = i;
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            AuthenticatedClientUser.set(TEST_USER);
            barrier.await();
            mFileSystemMaster.rename(src[iteration], dst[iteration]);
          } catch (Exception e) {
            Throwables.propagate(e);
          }
        }
      });
      t.setUncaughtExceptionHandler(exceptionHandler);
      threads.add(t);
    }
    Collections.shuffle(threads);
    for (Thread t : threads) {
      t.start();
    }
    for (Thread t : threads) {
      t.join();
    }
    return errors.size();
  }

  /**
   * Tests that getFileInfo (read operation) either returns the correct file info or fails if it
   * has been renamed while the operation was waiting for the file lock.
   */
  @Test
  public void consistentGetFileInfo() throws Exception {
    final int iterations = 100;
    final AlluxioURI file = new AlluxioURI("/file");
    final AlluxioURI dst = new AlluxioURI("/dst");
    final CyclicBarrier barrier = new CyclicBarrier(2);
    // If there are exceptions, we will store them here.
    final ConcurrentHashSet<Throwable> errors = new ConcurrentHashSet<>();
    Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread th, Throwable ex) {
        errors.add(ex);
      }
    };
    for (int i = 0; i < iterations; i++) {
      mFileSystemMaster.createFile(file, sNestedFileOptions);
      Thread renamer = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            AuthenticatedClientUser.set(TEST_USER);
            barrier.await();
            mFileSystemMaster.rename(file, dst);
            mFileSystemMaster.delete(dst, true);
          } catch (Exception e) {
            Assert.fail(e.getMessage());
          }
        }
      });
      renamer.setUncaughtExceptionHandler(exceptionHandler);
      Thread reader = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            AuthenticatedClientUser.set(TEST_USER);
            barrier.await();
            FileInfo info = mFileSystemMaster.getFileInfo(file);
            // If the file info is successfully obtained, then the path should match
            Assert.assertEquals(file.getName(), info.getName());
          } catch (InvalidPathException | FileDoesNotExistException e) {
            // InvalidPathException - if the file is renamed while the thread waits for the lock.
            // FileDoesNotExistException - if the file is fully renamed before the getFileInfo call.
          } catch (Exception e) {
            Assert.fail(e.getMessage());
          }
        }
      });
      reader.setUncaughtExceptionHandler(exceptionHandler);
      renamer.start();
      reader.start();
      renamer.join();
      reader.join();
      Assert.assertTrue("Errors detected: " + errors, errors.isEmpty());
    }
  }
}
