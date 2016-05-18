/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.exception.BlockInfoException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.MasterContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.PersistenceState;
import alluxio.master.file.meta.TtlBucket;
import alluxio.master.file.meta.TtlBucketPrivateAccess;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.LoadMetadataOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.master.journal.Journal;
import alluxio.master.journal.ReadWriteJournal;
import alluxio.thrift.Command;
import alluxio.thrift.CommandType;
import alluxio.thrift.FileSystemCommand;
import alluxio.util.IdUtils;
import alluxio.util.io.FileUtils;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;
import alluxio.wire.WorkerNetAddress;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link FileSystemMaster}.
 */
public final class FileSystemMasterTest {
  private static final long TTLCHECKER_INTERVAL_MS = 0;
  private static final AlluxioURI NESTED_URI = new AlluxioURI("/nested/test");
  private static final AlluxioURI NESTED_FILE_URI = new AlluxioURI("/nested/test/file");
  private static final AlluxioURI ROOT_URI = new AlluxioURI("/");
  private static final AlluxioURI ROOT_FILE_URI = new AlluxioURI("/file");
  private static final AlluxioURI TEST_URI = new AlluxioURI("/test");
  private static CreateFileOptions sNestedFileOptions;
  private static long sOldTtlIntervalMs;

  private BlockMaster mBlockMaster;
  private FileSystemMaster mFileSystemMaster;
  private long mWorkerId1;
  private long mWorkerId2;

  private String mUnderFS;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule = new ManuallyScheduleHeartbeat(
      HeartbeatContext.MASTER_TTL_CHECK,
      HeartbeatContext.MASTER_LOST_FILES_DETECTION);

  /**
   * Sets up the dependencies before a single test runs.
   */
  @BeforeClass
  public static void beforeClass() throws Exception {
    MasterContext.reset(new Configuration());
    sNestedFileOptions =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setRecursive(true);
    sOldTtlIntervalMs = TtlBucket.getTtlIntervalMs();
    TtlBucketPrivateAccess.setTtlIntervalMs(TTLCHECKER_INTERVAL_MS);
  }

  /**
   * Resets the TTL interval after all test ran.
   */
  @AfterClass
  public static void afterClass() {
    TtlBucketPrivateAccess.setTtlIntervalMs(sOldTtlIntervalMs);
  }

  /**
   * Resets the {@link MasterContext} after a test ran.
   */
  @After
  public void after() {
    MasterContext.reset();
  }

  /**
   * Sets up the dependencies before a test runs.
   *
   * @throws Exception if creating the temporary folder, starting the masters or register the
   *                   workers fails.
   */
  @Before
  public void before() throws Exception {
    mUnderFS = mTestFolder.newFolder().getAbsolutePath();
    MasterContext.getConf()
        .set(Constants.MASTER_TTL_CHECKER_INTERVAL_MS, String.valueOf(TTLCHECKER_INTERVAL_MS));
    MasterContext.getConf().set(Constants.UNDERFS_ADDRESS, mUnderFS);
    Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    Journal fsJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());

    mBlockMaster = new BlockMaster(blockJournal);
    mFileSystemMaster = new FileSystemMaster(mBlockMaster, fsJournal);

    mBlockMaster.start(true);
    mFileSystemMaster.start(true);

    // set up workers
    mWorkerId1 = mBlockMaster.getWorkerId(
        new WorkerNetAddress().setHost("localhost").setRpcPort(80).setDataPort(81).setWebPort(82));
    mBlockMaster.workerRegister(mWorkerId1, Arrays.asList("MEM", "SSD"),
        ImmutableMap.of("MEM", Constants.MB * 1L, "SSD", Constants.MB * 1L),
        ImmutableMap.of("MEM", Constants.KB * 1L, "SSD", Constants.KB * 1L),
        new HashMap<String, List<Long>>());
    mWorkerId2 = mBlockMaster.getWorkerId(
        new WorkerNetAddress().setHost("remote").setRpcPort(80).setDataPort(81).setWebPort(82));
    mBlockMaster.workerRegister(mWorkerId2, Arrays.asList("MEM", "SSD"),
        ImmutableMap.of("MEM", Constants.MB * 1L, "SSD", Constants.MB * 1L),
        ImmutableMap.of("MEM", Constants.KB * 1L, "SSD", Constants.KB * 1L),
        new HashMap<String, List<Long>>());
  }

  /**
   * Tests the {@link FileSystemMaster#delete(AlluxioURI, boolean)} method.
   *
   * @throws Exception if deleting a file fails
   */
  @Test
  public void deleteFileTest() throws Exception {
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
    Command heartBeat1 = mBlockMaster
        .workerHeartbeat(mWorkerId1, ImmutableMap.of("MEM", Constants.KB * 1L),
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
   *
   * @throws Exception if deleting a directory fails
   */
  @Test
  public void deleteNonemptyDirectoryTest() throws Exception {
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
   *
   * @throws Exception if deleting the directory fails
   */
  @Test
  public void deleteDirTest() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    // delete the dir
    mFileSystemMaster.delete(NESTED_URI, true);

    // verify the dir is deleted
    Assert.assertEquals(-1, mFileSystemMaster.getFileId(NESTED_URI));
  }

  /**
   * Tests the {@link FileSystemMaster#getNewBlockIdForFile(AlluxioURI)} method.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void getNewBlockIdForFileTest() throws Exception {
    mFileSystemMaster.createFile(NESTED_FILE_URI, sNestedFileOptions);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(NESTED_FILE_URI);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI);
    Assert.assertEquals(Lists.newArrayList(blockId), fileInfo.getBlockIds());
  }

  private void executeTtlCheckOnce() throws Exception {
    // Wait for the TTL check executor to be ready to execute its heartbeat.
    Assert.assertTrue(
        HeartbeatScheduler.await(HeartbeatContext.MASTER_TTL_CHECK, 1, TimeUnit.SECONDS));
    // Execute the TTL check executor heartbeat.
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_TTL_CHECK);
    // Wait for the TLL check executor to be ready to execute its heartbeat again. This is needed to
    // avoid a race between the subsequent test logic and the heartbeat thread.
    Assert.assertTrue(
        HeartbeatScheduler.await(HeartbeatContext.MASTER_TTL_CHECK, 1, TimeUnit.SECONDS));
  }

  @Test
  public void getPathTest() throws Exception {
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

  @Test
  public void getPersistenceStateTest() throws Exception {
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

  @Test
  public void getFileIdTest() throws Exception {
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

  @Test
  public void getFileInfoTest() throws Exception {
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
  public void getFileInfoWithLoadMetadataTest() throws Exception {
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
  public void getFileIdWithLoadMetadataTest() throws Exception {
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
  public void getFileInfoListTest() throws Exception {
    final int files = 10;
    List<FileInfo> infos;
    List<String> filenames;

    // Test files in root directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(ROOT_URI.join("file" + String.format("%05d", i)));
    }
    infos = mFileSystemMaster.getFileInfoList(ROOT_URI, false);
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
    infos = mFileSystemMaster.getFileInfoList(ROOT_FILE_URI, false);
    Assert.assertEquals(1, infos.size());
    Assert.assertEquals(ROOT_FILE_URI.getPath(), infos.get(0).getPath());

    // Test files in nested directory.
    for (int i = 0; i < files; i++) {
      createFileWithSingleBlock(NESTED_URI.join("file" + String.format("%05d", i)));
    }
    infos = mFileSystemMaster.getFileInfoList(NESTED_URI, false);
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
      mFileSystemMaster.getFileInfoList(NESTED_URI.join("DNE"), false);
      Assert.fail("getFileInfoList() for a non-existent URI should fail.");
    } catch (FileDoesNotExistException e) {
      // Expected case.
    }
  }

  @Test
  public void getFileBlockInfoListTest() throws Exception {
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
  public void mountUnmountTest() throws Exception {
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
  public void loadMetadataTest() throws Exception {
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
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void createFileWithTtlTest() throws Exception {
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setRecursive(true).setTtl(0);
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, options);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(fileInfo.getFileId(), fileId);

    executeTtlCheckOnce();
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * Tests that an exception is thrown when trying to get information about a file after it has been
   * deleted because of a TTL of 0.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  @Ignore("https://alluxio.atlassian.net/browse/ALLUXIO-1914")
  public void setTtlForFileWithNoTtlTest() throws Exception {
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setRecursive(true);
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, options);
    executeTtlCheckOnce();
    // Since no TTL is set, the file should not be deleted.
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(NESTED_FILE_URI).getFileId());

    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeOptions.defaults().setTtl(0));
    executeTtlCheckOnce();
    // TTL is set to 0, the file should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * Tests that an exception is thrown when trying to get information about a file after it has been
   * deleted after the TTL has been set to 0.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void setSmallerTtlForFileWithTtlTest() throws Exception {
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setRecursive(true)
            .setTtl(Constants.HOUR_MS);
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, options);
    executeTtlCheckOnce();
    // Since TTL is 1 hour, the file won't be deleted during last TTL check.
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(NESTED_FILE_URI).getFileId());

    mFileSystemMaster.setAttribute(NESTED_FILE_URI, SetAttributeOptions.defaults().setTtl(0));
    executeTtlCheckOnce();
    // TTL is reset to 0, the file should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  /**
   * Tests that a file has not been deleted after the TTL has been reset to a valid value.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void setLargerTtlForFileWithTtlTest() throws Exception {
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setRecursive(true).setTtl(0);
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, options);
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(NESTED_FILE_URI).getFileId());

    mFileSystemMaster
        .setAttribute(NESTED_FILE_URI, SetAttributeOptions.defaults().setTtl(Constants.HOUR_MS));
    executeTtlCheckOnce();
    // TTL is reset to 1 hour, the file should not be deleted during last TTL check.
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).getFileId());
  }

  /**
   * Tests that the original TTL is removed after setting it to {@link Constants#NO_TTL} for a file.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void setNoTtlForFileWithTtlTest() throws Exception {
    CreateFileOptions options =
        CreateFileOptions.defaults().setBlockSizeBytes(Constants.KB).setRecursive(true).setTtl(0);
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, options);
    // After setting TTL to NO_TTL, the original TTL will be removed, and the file will not be
    // deleted during next TTL check.
    mFileSystemMaster
        .setAttribute(NESTED_FILE_URI, SetAttributeOptions.defaults().setTtl(Constants.NO_TTL));
    executeTtlCheckOnce();
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).getFileId());
  }

  /**
   * Tests the {@link FileSystemMaster#setAttribute(AlluxioURI, SetAttributeOptions)} method and
   * that an exception is thrown when trying to set a TTL for a directory.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void setAttributeTest() throws Exception {
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
    mFileSystemMaster
        .setAttribute(NESTED_FILE_URI, SetAttributeOptions.defaults().setPinned(false).setTtl(1));
    fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI);
    Assert.assertFalse(fileInfo.isPinned());
    Assert.assertEquals(1, fileInfo.getTtl());

    // Set ttl for a directory, raise IllegalArgumentException.
    mThrown.expect(IllegalArgumentException.class);
    mFileSystemMaster.setAttribute(NESTED_URI, SetAttributeOptions.defaults().setTtl(1));
  }

  /**
   * Tests the permission bits are 0755 for directories and 0644 for files by default.
   */
  @Test
  public void permissionTest() throws Exception {
    mFileSystemMaster.createFile(NESTED_FILE_URI, sNestedFileOptions);
    Assert.assertEquals(0755, mFileSystemMaster.getFileInfo(NESTED_URI).getPermission());
    Assert.assertEquals(0644, mFileSystemMaster.getFileInfo(NESTED_FILE_URI).getPermission());
  }

  /**
   * Tests that a file is fully written to memory.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void isFullyInMemoryTest() throws Exception {
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
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void renameTest() throws Exception {
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
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
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

  /**
   * Tests that an exception is thrown when trying to rename a file to a prefix of the original
   * file.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void renameToSubpathTest() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Traversal failed. Component 2(test) is a file");

    mFileSystemMaster.createFile(NESTED_URI, sNestedFileOptions);
    mFileSystemMaster.rename(NESTED_URI, NESTED_FILE_URI);
  }

  /**
   * Tests the {@link FileSystemMaster#free(AlluxioURI, boolean)} method.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void freeTest() throws Exception {
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    Assert.assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // cannot free directory with recursive argument to false
    Assert.assertFalse(mFileSystemMaster.free(NESTED_FILE_URI.getParent(), false));

    // free the file
    Assert.assertTrue(mFileSystemMaster.free(NESTED_FILE_URI, false));
    // Update the heartbeat of removedBlockId received from worker 1
    Command heartBeat2 = mBlockMaster
        .workerHeartbeat(mWorkerId1, ImmutableMap.of("MEM", Constants.KB * 1L),
            ImmutableList.of(blockId), ImmutableMap.<String, List<Long>>of());
    // Verify the muted Free command on worker1
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat2);
    Assert.assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests the {@link FileSystemMaster#free(AlluxioURI, boolean)} method with a directory.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void freeDirTest() throws Exception {
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    Assert.assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // free the dir
    Assert.assertTrue(mFileSystemMaster.free(NESTED_FILE_URI.getParent(), true));
    // Update the heartbeat of removedBlockId received from worker 1
    Command heartBeat3 = mBlockMaster
        .workerHeartbeat(mWorkerId1, ImmutableMap.of("MEM", Constants.KB * 1L),
            ImmutableList.of(blockId), ImmutableMap.<String, List<Long>>of());
    // Verify the muted Free command on worker1
    Assert.assertEquals(new Command(CommandType.Nothing, ImmutableList.<Long>of()), heartBeat3);
    Assert.assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests the {@link FileSystemMaster#mount(AlluxioURI, AlluxioURI, MountOptions)} method.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void mountTest() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountOptions.defaults());
  }

  /**
   * Tests mounting an existing dir.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void mountExistingDirTest() throws Exception {
    AlluxioURI alluxioURI = new AlluxioURI("/hello");
    mFileSystemMaster.createDirectory(alluxioURI, CreateDirectoryOptions.defaults());
    mThrown.expect(InvalidPathException.class);
    AlluxioURI ufsURI = createTempUfsDir("ufs/hello");
    mFileSystemMaster.mount(alluxioURI, ufsURI, MountOptions.defaults());
  }

  /**
   * Tests mounting a shadow Alluxio dir.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void mountShadowDirTest() throws Exception {
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
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void mountPrefixUfsDirTest() throws Exception {
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
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void mountSuffixUfsDirTest() throws Exception {
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
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void unmountTest() throws Exception {
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
   * @throws IOException if {@link TemporaryFolder#newFolder(String...)} operation fails
   */
  private AlluxioURI createTempUfsDir(String ufsPath) throws IOException {
    String path = mTestFolder.newFolder(ufsPath.split("/")).getPath();
    return new AlluxioURI(path);
  }

  /**
   * Tests the {@link FileSystemMaster#stop()} method.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void stopTest() throws Exception {
    ExecutorService service =
        (ExecutorService) Whitebox.getInternalState(mFileSystemMaster, "mExecutorService");
    Future<?> ttlThread =
        (Future<?>) Whitebox.getInternalState(mFileSystemMaster, "mTtlCheckerService");
    Assert.assertFalse(ttlThread.isDone());
    Assert.assertFalse(service.isShutdown());
    mFileSystemMaster.stop();
    Assert.assertTrue(ttlThread.isDone());
    Assert.assertTrue(service.isShutdown());
  }

  /**
   * Tests the {@link FileSystemMaster#workerHeartbeat(long, List)} method.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void workerHeartbeatTest() throws Exception {
    long blockId = createFileWithSingleBlock(ROOT_FILE_URI);

    long fileId = mFileSystemMaster.getFileId(ROOT_FILE_URI);
    mFileSystemMaster.scheduleAsyncPersistence(ROOT_FILE_URI);

    FileSystemCommand command =
        mFileSystemMaster.workerHeartbeat(mWorkerId1, Lists.newArrayList(fileId));
    Assert.assertEquals(CommandType.Persist, command.getCommandType());
    Assert
        .assertEquals(1, command.getCommandOptions().getPersistOptions().getPersistFiles().size());
    Assert.assertEquals(fileId,
        command.getCommandOptions().getPersistOptions().getPersistFiles().get(0).getFileId());
    Assert.assertEquals(blockId,
        (long) command.getCommandOptions().getPersistOptions().getPersistFiles().get(0)
            .getBlockIds().get(0));
  }

  /**
   * Tests that lost files can successfully be detected.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void lostFilesDetectionTest() throws Exception {
    HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_FILES_DETECTION, 5, TimeUnit.SECONDS);

    createFileWithSingleBlock(NESTED_FILE_URI);
    long fileId = mFileSystemMaster.getFileId(NESTED_FILE_URI);
    mFileSystemMaster.reportLostFile(fileId);

    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(PersistenceState.NOT_PERSISTED.name(), fileInfo.getPersistenceState());
    // Check with getPersistenceState
    Assert.assertEquals(PersistenceState.NOT_PERSISTED,
        mFileSystemMaster.getPersistenceState(fileId));

    // run the detector
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_LOST_FILES_DETECTION);
    Assert.assertTrue(HeartbeatScheduler
        .await(HeartbeatContext.MASTER_LOST_FILES_DETECTION, 5, TimeUnit.SECONDS));

    fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(PersistenceState.LOST.name(), fileInfo.getPersistenceState());
    // Check with getPersistenceState
    Assert.assertEquals(PersistenceState.LOST, mFileSystemMaster.getPersistenceState(fileId));
  }

  /**
   * Tests load metadata logic.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void testLoadMetadataTest() throws Exception {
    FileUtils.createDir(Paths.get(mUnderFS).resolve("a").toString());
    mFileSystemMaster.loadMetadata(new AlluxioURI("alluxio:/a"),
        LoadMetadataOptions.defaults().setCreateAncestors(true));
    mFileSystemMaster.loadMetadata(new AlluxioURI("alluxio:/a"),
        LoadMetadataOptions.defaults().setCreateAncestors(true));

    // TODO(peis): Avoid this hack by adding an option in getFileInfo to skip loading metadata.
    try {
      mFileSystemMaster
          .createDirectory(new AlluxioURI("alluxio:/a"), CreateDirectoryOptions.defaults());
      Assert.fail("createDirectory was expected to fail with FileAlreadyExistsException");
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(
          ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(new AlluxioURI("alluxio:/a")),
          e.getMessage());
    }

    FileUtils.createFile(Paths.get(mUnderFS).resolve("a/f").toString());
    mFileSystemMaster.loadMetadata(new AlluxioURI("alluxio:/a"),
        LoadMetadataOptions.defaults().setCreateAncestors(true).setLoadDirectChildren(true));

    // TODO(peis): Avoid this hack by adding an option in getFileInfo to skip loading metadata.
    try {
      mFileSystemMaster.createFile(new AlluxioURI("alluxio:/a/f"), CreateFileOptions.defaults());
      Assert.fail("createDirectory was expected to fail with FileAlreadyExistsException");
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(
          ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(new AlluxioURI("alluxio:/a/f")),
          e.getMessage());
    }
  }

  /**
   * Tests load root metadata. It should not fail.
   *
    * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void testLoadRoot() throws Exception {
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
}
