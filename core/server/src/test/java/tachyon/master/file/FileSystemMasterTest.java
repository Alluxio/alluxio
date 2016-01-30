/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.file;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.internal.util.reflection.Whitebox;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.exception.DirectoryNotEmptyException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.heartbeat.HeartbeatScheduler;
import tachyon.master.MasterContext;
import tachyon.master.block.BlockMaster;
import tachyon.master.file.meta.PersistenceState;
import tachyon.master.file.meta.TtlBucket;
import tachyon.master.file.meta.TtlBucketPrivateAccess;
import tachyon.master.file.options.CompleteFileOptions;
import tachyon.master.file.options.CreateFileOptions;
import tachyon.master.file.options.SetAttributeOptions;
import tachyon.master.journal.Journal;
import tachyon.master.journal.ReadWriteJournal;
import tachyon.thrift.CommandType;
import tachyon.thrift.FileSystemCommand;
import tachyon.util.IdUtils;
import tachyon.wire.FileInfo;
import tachyon.wire.WorkerNetAddress;

/**
 * Unit tests for {@link FileSystemMaster}.
 */
public final class FileSystemMasterTest {
  private static final long TTLCHECKER_INTERVAL_MS = 0;
  private static final TachyonURI NESTED_URI = new TachyonURI("/nested/test");
  private static final TachyonURI NESTED_FILE_URI = new TachyonURI("/nested/test/file");
  private static final TachyonURI ROOT_URI = new TachyonURI("/");
  private static final TachyonURI ROOT_FILE_URI = new TachyonURI("/file");
  private static final TachyonURI TEST_URI = new TachyonURI("/test");
  private static CreateFileOptions sNestedFileOptions;
  private static long sOldTtlIntervalMs;

  private BlockMaster mBlockMaster;
  private FileSystemMaster mFileSystemMaster;
  private long mWorkerId1;
  private long mWorkerId2;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /** The exception expected to be thrown. */
  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  /**
   * Sets up the dependencies before a single test runs.
   */
  @BeforeClass
  public static void beforeClass() {
    MasterContext.reset(new TachyonConf());
    sNestedFileOptions =
        new CreateFileOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).build();
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
    MasterContext.getConf().set(Constants.MASTER_TTLCHECKER_INTERVAL_MS,
        String.valueOf(TTLCHECKER_INTERVAL_MS));
    Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    Journal fsJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    HeartbeatContext.setTimerClass(HeartbeatContext.MASTER_TTL_CHECK,
        HeartbeatContext.SCHEDULED_TIMER_CLASS);
    HeartbeatContext.setTimerClass(HeartbeatContext.MASTER_LOST_FILES_DETECTION,
        HeartbeatContext.SCHEDULED_TIMER_CLASS);

    mBlockMaster = new BlockMaster(blockJournal);
    mFileSystemMaster = new FileSystemMaster(mBlockMaster, fsJournal);

    mBlockMaster.start(true);
    mFileSystemMaster.start(true);

    // set up workers
    mWorkerId1 = mBlockMaster.getWorkerId(new WorkerNetAddress().setHost("localhost")
        .setRpcPort(80).setDataPort(81).setWebPort(82));
    mBlockMaster.workerRegister(mWorkerId1, Arrays.asList("MEM", "SSD"),
        ImmutableMap.of("MEM", Constants.MB * 1L, "SSD", Constants.MB * 1L),
        ImmutableMap.of("MEM", Constants.KB * 1L, "SSD", Constants.KB * 1L),
        Maps.<String, List<Long>>newHashMap());
    mWorkerId2 = mBlockMaster.getWorkerId(new WorkerNetAddress().setHost("localhost")
        .setRpcPort(80).setDataPort(81).setWebPort(82));
    mBlockMaster.workerRegister(mWorkerId2, Arrays.asList("MEM", "SSD"),
        ImmutableMap.of("MEM", Constants.MB * 1L, "SSD", Constants.MB * 1L),
        ImmutableMap.of("MEM", Constants.KB * 1L, "SSD", Constants.KB * 1L),
        Maps.<String, List<Long>>newHashMap());
  }

  /**
   * Tests the {@link FileSystemMaster#deleteFile(TachyonURI, boolean)} method.
   *
   * @throws Exception if deleting a file fails
   */
  @Test
  public void deleteFileTest() throws Exception {
    // cannot delete root
    Assert.assertFalse(mFileSystemMaster.deleteFile(ROOT_URI, true));

    // delete the file
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    Assert.assertTrue(
        mFileSystemMaster.deleteFile(NESTED_FILE_URI, false));
    Assert.assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // verify the file is deleted
    Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
  }

  /**
   * Tests the {@link FileSystemMaster#deleteFile(TachyonURI, boolean)} method with a non-empty
   * directory.
   *
   * @throws Exception if deleting a directory fails
   */
  @Test
  public void deleteNonemptyDirectoryTest() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    String dirName = mFileSystemMaster.getFileInfo(NESTED_URI).getName();
    try {
      mFileSystemMaster.deleteFile(NESTED_URI, false);
      Assert.fail("Deleting a non-empty directory without setting recursive should fail");
    } catch (DirectoryNotEmptyException e) {
      String expectedMessage =
          ExceptionMessage.DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE.getMessage(dirName);
      Assert.assertEquals(expectedMessage, e.getMessage());
    }

    // Now delete with recursive set to true
    Assert.assertTrue(mFileSystemMaster.deleteFile(NESTED_URI, true));
  }

  /**
   * Tests the {@link FileSystemMaster#deleteFile(TachyonURI, boolean)} method for a directory.
   *
   * @throws Exception if deleting the directory fails
   */
  @Test
  public void deleteDirTest() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    // delete the dir
    Assert.assertTrue(mFileSystemMaster.deleteFile(NESTED_URI, true));

    // verify the dir is deleted
    Assert.assertEquals(-1, mFileSystemMaster.getFileId(NESTED_URI));
  }

  /**
   * Tests the {@link FileSystemMaster#getNewBlockIdForFile(TachyonURI)} method.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void getNewBlockIdForFileTest() throws Exception {
    mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(NESTED_FILE_URI);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI);
    Assert.assertEquals(Lists.newArrayList(blockId), fileInfo.getBlockIds());
  }

  private void executeTtlCheckOnce() throws Exception {
    // Wait for the TTL check executor to be ready to execute its heartbeat.
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_TTL_CHECK, 1,
        TimeUnit.SECONDS));
    // Execute the TTL check executor heartbeat.
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_TTL_CHECK);
    // Wait for the TLL check executor to be ready to execute its heartbeat again. This is needed to
    // avoid a race between the subsequent test logic and the heartbeat thread.
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_TTL_CHECK, 1,
        TimeUnit.SECONDS));
  }

  /**
   * Tests that an exception is in the
   * {@link FileSystemMaster#create(TachyonURI, CreateFileOptions)} with a TTL set in the
   * {@link CreateFileOptions} after the TTL check was done once.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void createFileWithTtlTest() throws Exception {
    CreateFileOptions options =
        new CreateFileOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).setTtl(1).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
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
  public void setTtlForFileWithNoTtlTest() throws Exception {
    CreateFileOptions options =
        new CreateFileOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    executeTtlCheckOnce();
    // Since no valid TTL is set, the file should not be deleted.
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(NESTED_FILE_URI).getFileId());

    mFileSystemMaster.setState(NESTED_FILE_URI,
        new SetAttributeOptions.Builder().setTtl(0).build());
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
        new CreateFileOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).setTtl(Constants.HOUR_MS).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    executeTtlCheckOnce();
    // Since TTL is 1 hour, the file won't be deleted during last TTL check.
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(NESTED_FILE_URI).getFileId());

    mFileSystemMaster.setState(NESTED_FILE_URI,
        new SetAttributeOptions.Builder().setTtl(0).build());
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
        new CreateFileOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).setTtl(0).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(NESTED_FILE_URI).getFileId());

    mFileSystemMaster.setState(NESTED_FILE_URI,
        new SetAttributeOptions.Builder().setTtl(Constants.HOUR_MS).build());
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
        new CreateFileOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).setTtl(0).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    // After setting TTL to NO_TTL, the original TTL will be removed, and the file will not be
    // deleted during next TTL check.
    mFileSystemMaster.setState(NESTED_FILE_URI,
        new SetAttributeOptions.Builder().setTtl(Constants.NO_TTL).build());
    executeTtlCheckOnce();
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).getFileId());
  }

  /**
   * Tests the {@link FileSystemMaster#setState(TachyonURI, SetAttributeOptions)} method and that an
   * exception is thrown when trying to set a TTL for a directory.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void setStateTest() throws Exception {
    mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI);
    Assert.assertFalse(fileInfo.isPinned());
    Assert.assertEquals(Constants.NO_TTL, fileInfo.getTtl());

    // No State.
    mFileSystemMaster.setState(NESTED_FILE_URI, new SetAttributeOptions.Builder().build());
    fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI);
    Assert.assertFalse(fileInfo.isPinned());
    Assert.assertEquals(Constants.NO_TTL, fileInfo.getTtl());

    // Just set pinned flag.
    mFileSystemMaster.setState(NESTED_FILE_URI,
        new SetAttributeOptions.Builder().setPinned(true).build());
    fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI);
    Assert.assertTrue(fileInfo.isPinned());
    Assert.assertEquals(Constants.NO_TTL, fileInfo.getTtl());

    // Both pinned flag and ttl value.
    mFileSystemMaster.setState(NESTED_FILE_URI,
        new SetAttributeOptions.Builder().setPinned(false).setTtl(1).build());
    fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI);
    Assert.assertFalse(fileInfo.isPinned());
    Assert.assertEquals(1, fileInfo.getTtl());

    // Set ttl for a directory, raise IllegalArgumentException.
    mThrown.expect(IllegalArgumentException.class);
    mFileSystemMaster.setState(NESTED_URI, new SetAttributeOptions.Builder().setTtl(1).build());
  }

  /**
   * Tests the {@link FileSystemMaster#isDirectory(long)} method.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void isDirectoryTest() throws Exception {
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);
    Assert.assertFalse(mFileSystemMaster.isDirectory(fileId));
    Assert.assertTrue(mFileSystemMaster.isDirectory(mFileSystemMaster.getFileId(NESTED_URI)));
  }

  /**
   * Tests that a file is fully written to memory.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void isFullyInMemoryTest() throws Exception {
    // add nested file
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);
    // add in-memory block
    long blockId = mFileSystemMaster.getNewBlockIdForFile(NESTED_FILE_URI);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB, "MEM", blockId, Constants.KB);
    // add SSD block
    blockId = mFileSystemMaster.getNewBlockIdForFile(NESTED_FILE_URI);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB, "SSD", blockId, Constants.KB);
    mFileSystemMaster.completeFile(NESTED_FILE_URI, CompleteFileOptions.defaults());

    createFileWithSingleBlock(ROOT_FILE_URI);
    Assert.assertEquals(Lists.newArrayList(ROOT_FILE_URI), mFileSystemMaster.getInMemoryFiles());
  }

  /**
   * Tests the {@link FileSystemMaster#rename(TachyonURI, TachyonURI)} method.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void renameTest() throws Exception {
    mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);

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
  }

  /**
   * Tests that an exception is thrown when trying to create a file in a non-existing directory
   * without setting the {@code recursive} flag.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void renameUnderNonexistingDir() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/nested/test"));

    CreateFileOptions options =
        new CreateFileOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .build();
    mFileSystemMaster.create(TEST_URI, options);

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
    mThrown.expectMessage("/nested/test is a prefix of /nested/test/file");

    mFileSystemMaster.create(NESTED_URI, sNestedFileOptions);
    mFileSystemMaster.rename(NESTED_URI, NESTED_FILE_URI);
  }

  /**
   * Tests the {@link FileSystemMaster#free(TachyonURI, boolean)} method.
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
    Assert.assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  /**
   * Tests the {@link FileSystemMaster#free(TachyonURI, boolean)} method with a directory.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void freeDirTest() throws Exception {
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    Assert.assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // free the dir
    Assert.assertTrue(mFileSystemMaster.free(NESTED_FILE_URI.getParent(), true));
    Assert.assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
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
    Assert.assertEquals(1, command.getCommandOptions().getPersistOptions().getPersistFiles()
            .size());
    Assert.assertEquals(fileId,
        command.getCommandOptions().getPersistOptions().getPersistFiles().get(0).getFileId());
    Assert.assertEquals(blockId,
        (long) command.getCommandOptions().getPersistOptions().getPersistFiles().get(0)
                .getBlockIds().get(0));
  }

  /**
   * Tests the persistence of file with block on multiple workers.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void persistenceFileWithBlocksOnMultipleWorkers() throws Exception {
    long fileId = mFileSystemMaster.create(ROOT_FILE_URI, sNestedFileOptions);
    long blockId1 = mFileSystemMaster.getNewBlockIdForFile(ROOT_FILE_URI);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB, "MEM", blockId1, Constants.KB);
    long blockId2 = mFileSystemMaster.getNewBlockIdForFile(ROOT_FILE_URI);
    mBlockMaster.commitBlock(mWorkerId2, Constants.KB, "MEM", blockId2, Constants.KB);
    CompleteFileOptions options =
        new CompleteFileOptions.Builder(MasterContext.getConf()).setUfsLength(Constants.KB).build();
    mFileSystemMaster.completeFile(ROOT_FILE_URI, options);

    long workerId = mFileSystemMaster.scheduleAsyncPersistence(ROOT_FILE_URI);
    Assert.assertEquals(IdUtils.INVALID_WORKER_ID, workerId);
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

    // run the detector
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_LOST_FILES_DETECTION);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_LOST_FILES_DETECTION, 5,
        TimeUnit.SECONDS));

    fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(PersistenceState.LOST.name(), fileInfo.getPersistenceState());
  }

  private long createFileWithSingleBlock(TachyonURI uri) throws Exception {
    mFileSystemMaster.create(uri, sNestedFileOptions);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(uri);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB, "MEM", blockId, Constants.KB);
    CompleteFileOptions options =
        new CompleteFileOptions.Builder(MasterContext.getConf()).setUfsLength(Constants.KB).build();
    mFileSystemMaster.completeFile(uri, options);
    return blockId;
  }
}
