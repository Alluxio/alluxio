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
import tachyon.client.file.options.SetAttributeOptions;
import tachyon.exception.DirectoryNotEmptyException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.heartbeat.HeartbeatScheduler;
import tachyon.master.MasterContext;
import tachyon.master.block.BlockMaster;
import tachyon.master.file.meta.TTLBucket;
import tachyon.master.file.meta.TTLBucketPrivateAccess;
import tachyon.master.file.options.CompleteFileOptions;
import tachyon.master.file.options.CreateFileOptions;
import tachyon.master.journal.Journal;
import tachyon.master.journal.ReadWriteJournal;
import tachyon.thrift.CommandType;
import tachyon.thrift.FileInfo;
import tachyon.thrift.FileSystemCommand;
import tachyon.thrift.NetAddress;
import tachyon.util.IdUtils;

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

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() {
    sNestedFileOptions =
        new CreateFileOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).build();
    sOldTtlIntervalMs = TTLBucket.getTTLIntervalMs();
    TTLBucketPrivateAccess.setTTLIntervalMs(TTLCHECKER_INTERVAL_MS);
  }

  @AfterClass
  public static void afterClass() {
    TTLBucketPrivateAccess.setTTLIntervalMs(sOldTtlIntervalMs);
  }

  @Before
  public void before() throws Exception {
    MasterContext.getConf().set(Constants.MASTER_TTLCHECKER_INTERVAL_MS,
        String.valueOf(TTLCHECKER_INTERVAL_MS));
    Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    Journal fsJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    HeartbeatContext.setTimerClass(HeartbeatContext.MASTER_TTL_CHECK,
        HeartbeatContext.SCHEDULED_TIMER_CLASS);

    mBlockMaster = new BlockMaster(blockJournal);
    mFileSystemMaster = new FileSystemMaster(mBlockMaster, fsJournal);

    mBlockMaster.start(true);
    mFileSystemMaster.start(true);

    // set up workers
    mWorkerId1 = mBlockMaster.getWorkerId(new NetAddress("localhost", 80, 81, 82));
    mBlockMaster.workerRegister(mWorkerId1, Arrays.asList("MEM", "SSD"),
        ImmutableMap.of("MEM", Constants.MB * 1L, "SSD", Constants.MB * 1L),
        ImmutableMap.of("MEM", Constants.KB * 1L, "SSD", Constants.KB * 1L),
        Maps.<String, List<Long>>newHashMap());
    mWorkerId2 = mBlockMaster.getWorkerId(new NetAddress("remote", 80, 81, 82));
    mBlockMaster.workerRegister(mWorkerId2, Arrays.asList("MEM", "SSD"),
        ImmutableMap.of("MEM", Constants.MB * 1L, "SSD", Constants.MB * 1L),
        ImmutableMap.of("MEM", Constants.KB * 1L, "SSD", Constants.KB * 1L),
        Maps.<String, List<Long>>newHashMap());
  }

  @Test
  public void deleteFileTest() throws Exception {
    // cannot delete root
    long rootId = mFileSystemMaster.getFileId(ROOT_URI);
    Assert.assertFalse(mFileSystemMaster.deleteFile(rootId, true));

    // delete the file
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    Assert.assertTrue(
        mFileSystemMaster.deleteFile(mFileSystemMaster.getFileId(NESTED_FILE_URI), false));
    Assert.assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // verify the file is deleted
    Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
  }

  @Test
  public void deleteNonemptyDirectoryTest() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    long dirId = mFileSystemMaster.getFileId(NESTED_URI);
    String dirName = mFileSystemMaster.getFileInfo(dirId).getName();
    try {
      mFileSystemMaster.deleteFile(dirId, false);
      Assert.fail("Deleting a non-empty directory without setting recursive should fail");
    } catch (DirectoryNotEmptyException e) {
      String expectedMessage =
          ExceptionMessage.DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE.getMessage(dirName);
      Assert.assertEquals(expectedMessage, e.getMessage());
    }

    // Now delete with recursive set to true
    Assert.assertTrue(mFileSystemMaster.deleteFile(dirId, true));
  }

  @Test
  public void deleteDirTest() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    long dirId = mFileSystemMaster.getFileId(NESTED_URI);
    // delete the dir
    Assert.assertTrue(mFileSystemMaster.deleteFile(dirId, true));

    // verify the dir is deleted
    Assert.assertEquals(-1, mFileSystemMaster.getFileId(NESTED_URI));
  }

  @Test
  public void getNewBlockIdForFileTest() throws Exception {
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(fileId);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(Lists.newArrayList(blockId), fileInfo.getBlockIds());
  }

  private void executeTTLCheckOnce() throws Exception {
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

  @Test
  public void createFileWithTTLTest() throws Exception {
    CreateFileOptions options =
        new CreateFileOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).setTTL(1).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(fileInfo.fileId, fileId);

    executeTTLCheckOnce();
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  @Test
  public void setTTLForFileWithNoTTLTest() throws Exception {
    CreateFileOptions options =
        new CreateFileOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    executeTTLCheckOnce();
    // Since no valid TTL is set, the file should not be deleted.
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).fileId);

    mFileSystemMaster.setState(fileId, SetAttributeOptions.defaults().setTTL(0));
    executeTTLCheckOnce();
    // TTL is set to 0, the file should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  @Test
  public void setSmallerTTLForFileWithTTLTest() throws Exception {
    CreateFileOptions options =
        new CreateFileOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).setTTL(Constants.HOUR_MS).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    executeTTLCheckOnce();
    // Since TTL is 1 hour, the file won't be deleted during last TTL check.
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).fileId);

    mFileSystemMaster.setState(fileId, SetAttributeOptions.defaults().setTTL(0));
    executeTTLCheckOnce();
    // TTL is reset to 0, the file should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  @Test
  public void setLargerTTLForFileWithTTLTest() throws Exception {
    CreateFileOptions options =
        new CreateFileOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).setTTL(0).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).fileId);

    mFileSystemMaster.setState(fileId, SetAttributeOptions.defaults().setTTL(Constants.HOUR_MS));
    executeTTLCheckOnce();
    // TTL is reset to 1 hour, the file should not be deleted during last TTL check.
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).fileId);
  }

  @Test
  public void setNoTTLForFileWithTTLTest() throws Exception {
    CreateFileOptions options =
        new CreateFileOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).setTTL(0).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    // After setting TTL to NO_TTL, the original TTL will be removed, and the file will not be
    // deleted during next TTL check.
    mFileSystemMaster.setState(fileId, SetAttributeOptions.defaults().setTTL(Constants.NO_TTL));
    executeTTLCheckOnce();
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).fileId);
  }

  @Test
  public void setStateTest() throws Exception {
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertFalse(fileInfo.isPinned);
    Assert.assertEquals(Constants.NO_TTL, fileInfo.getTtl());

    // No State.
    mFileSystemMaster.setState(fileId, SetAttributeOptions.defaults());
    fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertFalse(fileInfo.isPinned);
    Assert.assertEquals(Constants.NO_TTL, fileInfo.getTtl());

    // Just set pinned flag.
    mFileSystemMaster.setState(fileId, SetAttributeOptions.defaults().setPinned(true));
    fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertTrue(fileInfo.isPinned);
    Assert.assertEquals(Constants.NO_TTL, fileInfo.getTtl());

    // Both pinned flag and ttl value.
    mFileSystemMaster.setState(fileId, SetAttributeOptions.defaults().setPinned(false).setTTL(1));
    fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertFalse(fileInfo.isPinned);
    Assert.assertEquals(1, fileInfo.getTtl());

    // Set ttl for a directory, raise IllegalArgumentException.
    mThrown.expect(IllegalArgumentException.class);
    mFileSystemMaster.setState(mFileSystemMaster.getFileId(NESTED_URI), SetAttributeOptions
        .defaults().setTTL(1));
  }

  @Test
  public void isDirectoryTest() throws Exception {
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);
    Assert.assertFalse(mFileSystemMaster.isDirectory(fileId));
    Assert.assertTrue(mFileSystemMaster.isDirectory(mFileSystemMaster.getFileId(NESTED_URI)));
  }

  @Test
  public void isFullyInMemoryTest() throws Exception {
    // add nested file
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);
    // add in-memory block
    long blockId = mFileSystemMaster.getNewBlockIdForFile(fileId);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB, "MEM", blockId, Constants.KB);
    // add SSD block
    blockId = mFileSystemMaster.getNewBlockIdForFile(fileId);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB, "SSD", blockId, Constants.KB);
    mFileSystemMaster.completeFile(fileId, CompleteFileOptions.defaults());

    createFileWithSingleBlock(ROOT_FILE_URI);
    Assert.assertEquals(Lists.newArrayList(ROOT_FILE_URI), mFileSystemMaster.getInMemoryFiles());
  }

  @Test
  public void renameTest() throws Exception {
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);

    // try to rename a file to root
    try {
      mFileSystemMaster.rename(fileId, ROOT_URI);
      Assert.fail("Renaming to root should fail.");
    } catch (InvalidPathException e) {
      Assert.assertEquals(ExceptionMessage.ROOT_CANNOT_BE_RENAMED.getMessage(), e.getMessage());
    }

    // move root to another path
    long rootId = mFileSystemMaster.getFileId(ROOT_URI);
    try {
      mFileSystemMaster.rename(rootId, TEST_URI);
      Assert.fail("Should not be able to rename root");
    } catch (InvalidPathException e) {
      Assert.assertEquals(ExceptionMessage.ROOT_CANNOT_BE_RENAMED.getMessage(), e.getMessage());
    }

    // move to existing path
    try {
      mFileSystemMaster.rename(fileId, NESTED_URI);
      Assert.fail("Should not be able to overwrite existing file.");
    } catch (FileAlreadyExistsException e) {
      Assert.assertEquals(ExceptionMessage.FILE_ALREADY_EXISTS.getMessage(NESTED_URI.getPath()),
          e.getMessage());
    }

    // move a nested file to a root file
    mFileSystemMaster.rename(fileId, TEST_URI);
    Assert.assertEquals(mFileSystemMaster.getFileInfo(fileId).getPath(), TEST_URI.getPath());
  }

  @Test
  public void renameUnderNonexistingDir() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/nested/test"));

    CreateFileOptions options =
        new CreateFileOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .build();
    long fileId = mFileSystemMaster.create(TEST_URI, options);

    // nested dir
    mFileSystemMaster.rename(fileId, NESTED_FILE_URI);
  }

  @Test
  public void renameToSubpathTest() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Failed to rename: /nested/test is a prefix of /nested/test/file");

    long fileId = mFileSystemMaster.create(NESTED_URI, sNestedFileOptions);
    mFileSystemMaster.rename(fileId, NESTED_FILE_URI);
  }

  @Test
  public void freeTest() throws Exception {
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    Assert.assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // cannot free directory with recursive argument to false
    long dirId = mFileSystemMaster.getFileId(NESTED_FILE_URI.getParent());
    Assert.assertFalse(mFileSystemMaster.free(dirId, false));

    // free the file
    Assert.assertTrue(mFileSystemMaster.free(mFileSystemMaster.getFileId(NESTED_FILE_URI), false));
    Assert.assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  @Test
  public void freeDirTest() throws Exception {
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    Assert.assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // free the dir
    long dirId = mFileSystemMaster.getFileId(NESTED_FILE_URI.getParent());
    Assert.assertTrue(mFileSystemMaster.free(dirId, true));
    Assert.assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());
  }

  @Test
  public void stopTest() throws Exception {
    ExecutorService service =
        (ExecutorService) Whitebox.getInternalState(mFileSystemMaster, "mExecutorService");
    Future<?> ttlThread =
        (Future<?>) Whitebox.getInternalState(mFileSystemMaster, "mTTLCheckerService");
    Assert.assertFalse(ttlThread.isDone());
    Assert.assertFalse(service.isShutdown());
    mFileSystemMaster.stop();
    Assert.assertTrue(ttlThread.isDone());
    Assert.assertTrue(service.isShutdown());
  }

  @Test
  public void workerHeartbeatTest() throws Exception {
    long blockId = createFileWithSingleBlock(ROOT_FILE_URI);

    long fileId = mFileSystemMaster.getFileId(ROOT_FILE_URI);
    mFileSystemMaster.scheduleAsyncPersistence(fileId);

    FileSystemCommand command =
        mFileSystemMaster.workerHeartbeat(mWorkerId1, Lists.newArrayList(fileId));
    Assert.assertEquals(CommandType.Persist, command.commandType);
    Assert.assertEquals(1, command.getCommandOptions().getPersistOptions().persistFiles.size());
    Assert.assertEquals(fileId,
        command.getCommandOptions().getPersistOptions().persistFiles.get(0).fileId);
    Assert.assertEquals(blockId,
        (long) command.getCommandOptions().getPersistOptions().persistFiles.get(0).blockIds.get(0));
  }

  @Test
  public void persistenceFileWithBlocksOnMultipleWorkers() throws Exception {
    long fileId = mFileSystemMaster.create(ROOT_FILE_URI, sNestedFileOptions);
    long blockId1 = mFileSystemMaster.getNewBlockIdForFile(fileId);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB, "MEM", blockId1, Constants.KB);
    long blockId2 = mFileSystemMaster.getNewBlockIdForFile(fileId);
    mBlockMaster.commitBlock(mWorkerId2, Constants.KB, "MEM", blockId2, Constants.KB);
    CompleteFileOptions options =
        new CompleteFileOptions.Builder(MasterContext.getConf()).setUfsLength(Constants.KB).build();
    mFileSystemMaster.completeFile(fileId, options);

    long workerId = mFileSystemMaster.scheduleAsyncPersistence(fileId);
    Assert.assertEquals(IdUtils.INVALID_WORKER_ID, workerId);
  }

  private long createFileWithSingleBlock(TachyonURI uri) throws Exception {
    long fileId = mFileSystemMaster.create(uri, sNestedFileOptions);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(fileId);
    mBlockMaster.commitBlock(mWorkerId1, Constants.KB, "MEM", blockId, Constants.KB);
    CompleteFileOptions options =
        new CompleteFileOptions.Builder(MasterContext.getConf()).setUfsLength(Constants.KB).build();
    mFileSystemMaster.completeFile(fileId, options);
    return blockId;
  }
}
