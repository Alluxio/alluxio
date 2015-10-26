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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.file.options.SetStateOptions;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.heartbeat.HeartbeatScheduler;
import tachyon.master.MasterContext;
import tachyon.master.block.BlockMaster;
import tachyon.master.file.options.CreateOptions;
import tachyon.master.journal.Journal;
import tachyon.master.journal.ReadWriteJournal;
import tachyon.thrift.FileInfo;
import tachyon.thrift.NetAddress;
import tachyon.util.IdUtils;

/**
 * Unit tests for tachyon.master.filesystem.FileSystemMaster.
 */
public final class FileSystemMasterTest {
  private static final TachyonURI NESTED_URI = new TachyonURI("/nested/test");
  private static final TachyonURI NESTED_FILE_URI = new TachyonURI("/nested/test/file");
  private static final TachyonURI ROOT_URI = new TachyonURI("/");
  private static final TachyonURI ROOT_FILE_URI = new TachyonURI("/file");
  private static final TachyonURI TEST_URI = new TachyonURI("/test");
  private static CreateOptions sNestedFileOptions;

  private BlockMaster mBlockMaster;
  private FileSystemMaster mFileSystemMaster;
  private long mWorkerId;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    MasterContext.getConf().set(Constants.MASTER_TTLCHECKER_INTERVAL_MS, "0");
    Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    Journal fsJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    HeartbeatContext.setTimerClass(HeartbeatContext.MASTER_TTL_CHECK,
        HeartbeatContext.SCHEDULED_TIMER_CLASS);

    mBlockMaster = new BlockMaster(blockJournal);
    mFileSystemMaster = new FileSystemMaster(mBlockMaster, fsJournal);

    mBlockMaster.start(true);
    mFileSystemMaster.start(true);

    // set up worker
    mWorkerId = mBlockMaster.getWorkerId(new NetAddress("localhost", 80, 81));
    mBlockMaster.workerRegister(mWorkerId, Lists.newArrayList(Constants.MB * 1L, Constants.MB * 1L),
        Lists.<Long>newArrayList(Constants.KB * 1L, Constants.KB * 1L),
        Maps.<Long, List<Long>>newHashMap());
  }

  @BeforeClass
  public static void beforeClass() {
    sNestedFileOptions =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).build();
  }

  @Test
  public void deleteFileTest() throws Exception {
    // cannot delete root
    long rootId = mFileSystemMaster.getFileId(ROOT_URI);
    Assert.assertFalse(mFileSystemMaster.deleteFile(rootId, true));

    // cannot delete directory with recursive argument to false
    long blockId = createFileWithSingleBlock(NESTED_FILE_URI);
    long dirId = mFileSystemMaster.getFileId(NESTED_URI);
    Assert.assertFalse(mFileSystemMaster.deleteFile(dirId, false));

    // delete the file
    Assert.assertTrue(
        mFileSystemMaster.deleteFile(mFileSystemMaster.getFileId(NESTED_FILE_URI), false));
    Assert.assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // verify the file is deleted
    Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFileSystemMaster.getFileId(NESTED_FILE_URI));
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
    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
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
    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    executeTTLCheckOnce();
    // Since no valid TTL is set, the file should not be deleted.
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).fileId);

    mFileSystemMaster.setState(fileId, new SetStateOptions.Builder(MasterContext.getConf())
        .setTTL(0).build());
    executeTTLCheckOnce();
    // TTL is set to 0, the file should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  @Test
  public void setSmallerTTLForFileWithTTLTest() throws Exception {
    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).setTTL(Constants.HOUR_MS).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    executeTTLCheckOnce();
    // Since TTL is 1 hour, the file won't be deleted during last TTL check.
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).fileId);

    mFileSystemMaster.setState(fileId, new SetStateOptions.Builder(MasterContext.getConf())
        .setTTL(0).build());
    executeTTLCheckOnce();
    // TTL is reset to 0, the file should have been deleted during last TTL check.
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  @Test
  public void setLargerTTLForFileWithTTLTest() throws Exception {
    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).setTTL(0).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).fileId);

    mFileSystemMaster.setState(fileId, new SetStateOptions.Builder(MasterContext.getConf())
        .setTTL(Constants.HOUR_MS).build());
    executeTTLCheckOnce();
    // TTL is reset to 1 hour, the file should not be deleted during last TTL check.
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).fileId);
  }

  @Test
  public void setNoTTLForFileWithTTLTest() throws Exception {
    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
            .setRecursive(true).setTTL(0).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    // After setting TTL to NO_TTL, the original TTL will be removed, and the file will not be
    // deleted during next TTL check.
    mFileSystemMaster.setState(fileId, new SetStateOptions.Builder(MasterContext.getConf())
        .setTTL(Constants.NO_TTL).build());
    executeTTLCheckOnce();
    Assert.assertEquals(fileId, mFileSystemMaster.getFileInfo(fileId).fileId);
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
    mBlockMaster.commitBlock(mWorkerId, Constants.KB, 1, blockId, Constants.KB);
    // add SSD block
    blockId = mFileSystemMaster.getNewBlockIdForFile(fileId);
    mBlockMaster.commitBlock(mWorkerId, Constants.KB, 2, blockId, Constants.KB);
    mFileSystemMaster.completeFile(fileId);

    createFileWithSingleBlock(ROOT_FILE_URI);
    Assert.assertEquals(Lists.newArrayList(ROOT_FILE_URI), mFileSystemMaster.getInMemoryFiles());
  }

  @Test
  public void renameTest() throws Exception {
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);

    // move a nested file to root
    Assert.assertFalse(mFileSystemMaster.rename(fileId, ROOT_URI));

    // move root
    long rootId = mFileSystemMaster.getFileId(ROOT_URI);
    Assert.assertFalse(mFileSystemMaster.rename(rootId, TEST_URI));

    // move to existing path
    Assert.assertFalse(mFileSystemMaster.rename(fileId, NESTED_URI));

    // move a nested file to a root file
    Assert.assertTrue(mFileSystemMaster.rename(fileId, TEST_URI));
  }

  @Test
  public void renameUnderNonexistingDir() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/nested/test"));

    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB).build();
    long fileId = mFileSystemMaster.create(TEST_URI, options);

    // nested dir
    Assert.assertFalse(mFileSystemMaster.rename(fileId, NESTED_FILE_URI));
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

  private long createFileWithSingleBlock(TachyonURI uri) throws Exception {
    long fileId = mFileSystemMaster.create(uri, sNestedFileOptions);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(fileId);
    mBlockMaster.commitBlock(mWorkerId, Constants.KB, 1, blockId, Constants.KB);
    mFileSystemMaster.completeFile(fileId);
    return blockId;
  }
}
