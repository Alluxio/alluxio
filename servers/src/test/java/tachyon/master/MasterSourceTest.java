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

package tachyon.master;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.codahale.metrics.Counter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.file.options.SetAttributeOptions;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileAlreadyCompletedException;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.master.block.BlockMaster;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.file.options.CompleteFileOptions;
import tachyon.master.file.options.CreateFileOptions;
import tachyon.master.file.options.CreateDirectoryOptions;
import tachyon.master.journal.Journal;
import tachyon.master.journal.ReadWriteJournal;
import tachyon.thrift.FileInfo;
import tachyon.thrift.NetAddress;
import tachyon.underfs.UnderFileSystem;

/**
 * Unit tests for {@link MasterSource}.
 */
public final class MasterSourceTest {
  private static final long TTLCHECKER_INTERVAL_MS = 0;
  private static final TachyonURI NESTED_FILE_URI = new TachyonURI("/nested/test/file");
  private static final TachyonURI ROOT_URI = new TachyonURI("/");
  private static final TachyonURI ROOT_FILE_URI = new TachyonURI("/file");
  private static final TachyonURI TEST_URI = new TachyonURI("/test");

  private static final TachyonURI DIRECTORY_URI = new TachyonURI("/directory");
  private static final TachyonURI MOUNT_URI =
      new TachyonURI("/tmp/mount-" + System.currentTimeMillis());

  private static CreateFileOptions sNestedFileOptions =
      new CreateFileOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
          .setRecursive(true).build();

  private BlockMaster mBlockMaster;
  private FileSystemMaster mFileSystemMaster;
  private long mWorkerId;

  private UnderFileSystem mUfs = null;

  Map<String, Counter> mCounters;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

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

    // set up worker
    mWorkerId = mBlockMaster.getWorkerId(new NetAddress("localhost", 80, 81, 82));
    mBlockMaster.workerRegister(mWorkerId, Arrays.asList("MEM", "SSD"),
        ImmutableMap.of("MEM", (long) Constants.MB, "SSD", (long) Constants.MB),
        ImmutableMap.of("MEM", (long) Constants.KB, "SSD", (long) Constants.KB),
        Maps.<String, List<Long>>newHashMap());

    MasterContext.reset();
    mCounters = MasterContext.getMasterSource().getMetricRegistry().getCounters();

    mUfs = UnderFileSystem.get(TachyonURI.SEPARATOR, MasterContext.getConf());
  }

  @Test
  public void createFileTest() throws Exception {
    mFileSystemMaster.create(ROOT_FILE_URI, sNestedFileOptions);

    Assert.assertEquals(1, mCounters.get("CreateFileOps").getCount());
    Assert.assertEquals(1, mCounters.get("FilesCreated").getCount());

    // trying to create a file that already exist
    try {
      mFileSystemMaster.create(ROOT_FILE_URI, sNestedFileOptions);
      Assert.fail("create a file that already exist must throw an eception");
    } catch (FileAlreadyExistsException e) {
      // do nothing
    }

    Assert.assertEquals(2, mCounters.get("CreateFileOps").getCount());
    Assert.assertEquals(1, mCounters.get("FilesCreated").getCount());

    // create a nested path (i.e. 2 files and 2 directories will be created)
    mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);

    Assert.assertEquals(3, mCounters.get("CreateFileOps").getCount());
    Assert.assertEquals(2, mCounters.get("FilesCreated").getCount());
    Assert.assertEquals(0, mCounters.get("CreateDirectoryOps").getCount());
    Assert.assertEquals(2, mCounters.get("DirectoriesCreated").getCount());
  }

  @Test
  public void mkdirTest() throws Exception {
    mFileSystemMaster.mkdir(DIRECTORY_URI, CreateDirectoryOptions.defaults());

    Assert.assertEquals(1, mCounters.get("CreateDirectoryOps").getCount());
    Assert.assertEquals(1, mCounters.get("DirectoriesCreated").getCount());

    // trying to create a directory that already exist
    try {
      mFileSystemMaster.mkdir(DIRECTORY_URI, CreateDirectoryOptions.defaults());
      Assert.fail("create a directory that already exist must throw an exception");
    } catch (FileAlreadyExistsException e) {
      // do nothing
    }

    Assert.assertEquals(2, mCounters.get("CreateDirectoryOps").getCount());
    Assert.assertEquals(1, mCounters.get("DirectoriesCreated").getCount());
  }

  @Test
  public void getFileInfoTest() throws Exception {
    long fileId = mFileSystemMaster.create(ROOT_FILE_URI, sNestedFileOptions);

    mFileSystemMaster.getFileInfo(fileId);

    Assert.assertEquals(1, mCounters.get("GetFileInfoOps").getCount());
    Assert.assertEquals(1, mCounters.get("FileInfosGot").getCount());

    // trying to get non-existent file info
    try {
      mFileSystemMaster.getFileInfo(-1);
      Assert.fail("get file info for a non existing file must throw an exception");
    } catch (FileDoesNotExistException e) {
      // do nothing
    }

    Assert.assertEquals(2, mCounters.get("GetFileInfoOps").getCount());
    Assert.assertEquals(1, mCounters.get("FileInfosGot").getCount());
  }

  @Test
  public void getFileBlockInfoTest() throws Exception {
    mFileSystemMaster.create(ROOT_FILE_URI, sNestedFileOptions);
    writeBlockForFile(ROOT_FILE_URI);
    writeBlockForFile(ROOT_FILE_URI);
    completeFile(ROOT_FILE_URI);

    mFileSystemMaster.getFileBlockInfoList(ROOT_FILE_URI);

    Assert.assertEquals(1, mCounters.get("GetFileBlockInfoOps").getCount());
    Assert.assertEquals(2, mCounters.get("FileBlockInfosGot").getCount());

    mFileSystemMaster.create(TEST_URI, sNestedFileOptions);
    writeBlockForFile(TEST_URI);
    completeFile(TEST_URI);

    mFileSystemMaster.getFileBlockInfoList(TEST_URI);

    Assert.assertEquals(2, mCounters.get("GetFileBlockInfoOps").getCount());
    Assert.assertEquals(3, mCounters.get("FileBlockInfosGot").getCount());

    // trying to get block info list for a non-existent file
    try {
      mFileSystemMaster.getFileBlockInfoList(new TachyonURI("/doesNotExist"));
      Assert.fail("get file block info for a non existing file must throw an exception");
    } catch (InvalidPathException e) {
      Assert.assertEquals(ExceptionMessage.PATH_DOES_NOT_EXIST.getMessage("/doesNotExist"),
          e.getMessage());
    }

    Assert.assertEquals(3, mCounters.get("GetFileBlockInfoOps").getCount());
    Assert.assertEquals(3, mCounters.get("FileBlockInfosGot").getCount());
  }

  @Test
  public void completeFileTest() throws Exception {
    mFileSystemMaster.create(ROOT_FILE_URI, sNestedFileOptions);
    writeBlockForFile(ROOT_FILE_URI);
    completeFile(ROOT_FILE_URI);

    // mFileSystemMaster.completeFile(multipleBlocksfileId);

    Assert.assertEquals(1, mCounters.get("CompleteFileOps").getCount());
    Assert.assertEquals(1, mCounters.get("FilesCompleted").getCount());

    // trying to complete a completed file
    try {
      completeFile(ROOT_FILE_URI);
      Assert.fail("complete an already completed file must throw an exception");
    } catch (FileAlreadyCompletedException e) {
      // do nothing
    }

    mFileSystemMaster.getFileBlockInfoList(ROOT_FILE_URI);

    Assert.assertEquals(2, mCounters.get("CompleteFileOps").getCount());
    Assert.assertEquals(1, mCounters.get("FilesCompleted").getCount());
  }

  @Test
  public void deletePathTest() throws Exception {

    // cannot delete root
    Assert.assertFalse(mFileSystemMaster.deleteFile(ROOT_URI, true));

    Assert.assertEquals(1, mCounters.get("DeletePathOps").getCount());
    Assert.assertEquals(0, mCounters.get("PathsDeleted").getCount());

    // delete the file
    createCompleteFileWithSingleBlock(NESTED_FILE_URI);

    mFileSystemMaster.deleteFile(NESTED_FILE_URI, false);

    Assert.assertEquals(2, mCounters.get("DeletePathOps").getCount());
    Assert.assertEquals(1, mCounters.get("PathsDeleted").getCount());
  }

  @Test
  public void getNewBlockIdForFileTest() throws Exception {
    mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(NESTED_FILE_URI);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(NESTED_FILE_URI);
    Assert.assertEquals(Lists.newArrayList(blockId), fileInfo.getBlockIds());

    Assert.assertEquals(1, mCounters.get("GetNewBlockOps").getCount());
  }

  @Test
  public void setStateTest() throws Exception {
    mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);

    mFileSystemMaster.setState(NESTED_FILE_URI, SetAttributeOptions.defaults());

    Assert.assertEquals(1, mCounters.get("SetStateOps").getCount());
  }

  @Test
  public void filePersistedTest() throws Exception {
    createCompleteFileWithSingleBlock(NESTED_FILE_URI);

    mFileSystemMaster.setState(NESTED_FILE_URI, SetAttributeOptions.defaults().setPersisted(true));

    Assert.assertEquals(1, mCounters.get("FilesPersisted").getCount());
  }

  @Test
  public void renameTest() throws Exception {
    mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);

    // try to rename a file to root
    try {
      mFileSystemMaster.rename(NESTED_FILE_URI, ROOT_URI);
      Assert.fail("Renaming to root should fail.");
    } catch (Exception e) {
      // Expected
    }

    Assert.assertEquals(1, mCounters.get("RenamePathOps").getCount());
    Assert.assertEquals(0, mCounters.get("PathsRenamed").getCount());

    // move a nested file to a root file
    mFileSystemMaster.rename(NESTED_FILE_URI, TEST_URI);

    Assert.assertEquals(2, mCounters.get("RenamePathOps").getCount());
    Assert.assertEquals(1, mCounters.get("PathsRenamed").getCount());
  }

  @Test
  public void freeTest() throws Exception {
    mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);
    long blockId = writeBlockForFile(NESTED_FILE_URI);
    Assert.assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // cannot free directory with recursive argument to false
    Assert.assertFalse(mFileSystemMaster.free(NESTED_FILE_URI.getParent(), false));

    Assert.assertEquals(1, mCounters.get("FreeFileOps").getCount());
    Assert.assertEquals(0, mCounters.get("FilesFreed").getCount());

    // free the file
    Assert.assertTrue(mFileSystemMaster.free(NESTED_FILE_URI, false));
    Assert.assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    Assert.assertEquals(2, mCounters.get("FreeFileOps").getCount());
    Assert.assertEquals(1, mCounters.get("FilesFreed").getCount());
  }

  @Test
  public void mountUnmountTest() throws Exception {

    mUfs.mkdirs(MOUNT_URI.getPath(), false);

    mFileSystemMaster.mount(TEST_URI, MOUNT_URI);

    Assert.assertEquals(1, mCounters.get("PathsMounted").getCount());
    Assert.assertEquals(1, mCounters.get("MountOps").getCount());

    // trying to mount an existing file
    try {
      mFileSystemMaster.mount(TEST_URI, MOUNT_URI);
      Assert.fail("Should not be able to mount to an existing file");
    } catch (Exception e) {
      // Expected, continue
    }

    Assert.assertEquals(1, mCounters.get("PathsMounted").getCount());
    Assert.assertEquals(2, mCounters.get("MountOps").getCount());

    mFileSystemMaster.unmount(TEST_URI);

    Assert.assertEquals(1, mCounters.get("PathsUnmounted").getCount());
    Assert.assertEquals(1, mCounters.get("UnmountOps").getCount());
  }

  private void createCompleteFileWithSingleBlock(TachyonURI path) throws Exception {
    mFileSystemMaster.create(path, sNestedFileOptions);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(path);
    mBlockMaster.commitBlock(mWorkerId, Constants.KB, "MEM", blockId, Constants.KB);
    CompleteFileOptions options =
        new CompleteFileOptions.Builder(MasterContext.getConf()).setUfsLength(Constants.KB).build();
    mFileSystemMaster.completeFile(path, options);
  }

  private long writeBlockForFile(TachyonURI path) throws Exception {
    long blockId = mFileSystemMaster.getNewBlockIdForFile(path);
    mBlockMaster.commitBlock(mWorkerId, Constants.KB, "MEM", blockId, Constants.KB);
    return blockId;
  }

  private void completeFile(TachyonURI path) throws Exception {
    CompleteFileOptions options =
        new CompleteFileOptions.Builder(MasterContext.getConf()).setUfsLength(Constants.KB).build();
    mFileSystemMaster.completeFile(path, options);
  }
}
