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
import tachyon.client.file.options.SetStateOptions;
import tachyon.exception.FileAlreadyCompletedException;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.master.block.BlockMaster;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.file.options.CompleteFileOptions;
import tachyon.master.file.options.CreateOptions;
import tachyon.master.file.options.MkdirOptions;
import tachyon.master.journal.Journal;
import tachyon.master.journal.ReadWriteJournal;
import tachyon.thrift.FileInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.worker.NetAddress;

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

  private static CreateOptions sNestedFileOptions =
      new CreateOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(Constants.KB)
          .setRecursive(true).build();

  private BlockMaster mBlockMaster;
  private FileSystemMaster mFileSystemMaster;
  private long mWorkerId;

  private UnderFileSystem mUfs = null;

  Map<String, Counter> mCounters;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  /**
   * Sets up the dependencies before a test runs.
   *
   * @throws Exception if setting up the dependencies fails
   */
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

  /**
   * Tests the {@code CreateFileOps}, {@code FilesCreated}, {@code CreateDirectoryOps} and the
   * {@code DirectoriesCreated} counters when creating a file.
   *
   * @throws Exception if creating a file fails
   */
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

  /**
   * Tests the {@code CreateDirectoryOps} and the {@code DirectoryCreated} counters when creating a
   * directory.
   *
   * @throws Exception if creating a directory fails
   */
  @Test
  public void mkdirTest() throws Exception {
    mFileSystemMaster.mkdir(DIRECTORY_URI, MkdirOptions.defaults());

    Assert.assertEquals(1, mCounters.get("CreateDirectoryOps").getCount());
    Assert.assertEquals(1, mCounters.get("DirectoriesCreated").getCount());

    // trying to create a directory that already exist
    try {
      mFileSystemMaster.mkdir(DIRECTORY_URI, MkdirOptions.defaults());
      Assert.fail("create a directory that already exist must throw an exception");
    } catch (FileAlreadyExistsException e) {
      // do nothing
    }

    Assert.assertEquals(2, mCounters.get("CreateDirectoryOps").getCount());
    Assert.assertEquals(1, mCounters.get("DirectoriesCreated").getCount());
  }

  /**
   * Tests the {@code GetFileInfoOps} and {@code FileInfosGot} counters when retrieving information
   * about a file.
   *
   * @throws Exception if creating a file or retrieving its information fails
   */
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

  /**
   * Tests the {@code GetFileBlockInfoOps} and {@code FileBlockInfosGot} counters when retrieving
   * information about a block of a file.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void getFileBlockInfoTest() throws Exception {
    long multipleBlocksfileId = mFileSystemMaster.create(ROOT_FILE_URI, sNestedFileOptions);
    writeBlockForFile(multipleBlocksfileId);
    writeBlockForFile(multipleBlocksfileId);
    completeFile(multipleBlocksfileId);

    mFileSystemMaster.getFileBlockInfoList(multipleBlocksfileId);

    Assert.assertEquals(1, mCounters.get("GetFileBlockInfoOps").getCount());
    Assert.assertEquals(2, mCounters.get("FileBlockInfosGot").getCount());

    long singleBlocksfileId = mFileSystemMaster.create(TEST_URI, sNestedFileOptions);
    writeBlockForFile(singleBlocksfileId);
    completeFile(singleBlocksfileId);

    mFileSystemMaster.getFileBlockInfoList(singleBlocksfileId);

    Assert.assertEquals(2, mCounters.get("GetFileBlockInfoOps").getCount());
    Assert.assertEquals(3, mCounters.get("FileBlockInfosGot").getCount());

    // trying to get block info list for a non-existent file
    try {
      mFileSystemMaster.getFileBlockInfoList(-1);
      Assert.fail("get file block info for a non existing file must throw an exception");
    } catch (FileDoesNotExistException e) {
      // do nothing
    }

    Assert.assertEquals(3, mCounters.get("GetFileBlockInfoOps").getCount());
    Assert.assertEquals(3, mCounters.get("FileBlockInfosGot").getCount());
  }

  /**
   * Tests the {@code CompleteFileOps} and {@code FilesCompleted} counters when completing a file.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void completeFileTest() throws Exception {
    long singleBlocksfileId = mFileSystemMaster.create(ROOT_FILE_URI, sNestedFileOptions);
    writeBlockForFile(singleBlocksfileId);
    completeFile(singleBlocksfileId);

    // mFileSystemMaster.completeFile(multipleBlocksfileId);

    Assert.assertEquals(1, mCounters.get("CompleteFileOps").getCount());
    Assert.assertEquals(1, mCounters.get("FilesCompleted").getCount());

    // trying to complete a completed file
    try {
      completeFile(singleBlocksfileId);
      Assert.fail("complete an already completed file must throw an exception");
    } catch (FileAlreadyCompletedException e) {
      // do nothing
    }

    mFileSystemMaster.getFileBlockInfoList(singleBlocksfileId);

    Assert.assertEquals(2, mCounters.get("CompleteFileOps").getCount());
    Assert.assertEquals(1, mCounters.get("FilesCompleted").getCount());
  }

  /**
   * Tests the {@code DeletePathOps} and {@code PathsDeleted} counters when deleting a path.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void deletePathTest() throws Exception {

    // cannot delete root
    long rootId = mFileSystemMaster.getFileId(ROOT_URI);
    Assert.assertFalse(mFileSystemMaster.deleteFile(rootId, true));

    Assert.assertEquals(1, mCounters.get("DeletePathOps").getCount());
    Assert.assertEquals(0, mCounters.get("PathsDeleted").getCount());

    // delete the file
    long nestedId = createCompleteFileWithSingleBlock(NESTED_FILE_URI);

    mFileSystemMaster.deleteFile(nestedId, false);

    Assert.assertEquals(2, mCounters.get("DeletePathOps").getCount());
    Assert.assertEquals(1, mCounters.get("PathsDeleted").getCount());
  }

  /**
   * Tests the {@code GetNewBlockOps} counter when retrieving a new block id for a file.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void getNewBlockIdForFileTest() throws Exception {
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(fileId);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(Lists.newArrayList(blockId), fileInfo.getBlockIds());

    Assert.assertEquals(1, mCounters.get("GetNewBlockOps").getCount());
  }

  /**
   * Tests the {@code SetStateOps} counter when setting the state of a file.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void setStateTest() throws Exception {
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);

    mFileSystemMaster.setState(fileId, new SetStateOptions.Builder().build());

    Assert.assertEquals(1, mCounters.get("SetStateOps").getCount());
  }

  /**
   * Tests the {@code FilesPersisted} counter when setting a file to persisted.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void filePersistedTest() throws Exception {
    long fileId = createCompleteFileWithSingleBlock(NESTED_FILE_URI);

    mFileSystemMaster.setState(fileId, new SetStateOptions.Builder().setPersisted(true).build());

    Assert.assertEquals(1, mCounters.get("FilesPersisted").getCount());
  }

  /**
   * Tests the {@code RenamePathOps} and {@code PathsRenamed} counters when renaming a file.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void renameTest() throws Exception {
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);

    // move a nested file to root
    Assert.assertFalse(mFileSystemMaster.rename(fileId, ROOT_URI));

    Assert.assertEquals(1, mCounters.get("RenamePathOps").getCount());
    Assert.assertEquals(0, mCounters.get("PathsRenamed").getCount());

    // move a nested file to a root file
    Assert.assertTrue(mFileSystemMaster.rename(fileId, TEST_URI));

    Assert.assertEquals(2, mCounters.get("RenamePathOps").getCount());
    Assert.assertEquals(1, mCounters.get("PathsRenamed").getCount());
  }

  /**
   * Tests the {@code FreeFileOps} and {@code FielsFreed} counters when freeing a file.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void freeTest() throws Exception {
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, sNestedFileOptions);
    long blockId = writeBlockForFile(fileId);
    Assert.assertEquals(1, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    // cannot free directory with recursive argument to false
    long dirId = mFileSystemMaster.getFileId(NESTED_FILE_URI.getParent());
    Assert.assertFalse(mFileSystemMaster.free(dirId, false));

    Assert.assertEquals(1, mCounters.get("FreeFileOps").getCount());
    Assert.assertEquals(0, mCounters.get("FilesFreed").getCount());

    // free the file
    Assert.assertTrue(mFileSystemMaster.free(mFileSystemMaster.getFileId(NESTED_FILE_URI), false));
    Assert.assertEquals(0, mBlockMaster.getBlockInfo(blockId).getLocations().size());

    Assert.assertEquals(2, mCounters.get("FreeFileOps").getCount());
    Assert.assertEquals(1, mCounters.get("FilesFreed").getCount());
  }

  /**
   * Tests the {@code PathsMounted} and the {@code MountOps} counters when mounting or unmounting a
   * path.
   *
   * @throws Exception if a {@link FileSystemMaster} operation fails
   */
  @Test
  public void mountUnmountTest() throws Exception {

    mUfs.mkdirs(MOUNT_URI.getPath(), false);

    mFileSystemMaster.mount(TEST_URI, MOUNT_URI);

    Assert.assertEquals(1, mCounters.get("PathsMounted").getCount());
    Assert.assertEquals(1, mCounters.get("MountOps").getCount());

    // trying to mount an existing file
    Assert.assertFalse(mFileSystemMaster.mount(TEST_URI, MOUNT_URI));

    Assert.assertEquals(1, mCounters.get("PathsMounted").getCount());
    Assert.assertEquals(2, mCounters.get("MountOps").getCount());

    mFileSystemMaster.unmount(TEST_URI);

    Assert.assertEquals(1, mCounters.get("PathsUnmounted").getCount());
    Assert.assertEquals(1, mCounters.get("UnmountOps").getCount());
  }

  private long createCompleteFileWithSingleBlock(TachyonURI uri) throws Exception {
    long fileId = mFileSystemMaster.create(uri, sNestedFileOptions);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(fileId);
    mBlockMaster.commitBlock(mWorkerId, Constants.KB, "MEM", blockId, Constants.KB);
    CompleteFileOptions options =
        new CompleteFileOptions.Builder(MasterContext.getConf()).setUfsLength(Constants.KB).build();
    mFileSystemMaster.completeFile(fileId, options);
    return fileId;
  }

  private long writeBlockForFile(long fileId) throws Exception {
    long blockId = mFileSystemMaster.getNewBlockIdForFile(fileId);
    mBlockMaster.commitBlock(mWorkerId, Constants.KB, "MEM", blockId, Constants.KB);
    return blockId;
  }

  private long completeFile(long fileId) throws Exception {
    CompleteFileOptions options =
        new CompleteFileOptions.Builder(MasterContext.getConf()).setUfsLength(Constants.KB).build();
    mFileSystemMaster.completeFile(fileId, options);
    return fileId;
  }
}
