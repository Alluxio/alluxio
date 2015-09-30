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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.master.MasterContext;
import tachyon.master.block.BlockMaster;
import tachyon.master.file.options.CreateOptions;
import tachyon.master.journal.Journal;
import tachyon.master.journal.ReadWriteJournal;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;

/**
 * Unit tests for tachyon.master.filesystem.FileSystemMaster.
 */
public final class FileSystemMasterTest {
  private static final TachyonURI NESTED_URI = new TachyonURI("/nested/test");
  private static final TachyonURI NESTED_FILE_URI = new TachyonURI("/nested/test/file");
  private static final TachyonURI ROOT_URI = new TachyonURI("/");
  private static final TachyonURI ROOT_FILE_URI = new TachyonURI("/file");
  private static final TachyonURI TEST_URI = new TachyonURI("/test");

  private BlockMaster mBlockMaster;
  private FileSystemMaster mFileSystemMaster;
  private long mWorkerId;

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Before
  public void before() throws Exception {
    MasterContext.getConf().set(Constants.MASTER_TTLCHECKER_INTERVAL_MS, "1000");
    Journal blockJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());
    Journal fsJournal = new ReadWriteJournal(mTestFolder.newFolder().getAbsolutePath());

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
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Could not find path: /nested/test/file");

    mFileSystemMaster.getFileId(NESTED_FILE_URI);
  }

  @Test
  public void deleteDirTest() throws Exception {
    createFileWithSingleBlock(NESTED_FILE_URI);
    long dirId = mFileSystemMaster.getFileId(NESTED_URI);
    // delete the dir
    Assert.assertTrue(mFileSystemMaster.deleteFile(dirId, true));

    // verify the dir is deleted
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Could not find path: /nested/test");

    mFileSystemMaster.getFileId(NESTED_URI);
  }

  @Test
  public void getNewBlockIdForFileTest() throws Exception {
    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSize(Constants.KB)
            .setRecursive(true).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(fileId);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(Lists.newArrayList(blockId), fileInfo.getBlockIds());
  }

  @Test
  public void createFileWithTTLTest() throws Exception {
    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSize(Constants.KB)
            .setRecursive(true).setTTL(1).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(fileInfo.fileId, fileId);
    CommonUtils.sleepMs(5000);
    mThrown.expect(FileDoesNotExistException.class);
    mFileSystemMaster.getFileInfo(fileId);
  }

  @Test
  public void isDirectoryTest() throws Exception {
    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSize(Constants.KB)
            .setRecursive(true).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
    Assert.assertFalse(mFileSystemMaster.isDirectory(fileId));
    Assert.assertTrue(mFileSystemMaster.isDirectory(mFileSystemMaster.getFileId(NESTED_URI)));
  }

  @Test
  public void isFullyInMemoryTest() throws Exception {
    // add nested file
    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSize(Constants.KB)
            .setRecursive(true).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);
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
    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSize(Constants.KB)
            .setRecursive(true).build();
    long fileId = mFileSystemMaster.create(NESTED_FILE_URI, options);

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
    mThrown.expectMessage("Could not find path: /nested/test");

    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSize(Constants.KB).build();
    long fileId = mFileSystemMaster.create(TEST_URI, options);

    // nested dir
    Assert.assertFalse(mFileSystemMaster.rename(fileId, NESTED_FILE_URI));
  }

  @Test
  public void renameToSubpathTest() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Failed to rename: /nested/test is a prefix of /nested/test/file");

    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSize(Constants.KB)
            .setRecursive(true).build();
    long fileId = mFileSystemMaster.create(NESTED_URI, options);
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
    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSize(Constants.KB)
            .setRecursive(true).build();
    long fileId = mFileSystemMaster.create(uri, options);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(fileId);
    mBlockMaster.commitBlock(mWorkerId, Constants.KB, 1, blockId, Constants.KB);
    mFileSystemMaster.completeFile(fileId);
    return blockId;
  }
}
