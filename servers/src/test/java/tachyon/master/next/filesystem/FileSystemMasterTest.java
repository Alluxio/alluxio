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

package tachyon.master.next.filesystem;

import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.master.next.block.BlockMaster;
import tachyon.master.next.journal.Journal;
import tachyon.thrift.FileInfo;
import tachyon.thrift.NetAddress;

/**
 * Unit tests for tachyon.master.filesystem.FileSystemMaster.
 *
 */
public final class FileSystemMasterTest {
  private static final TachyonURI NESTED_URI = new TachyonURI("/nested/test");
  private static final TachyonURI NESTED_FILE_URI = new TachyonURI("/nested/test/file");

  private final TachyonConf mTachyonConf = new TachyonConf();
  private final Journal mJournal = new Journal("directory", mTachyonConf);
  private BlockMaster mBlockMaster;
  private FileSystemMaster mFileSystemMaster;
  private long mWorkerId;

  @Before
  public void before() throws Exception {
    mBlockMaster = new BlockMaster(mJournal, mTachyonConf);
    mFileSystemMaster = new FileSystemMaster(mTachyonConf, mBlockMaster, mJournal);

    // set up worker
    mWorkerId = mBlockMaster.getWorkerId(new NetAddress("localhost", 80, 81));
    mBlockMaster.workerRegister(mWorkerId, Lists.newArrayList(Constants.MB * 1L, Constants.MB * 1L),
        Lists.<Long>newArrayList(Constants.KB * 1L, Constants.KB * 1L),
        Maps.<Long, List<Long>>newHashMap());
  }

  @Test
  public void isDirectoryTest() throws Exception {
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, Constants.KB, true);
    Assert.assertFalse(mFileSystemMaster.isDirectory(fileId));
    Assert.assertTrue(mFileSystemMaster.isDirectory(mFileSystemMaster.getFileId(NESTED_URI)));
  }

  @Test
  public void getNewBlockIdForFileTest() throws Exception {
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, Constants.KB, true);
    long blockId = mFileSystemMaster.getNewBlockIdForFile(fileId);
    FileInfo fileInfo = mFileSystemMaster.getFileInfo(fileId);
    Assert.assertEquals(Lists.newArrayList(blockId), fileInfo.getBlockIds());
  }

  @Test
  public void isFullyInMemoryTest() throws Exception {
    // empty file
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, Constants.KB, true);
    Assert.assertTrue(mFileSystemMaster.isFullyInMemory(NESTED_FILE_URI));

    // add in-memory block
    long blockId = mFileSystemMaster.getNewBlockIdForFile(fileId);
    mBlockMaster.commitBlock(mWorkerId, Constants.KB, 1, blockId, Constants.KB);
    Assert.assertTrue(mFileSystemMaster.isFullyInMemory(NESTED_FILE_URI));

    // add SSD block
    blockId = mFileSystemMaster.getNewBlockIdForFile(fileId);
    mBlockMaster.commitBlock(mWorkerId, Constants.KB, 2, blockId, Constants.KB);
    mFileSystemMaster.completeFile(fileId);
    Assert.assertFalse(mFileSystemMaster.isFullyInMemory(NESTED_FILE_URI));
  }

  @Test
  public void renameTest() throws Exception {
    // src
    long fileId = mFileSystemMaster.createFile(NESTED_FILE_URI, Constants.KB, true);
    TachyonURI dstPath = new TachyonURI("/dst");
    Assert.assertTrue(mFileSystemMaster.rename(fileId, dstPath));
  }
}
