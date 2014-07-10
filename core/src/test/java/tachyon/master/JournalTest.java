/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.master;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TestUtils;
import tachyon.UnderFileSystem;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.conf.MasterConf;
import tachyon.thrift.ClientFileInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;

/**
 * Test master journal, including image and edit log.
 * Most tests will test edit log first, followed by the image.
 */
public class JournalTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFS mTfs = null;

  /**
   * Test add block
   * 
   * @throws Exception
   */
  @Test
  public void AddBlockTest() throws Exception {
    mTfs.createFile("/xyz", 64);
    TachyonFile file = mTfs.getFile("/xyz");
    OutputStream os = file.getOutStream(WriteType.MUST_CACHE);
    for (int k = 0; k < 1000; k ++) {
      os.write(k);
    }
    os.close();
    ClientFileInfo fInfo = mLocalTachyonCluster.getMasterInfo().getClientFileInfo("/xyz");
    mLocalTachyonCluster.stopTFS();
    AddBlockTestUtil(fInfo);
    String editLogPath = mLocalTachyonCluster.getEditLogPath();
    UnderFileSystem.get(editLogPath).delete(editLogPath, true);
    AddBlockTestUtil(fInfo);
  }

  private void AddBlockTestUtil(ClientFileInfo fileInfo) throws IOException, InvalidPathException,
      FileDoesNotExistException {
    Journal journal = new Journal(MasterConf.get().JOURNAL_FOLDER, "image.data", "log.data");
    MasterInfo info = new MasterInfo(new InetSocketAddress(9999), journal);
    info.init();
    Assert.assertEquals(2, info.ls(Constants.PATH_SEPARATOR, true).size());
    Assert.assertTrue(info.getFileId(Constants.PATH_SEPARATOR) != -1);
    Assert.assertTrue(info.getFileId("/xyz") != -1);
    int temp = fileInfo.inMemoryPercentage;
    fileInfo.setInMemoryPercentage(0);
    Assert.assertEquals(fileInfo, info.getClientFileInfo(info.getFileId("/xyz")));
    fileInfo.setInMemoryPercentage(temp);
    info.stop();
  }

  /**
   * Test add checkpoint
   * 
   * @throws Exception
   */
  @Test
  public void AddCheckpointTest() throws Exception {
    TestUtils.createByteFile(mTfs, "/xyz", WriteType.THROUGH, 10);
    ClientFileInfo fInfo = mLocalTachyonCluster.getMasterInfo().getClientFileInfo("/xyz");
    String ckPath = fInfo.getUfsPath();
    mTfs.createFile("/xyz_ck", ckPath);
    ClientFileInfo ckFileInfo = mLocalTachyonCluster.getMasterInfo().getClientFileInfo("/xyz_ck");
    mLocalTachyonCluster.stopTFS();
    AddCheckpointTestUtil(fInfo, ckFileInfo);
    String editLogPath = mLocalTachyonCluster.getEditLogPath();
    UnderFileSystem.get(editLogPath).delete(editLogPath, true);
    AddCheckpointTestUtil(fInfo, ckFileInfo);
  }

  private void AddCheckpointTestUtil(ClientFileInfo fileInfo, ClientFileInfo ckFileInfo)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    Journal journal = new Journal(MasterConf.get().JOURNAL_FOLDER, "image.data", "log.data");
    MasterInfo info = new MasterInfo(new InetSocketAddress(9999), journal);
    info.init();
    Assert.assertEquals(3, info.ls(Constants.PATH_SEPARATOR, true).size());
    Assert.assertTrue(info.getFileId(Constants.PATH_SEPARATOR) != -1);
    Assert.assertTrue(info.getFileId("/xyz") != -1);
    Assert.assertTrue(info.getFileId("/xyz_ck") != -1);
    Assert.assertEquals(fileInfo, info.getClientFileInfo(info.getFileId("/xyz")));
    Assert.assertEquals(ckFileInfo, info.getClientFileInfo(info.getFileId("/xyz_ck")));
    info.stop();
  }

  /**
   * mLocalTachyonCluster is not closed in after(). Need to be closed by any test method.
   * 
   * @throws Exception
   */
  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("fs.hdfs.impl.disable.cache");
  }

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", "1000");
    System.setProperty("fs.hdfs.impl.disable.cache", "true");
    mLocalTachyonCluster = new LocalTachyonCluster(10000);
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
  }

  /**
   * Test completed Editlog deletion
   * 
   * @throws Exception
   */
  @Test
  public void CompltedEditLogDeletionTest() throws Exception {
    Journal journal = mLocalTachyonCluster.getMasterInfo().getJournal();
    journal.setMaxLogSize(Constants.KB);
    for (int i = 0; i < 124; i ++) {
      mTfs.createFile("/a" + i, (i + 10) / 10 * 64);
    }
    mLocalTachyonCluster.stopTFS();
    String editLogPath = mLocalTachyonCluster.getEditLogPath();
    String completedPath =
        editLogPath.substring(0, editLogPath.lastIndexOf(Constants.PATH_SEPARATOR)) + "/completed";
    Assert.assertTrue(UnderFileSystem.get(completedPath).list(completedPath).length > 1);
    MultiEditLogTestUtil();
    Assert.assertTrue(UnderFileSystem.get(completedPath).list(completedPath).length == 0);
    MultiEditLogTestUtil();
  }

  /**
   * Test file and folder creation and deletion;
   * 
   * @throws Exception
   */
  @Test
  public void DeleteTest() throws Exception {
    for (int i = 0; i < 10; i ++) {
      mTfs.mkdir("/i" + i);
      for (int j = 0; j < 10; j ++) {
        mTfs.createFile("/i" + i + "/j" + j, (i + j + 1) * 64);
        if (j >= 5) {
          mTfs.delete("/i" + i + "/j" + j, false);
        }
      }
      if (i >= 5) {
        mTfs.delete("/i" + i, true);
      }
    }
    mLocalTachyonCluster.stopTFS();
    DeleteTestUtil();
    String editLogPath = mLocalTachyonCluster.getEditLogPath();
    UnderFileSystem.get(editLogPath).delete(editLogPath, true);
    DeleteTestUtil();
  }

  private void DeleteTestUtil() throws IOException, InvalidPathException,
      FileDoesNotExistException {
    Journal journal = new Journal(MasterConf.get().JOURNAL_FOLDER, "image.data", "log.data");
    MasterInfo info = new MasterInfo(new InetSocketAddress(9999), journal);
    info.init();
    Assert.assertEquals(31, info.ls(Constants.PATH_SEPARATOR, true).size());
    Assert.assertTrue(info.getFileId(Constants.PATH_SEPARATOR) != -1);
    for (int i = 0; i < 5; i ++) {
      for (int j = 0; j < 5; j ++) {
        Assert.assertTrue(info.getFileId("/i" + i + "/j" + j) != -1);
      }
    }
    info.stop();
  }

  @Test
  public void EmptyImageTest() throws Exception {
    mLocalTachyonCluster.stopTFS();
    Journal journal = new Journal(MasterConf.get().JOURNAL_FOLDER, "image.data", "log.data");
    MasterInfo info = new MasterInfo(new InetSocketAddress(9999), journal);
    info.init();
    Assert.assertEquals(1, info.ls(Constants.PATH_SEPARATOR, true).size());
    Assert.assertTrue(info.getFileId(Constants.PATH_SEPARATOR) != -1);
    info.stop();
  }

  /**
   * Test file and folder creation.
   * 
   * @throws Exception
   */
  @Test
  public void FileFolderTest() throws Exception {
    for (int i = 0; i < 10; i ++) {
      mTfs.mkdir("/i" + i);
      for (int j = 0; j < 10; j ++) {
        mTfs.createFile("/i" + i + "/j" + j, (i + j + 1) * 64);
      }
    }
    mLocalTachyonCluster.stopTFS();
    FileFolderUtil();
    String editLogPath = mLocalTachyonCluster.getEditLogPath();
    UnderFileSystem.get(editLogPath).delete(editLogPath, true);
    FileFolderUtil();
  }

  private void FileFolderUtil() throws IOException, InvalidPathException,
      FileDoesNotExistException {
    Journal journal = new Journal(MasterConf.get().JOURNAL_FOLDER, "image.data", "log.data");
    MasterInfo info = new MasterInfo(new InetSocketAddress(9999), journal);
    info.init();
    Assert.assertEquals(111, info.ls(Constants.PATH_SEPARATOR, true).size());
    Assert.assertTrue(info.getFileId(Constants.PATH_SEPARATOR) != -1);
    for (int i = 0; i < 10; i ++) {
      for (int j = 0; j < 10; j ++) {
        Assert.assertTrue(info.getFileId("/i" + i + "/j" + j) != -1);
      }
    }
    info.stop();
  }

  /**
   * Test files creation.
   * 
   * @throws Exception
   */
  @Test
  public void FileTest() throws Exception {
    mTfs.createFile("/xyz", 64);
    ClientFileInfo fInfo = mLocalTachyonCluster.getMasterInfo().getClientFileInfo("/xyz");
    mLocalTachyonCluster.stopTFS();
    FileTestUtil(fInfo);
    String editLogPath = mLocalTachyonCluster.getEditLogPath();
    UnderFileSystem.get(editLogPath).delete(editLogPath, true);
    FileTestUtil(fInfo);
  }

  private void FileTestUtil(ClientFileInfo fileInfo) throws IOException, InvalidPathException,
      FileDoesNotExistException {
    Journal journal = new Journal(MasterConf.get().JOURNAL_FOLDER, "image.data", "log.data");
    MasterInfo info = new MasterInfo(new InetSocketAddress(9999), journal);
    info.init();
    Assert.assertEquals(2, info.ls(Constants.PATH_SEPARATOR, true).size());
    Assert.assertTrue(info.getFileId(Constants.PATH_SEPARATOR) != -1);
    Assert.assertTrue(info.getFileId("/xyz") != -1);
    Assert.assertEquals(fileInfo, info.getClientFileInfo(info.getFileId("/xyz")));
    info.stop();
  }

  /**
   * Test journalling of inodes being pinned.
   */
  @Test
  public void PinTest() throws Exception {
    mTfs.mkdir("/myFolder");
    int folderId = mTfs.getFileId("/myFolder");
    mTfs.setPinned(folderId, true);
    int file0Id = mTfs.createFile("/myFolder/file0", 64);
    mTfs.setPinned(file0Id, false);
    int file1Id = mTfs.createFile("/myFolder/file1", 64);
    ClientFileInfo folderInfo = mLocalTachyonCluster.getMasterInfo().getClientFileInfo(folderId);
    ClientFileInfo file0Info = mLocalTachyonCluster.getMasterInfo().getClientFileInfo(file0Id);
    ClientFileInfo file1Info = mLocalTachyonCluster.getMasterInfo().getClientFileInfo(file1Id);
    mLocalTachyonCluster.stopTFS();
    PinTestUtil(folderInfo, file0Info, file1Info);
    String editLogPath = mLocalTachyonCluster.getEditLogPath();
    UnderFileSystem.get(editLogPath).delete(editLogPath, true);
    PinTestUtil(folderInfo, file0Info, file1Info);
  }

  private void PinTestUtil(ClientFileInfo folder, ClientFileInfo file0, ClientFileInfo file1)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    Journal journal = new Journal(MasterConf.get().JOURNAL_FOLDER, "image.data", "log.data");
    MasterInfo info = new MasterInfo(new InetSocketAddress(9999), journal);
    info.init();
    Assert.assertEquals(folder, info.getClientFileInfo(info.getFileId("/myFolder")));
    Assert.assertTrue(info.getClientFileInfo(info.getFileId("/myFolder")).isPinned);
    Assert.assertEquals(file0, info.getClientFileInfo(info.getFileId("/myFolder/file0")));
    Assert.assertFalse(info.getClientFileInfo(info.getFileId("/myFolder/file0")).isPinned);
    Assert.assertEquals(file1, info.getClientFileInfo(info.getFileId("/myFolder/file1")));
    Assert.assertTrue(info.getClientFileInfo(info.getFileId("/myFolder/file1")).isPinned);
    info.stop();
  }

  /**
   * Test folder creation.
   * 
   * @throws Exception
   */
  @Test
  public void FolderTest() throws Exception {
    mTfs.mkdir("/xyz");
    ClientFileInfo fInfo = mLocalTachyonCluster.getMasterInfo().getClientFileInfo("/xyz");
    mLocalTachyonCluster.stopTFS();
    FolderTest(fInfo);
    String editLogPath = mLocalTachyonCluster.getEditLogPath();
    UnderFileSystem.get(editLogPath).delete(editLogPath, true);
    FolderTest(fInfo);
  }

  private void FolderTest(ClientFileInfo fileInfo) throws IOException, InvalidPathException,
      FileDoesNotExistException {
    Journal journal = new Journal(MasterConf.get().JOURNAL_FOLDER, "image.data", "log.data");
    MasterInfo info = new MasterInfo(new InetSocketAddress(9999), journal);
    info.init();
    Assert.assertEquals(2, info.ls(Constants.PATH_SEPARATOR, true).size());
    Assert.assertTrue(info.getFileId(Constants.PATH_SEPARATOR) != -1);
    Assert.assertTrue(info.getFileId("/xyz") != -1);
    Assert.assertEquals(fileInfo, info.getClientFileInfo(info.getFileId("/xyz")));
    info.stop();
  }

  /**
   * Test files creation.
   * 
   * @throws Exception
   */
  @Test
  public void ManyFileTest() throws Exception {
    for (int i = 0; i < 10; i ++) {
      mTfs.createFile("/a" + i, (i + 1) * 64);
    }
    mLocalTachyonCluster.stopTFS();
    ManyFileTestUtil();
    String editLogPath = mLocalTachyonCluster.getEditLogPath();
    UnderFileSystem.get(editLogPath).delete(editLogPath, true);
    ManyFileTestUtil();
  }

  private void ManyFileTestUtil() throws IOException, InvalidPathException,
      FileDoesNotExistException {
    Journal journal = new Journal(MasterConf.get().JOURNAL_FOLDER, "image.data", "log.data");
    MasterInfo info = new MasterInfo(new InetSocketAddress(9999), journal);
    info.init();
    Assert.assertEquals(11, info.ls(Constants.PATH_SEPARATOR, true).size());
    Assert.assertTrue(info.getFileId(Constants.PATH_SEPARATOR) != -1);
    for (int k = 0; k < 10; k ++) {
      Assert.assertTrue(info.getFileId("/a" + k) != -1);
    }
    info.stop();
  }

  /**
   * Test reading multiple edit logs.
   * 
   * @throws Exception
   */
  @Test
  public void MultiEditLogTest() throws Exception {
    Journal journal = mLocalTachyonCluster.getMasterInfo().getJournal();
    journal.setMaxLogSize(Constants.KB);
    for (int i = 0; i < 124; i ++) {
      mTfs.createFile("/a" + i, (i + 10) / 10 * 64);
    }
    mLocalTachyonCluster.stopTFS();
    MultiEditLogTestUtil();
    String editLogPath = mLocalTachyonCluster.getEditLogPath();
    UnderFileSystem.get(editLogPath).delete(editLogPath, true);
    MultiEditLogTestUtil();
  }

  private void MultiEditLogTestUtil() throws IOException, InvalidPathException,
      FileDoesNotExistException {
    Journal journal = new Journal(MasterConf.get().JOURNAL_FOLDER, "image.data", "log.data");
    MasterInfo info = new MasterInfo(new InetSocketAddress(9999), journal);
    info.init();
    Assert.assertEquals(125, info.ls(Constants.PATH_SEPARATOR, true).size());
    Assert.assertTrue(info.getFileId(Constants.PATH_SEPARATOR) != -1);
    for (int k = 0; k < 124; k ++) {
      Assert.assertTrue(info.getFileId("/a" + k) != -1);
    }
    info.stop();
  }

  /**
   * Test file and folder creation, and rename;
   * 
   * @throws Exception
   */
  @Test
  public void RenameTest() throws Exception {
    for (int i = 0; i < 10; i ++) {
      mTfs.mkdir("/i" + i);
      for (int j = 0; j < 10; j ++) {
        mTfs.createFile("/i" + i + "/j" + j, (i + j + 1) * 64);
        mTfs.rename("/i" + i + "/j" + j, "/i" + i + "/jj" + j);
      }
      mTfs.rename("/i" + i, "/ii" + i);
    }
    mLocalTachyonCluster.stopTFS();
    RenameTestUtil();
    String editLogPath = mLocalTachyonCluster.getEditLogPath();
    UnderFileSystem.get(editLogPath).delete(editLogPath, true);
    RenameTestUtil();
  }

  private void RenameTestUtil() throws IOException, InvalidPathException,
      FileDoesNotExistException {
    Journal journal = new Journal(MasterConf.get().JOURNAL_FOLDER, "image.data", "log.data");
    MasterInfo info = new MasterInfo(new InetSocketAddress(9999), journal);
    info.init();
    Assert.assertEquals(111, info.ls(Constants.PATH_SEPARATOR, true).size());
    Assert.assertTrue(info.getFileId(Constants.PATH_SEPARATOR) != -1);
    for (int i = 0; i < 10; i ++) {
      for (int j = 0; j < 10; j ++) {
        Assert.assertTrue(info.getFileId("/ii" + i + "/jj" + j) != -1);
      }
    }
    info.stop();
  }

  /**
   * Test folder creation.
   * 
   * @throws Exception
   */
  @Test
  public void TableTest() throws Exception {
    mTfs.createRawTable("/xyz", 10);
    ClientFileInfo fInfo = mLocalTachyonCluster.getMasterInfo().getClientFileInfo("/xyz");
    mLocalTachyonCluster.stopTFS();
    TableTest(fInfo);
    String editLogPath = mLocalTachyonCluster.getEditLogPath();
    UnderFileSystem.get(editLogPath).delete(editLogPath, true);
    TableTest(fInfo);
  }

  private void TableTest(ClientFileInfo fileInfo) throws IOException, InvalidPathException,
      FileDoesNotExistException {
    Journal journal = new Journal(MasterConf.get().JOURNAL_FOLDER, "image.data", "log.data");
    MasterInfo info = new MasterInfo(new InetSocketAddress(9999), journal);
    info.init();
    Assert.assertEquals(12, info.ls(Constants.PATH_SEPARATOR, true).size());
    Assert.assertTrue(info.getFileId(Constants.PATH_SEPARATOR) != -1);
    Assert.assertTrue(info.getFileId("/xyz") != -1);
    Assert.assertEquals(fileInfo, info.getClientFileInfo(info.getFileId("/xyz")));
    info.stop();
  }
}
