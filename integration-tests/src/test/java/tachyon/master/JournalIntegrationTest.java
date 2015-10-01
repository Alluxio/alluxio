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

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.DeleteOptions;
import tachyon.client.file.options.LoadMetadataOptions;
import tachyon.client.file.options.MkdirOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.client.file.options.SetStateOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.journal.Journal;
import tachyon.master.journal.ReadWriteJournal;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.InvalidPathException;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.PathUtils;

/**
 * Test master journal, including checkpoint and entry log. Most tests will test entry log first,
 * followed by the checkpoint.
 */
public class JournalIntegrationTest {
  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFileSystem mTfs = null;
  private TachyonURI mRootUri = new TachyonURI(TachyonURI.SEPARATOR);
  private final ExecutorService mExecutorService = Executors.newFixedThreadPool(2);
  private TachyonConf mMasterTachyonConf =  null;

  /**
   * Test add block
   *
   * @throws Exception
   */
  @Test
  public void addBlockTest() throws Exception {
    TachyonURI uri = new TachyonURI("/xyz");
    OutStreamOptions options =
        new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSize(64).build();
    FileOutStream os = mTfs.getOutStream(uri, options);
    for (int k = 0; k < 1000; k ++) {
      os.write(k);
    }
    os.close();
    FileInfo fInfo = mTfs.getInfo(mTfs.open(uri));
    mLocalTachyonCluster.stopTFS();
    addBlockTestUtil(fInfo);
  }

  private FileSystemMaster createFsMasterFromJournal() throws IOException {
    return MasterTestUtils.createFileSystemMasterFromJournal(mMasterTachyonConf);
  }

  private void deleteFsMasterJournalLogs() throws IOException {
    String journalFolder = mLocalTachyonCluster.getMaster().getJournalFolder();
    Journal journal = new ReadWriteJournal(
        PathUtils.concatPath(journalFolder, Constants.FILE_SYSTEM_MASTER_SERVICE_NAME));
    UnderFileSystem.get(journalFolder, mMasterTachyonConf).delete(journal.getCurrentLogFilePath(),
        true);
  }

  private void addBlockTestUtil(FileInfo fileInfo) throws IOException, InvalidPathException,
      FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != -1);
    Assert.assertEquals(1, fsMaster.getFileInfoList(rootId).size());
    long xyzId = fsMaster.getFileId(new TachyonURI("/xyz"));
    Assert.assertTrue(xyzId != -1);
    int temp = fileInfo.inMemoryPercentage;
    fileInfo.setInMemoryPercentage(0);
    Assert.assertEquals(fileInfo, fsMaster.getFileInfo(xyzId));
    fileInfo.setInMemoryPercentage(temp);
    fsMaster.stop();
  }

  /**
   * Test add checkpoint
   *
   * @throws Exception
   */
  @Test
  public void loadMetadataTest() throws Exception {
    String ufsRoot =
        PathUtils.concatPath(mLocalTachyonCluster.getMasterTachyonConf().get(
            Constants.UNDERFS_DATA_FOLDER));
    UnderFileSystem ufs = UnderFileSystem.get(ufsRoot, mLocalTachyonCluster.getMasterTachyonConf());
    ufs.create(ufsRoot + "/xyz");
    try {
      mTfs.getInfo(mTfs.open(new TachyonURI("/xyz")));
      Assert.fail("File /xyz should not exist.");
    } catch (TachyonException e) {
      // This is to expected.
    }
    LoadMetadataOptions recursive =
        new LoadMetadataOptions.Builder(new TachyonConf()).setRecursive(true).build();
    mTfs.loadMetadata(new TachyonURI("/xyz"), recursive);
    FileInfo fileInfo = mTfs.getInfo(mTfs.open(new TachyonURI("/xyz")));
    mLocalTachyonCluster.stopTFS();
    loadMetadataTestUtil(fileInfo);
    deleteFsMasterJournalLogs();
    loadMetadataTestUtil(fileInfo);
  }

  private void loadMetadataTestUtil(FileInfo fileInfo) throws IOException,
      InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != -1);
    Assert.assertEquals(1, fsMaster.getFileInfoList(rootId).size());
    Assert.assertTrue(fsMaster.getFileId(new TachyonURI("/xyz")) != -1);
    Assert.assertEquals(fileInfo, fsMaster.getFileInfo(fsMaster.getFileId(new TachyonURI("/xyz"))));
    fsMaster.stop();
  }

  /**
   * mLocalTachyonCluster is not closed in after(). Need to be closed by any test method.
   *
   * @throws Exception
   */
  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    mExecutorService.shutdown();
  }

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster = new LocalTachyonCluster(Constants.GB, 100, Constants.GB);
    MasterContext.getConf().set(Constants.MASTER_JOURNAL_MAX_LOG_SIZE_BYTES, Integer.toString(
        Constants.KB));
    mLocalTachyonCluster.start();
    mTfs = mLocalTachyonCluster.getClient();
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
  }

  /**
   * Test completed Editlog deletion
   *
   * @throws Exception
   */
  @Test
  public void completedEditLogDeletionTest() throws Exception {
    for (int i = 0; i < 124; i ++) {
      mTfs.getOutStream(new TachyonURI("/a" + i),
          new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSize((i + 10) / 10 * 64).build())
          .close();
    }
    mLocalTachyonCluster.stopTFS();

    String journalFolder =
        FileSystemMaster.getJournalDirectory(mLocalTachyonCluster.getMaster().getJournalFolder());
    Journal journal = new ReadWriteJournal(journalFolder);
    String completedPath = journal.getCompletedDirectory();
    Assert.assertTrue(UnderFileSystem.get(completedPath,
        mMasterTachyonConf).list(completedPath).length > 1);
    multiEditLogTestUtil();
    Assert.assertTrue(UnderFileSystem.get(completedPath,
        mMasterTachyonConf).list(completedPath).length <= 1);
    multiEditLogTestUtil();
  }

  /**
   * Test file and folder creation and deletion;
   *
   * @throws Exception
   */
  @Test
  public void deleteTest() throws Exception {
    MkdirOptions recMkdir = new MkdirOptions.Builder(new TachyonConf()).setRecursive(true).build();
    DeleteOptions recDelete =
        new DeleteOptions.Builder(new TachyonConf()).setRecursive(true).build();
    for (int i = 0; i < 10; i ++) {
      String dirPath = "/i" + i;
      mTfs.mkdir(new TachyonURI(dirPath), recMkdir);
      for (int j = 0; j < 10; j ++) {
        OutStreamOptions option =
            new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSize((i + j + 1) * 64).build();
        String filePath = dirPath + "/j" + j;
        mTfs.getOutStream(new TachyonURI(filePath), option).close();
        if (j >= 5) {
          mTfs.delete(mTfs.open(new TachyonURI(filePath)), recDelete);
        }
      }
      if (i >= 5) {
        mTfs.delete(mTfs.open(new TachyonURI(dirPath)), recDelete);
      }
    }
    mLocalTachyonCluster.stopTFS();
    deleteTestUtil();
    deleteFsMasterJournalLogs();
    deleteTestUtil();
  }

  private void deleteTestUtil() throws IOException, InvalidPathException,
      FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != -1);
    Assert.assertEquals(5, fsMaster.getFileInfoList(rootId).size());
    for (int i = 0; i < 5; i ++) {
      for (int j = 0; j < 5; j ++) {
        Assert.assertTrue(fsMaster.getFileId(new TachyonURI("/i" + i + "/j" + j)) != -1);
      }
    }
    fsMaster.stop();
  }

  @Test
  public void emptyImageTest() throws Exception {
    mLocalTachyonCluster.stopTFS();
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != -1);
    Assert.assertEquals(0, fsMaster.getFileInfoList(rootId).size());
    fsMaster.stop();
  }

  /**
   * Test file and folder creation.
   *
   * @throws Exception
   */
  @Test
  public void fileFolderTest() throws Exception {
    for (int i = 0; i < 10; i ++) {
      mTfs.mkdir(new TachyonURI("/i" + i));
      for (int j = 0; j < 10; j ++) {
        OutStreamOptions option =
            new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSize((i + j + 1) * 64).build();
        mTfs.getOutStream(new TachyonURI("/i" + i + "/j" + j), option).close();
      }
    }
    mLocalTachyonCluster.stopTFS();
    fileFolderUtil();
    deleteFsMasterJournalLogs();
    fileFolderUtil();
  }

  private void fileFolderUtil() throws IOException, InvalidPathException,
      FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != -1);
    Assert.assertEquals(10, fsMaster.getFileInfoList(rootId).size());
    for (int i = 0; i < 10; i ++) {
      for (int j = 0; j < 10; j ++) {
        Assert.assertTrue(fsMaster.getFileId(new TachyonURI("/i" + i + "/j" + j)) != -1);
      }
    }
    fsMaster.stop();
  }

  /**
   * Test files creation.
   *
   * @throws Exception
   */
  @Test
  public void fileTest() throws Exception {
    OutStreamOptions option =
        new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSize(64).build();
    TachyonURI filePath = new TachyonURI("/xyz");
    mTfs.getOutStream(filePath, option).close();
    FileInfo fInfo = mTfs.getInfo(mTfs.open(filePath));
    mLocalTachyonCluster.stopTFS();
    fileTestUtil(fInfo);
    deleteFsMasterJournalLogs();
    fileTestUtil(fInfo);
  }

  private void fileTestUtil(FileInfo fileInfo) throws IOException, InvalidPathException,
      FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != -1);
    Assert.assertEquals(1, fsMaster.getFileInfoList(rootId).size());
    long fileId = fsMaster.getFileId(new TachyonURI("/xyz"));
    Assert.assertTrue(fileId != -1);
    Assert.assertEquals(fileInfo, fsMaster.getFileInfo(fileId));
    fsMaster.stop();
  }

  /**
   * Test journalling of inodes being pinned.
   */
  @Test
  public void pinTest() throws Exception {
    SetStateOptions setPinned =
        new SetStateOptions.Builder(new TachyonConf()).setPinned(true).build();
    SetStateOptions setUnpinned =
        new SetStateOptions.Builder(new TachyonConf()).setPinned(false).build();
    mTfs.mkdir(new TachyonURI("/myFolder"));
    TachyonFile folder = mTfs.open(new TachyonURI("/myFolder"));
    mTfs.setState(folder, setPinned);

    TachyonURI file0Path = new TachyonURI("/myFolder/file0");
    OutStreamOptions op = new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSize(64).build();
    mTfs.getOutStream(file0Path, op).close();
    TachyonFile file0 = mTfs.open(file0Path);
    mTfs.setState(file0, setUnpinned);

    TachyonURI file1Path = new TachyonURI("/myFolder/file1");
    mTfs.getOutStream(file1Path, op).close();

    FileInfo folderInfo = mTfs.getInfo(folder);
    FileInfo file0Info = mTfs.getInfo(file0);
    FileInfo file1Info = mTfs.getInfo(mTfs.open(file1Path));

    mLocalTachyonCluster.stopTFS();

    pinTestUtil(folderInfo, file0Info, file1Info);
    deleteFsMasterJournalLogs();
    pinTestUtil(folderInfo, file0Info, file1Info);
  }

  private void pinTestUtil(FileInfo folder, FileInfo file0, FileInfo file1)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    FileInfo info = fsMaster.getFileInfo(fsMaster.getFileId(new TachyonURI("/myFolder")));
    Assert.assertEquals(folder, info);
    Assert.assertTrue(info.isPinned);

    info = fsMaster.getFileInfo(fsMaster.getFileId(new TachyonURI("/myFolder/file0")));
    Assert.assertEquals(file0, info);
    Assert.assertFalse(info.isPinned);

    info = fsMaster.getFileInfo(fsMaster.getFileId(new TachyonURI("/myFolder/file1")));
    Assert.assertEquals(file1, info);
    Assert.assertTrue(info.isPinned);

    fsMaster.stop();
  }

  /**
   * Test folder creation.
   *
   * @throws Exception
   */
  @Test
  public void folderTest() throws Exception {
    TachyonURI folderPath = new TachyonURI("/xyz");
    mTfs.mkdir(folderPath);
    FileInfo fInfo = mTfs.getInfo(mTfs.open(folderPath));
    mLocalTachyonCluster.stopTFS();
    folderTestUtil(fInfo);
    deleteFsMasterJournalLogs();
    folderTestUtil(fInfo);
  }

  private void folderTestUtil(FileInfo fileInfo) throws IOException, InvalidPathException,
      FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != -1);
    Assert.assertEquals(1, fsMaster.getFileInfoList(rootId).size());
    long fileId = fsMaster.getFileId(new TachyonURI("/xyz"));
    Assert.assertTrue(fileId != -1);
    Assert.assertEquals(fileInfo, fsMaster.getFileInfo(fileId));
    fsMaster.stop();
  }

  /**
   * Test files creation.
   *
   * @throws Exception
   */
  @Test
  public void manyFileTest() throws Exception {
    for (int i = 0; i < 10; i ++) {
      OutStreamOptions option =
          new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSize((i + 1) * 64).build();
      mTfs.getOutStream(new TachyonURI("/a" + i), option).close();
    }
    mLocalTachyonCluster.stopTFS();
    manyFileTestUtil();
    deleteFsMasterJournalLogs();
    manyFileTestUtil();
  }

  private void manyFileTestUtil() throws IOException, InvalidPathException,
      FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != -1);
    Assert.assertEquals(10, fsMaster.getFileInfoList(rootId).size());
    for (int k = 0; k < 10; k ++) {
      Assert.assertTrue(fsMaster.getFileId(new TachyonURI("/a" + k)) != -1);
    }
    fsMaster.stop();
  }

  /**
   * Test reading multiple edit logs.
   *
   * @throws Exception
   */
  @Test
  public void multiEditLogTest() throws Exception {
    for (int i = 0; i < 124; i ++) {
      OutStreamOptions op =
          new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSize((i + 10) / 10 * 64).build();
      mTfs.getOutStream(new TachyonURI("/a" + i), op);
    }
    mLocalTachyonCluster.stopTFS();
    multiEditLogTestUtil();
    deleteFsMasterJournalLogs();
    multiEditLogTestUtil();
  }

  private void multiEditLogTestUtil() throws IOException, InvalidPathException,
      FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != -1);
    Assert.assertEquals(124, fsMaster.getFileInfoList(rootId).size());
    for (int k = 0; k < 124; k ++) {
      Assert.assertTrue(fsMaster.getFileId(new TachyonURI("/a" + k)) != -1);
    }
    fsMaster.stop();
  }

  // TODO(cc) The edit log behavior this test was testing no longer exists, do we need to add it
  // back?
  ///**
  // * Test renaming completed edit logs.
  // *
  // * @throws Exception
  // */
  //@Test
  //public void RenameEditLogTest() throws Exception {
  //  String journalPrefix = "/tmp/JournalDir" + String.valueOf(System.currentTimeMillis());
  //  Journal journal = new Journal(journalPrefix, mMasterTachyonConf);
  //  UnderFileSystem ufs = UnderFileSystem.get(journalPrefix, mMasterTachyonConf);
  //  ufs.delete(journalPrefix, true);
  //  ufs.mkdir(journalPrefix, true);
  //  OutputStream ops = ufs.create(journal.getCurrentLogFilePath());
  //  if (ops != null) {
  //    ops.close();
  //  }
  //  if (ufs != null) {
  //    ufs.close();
  //  }

  //  // Write operation and flush them to completed directory.
  //  JournalWriter journalWriter = journal.getNewWriter();
  //  journalWriter.setMaxLogSize(100);
  //  JournalOutputStream entryOs = journalWriter.getEntryOutputStream();
  //  for (int i = 0; i < 124; i ++) {
  //    entryOs.writeEntry(new InodeFileEntry(System.currentTimeMillis(), i, "/sth" + i, 0L, false,
  //        System.currentTimeMillis(), Constants.DEFAULT_BLOCK_SIZE_BYTE, 10, false, false,
  //        "/sth" + i, Lists.newArrayList(1L)));
  //    entryOs.flush();
  //  }
  //  entryOs.close();

  //  // Rename completed edit logs when loading them.
  //  String completedDir = journal.getCompletedDirectory();
  //  ufs = UnderFileSystem.get(completedDir, mMasterTachyonConf);
  //  int numOfCompleteFiles = ufs.list(completedDir).length;
  //  Assert.assertTrue(numOfCompleteFiles > 0);
  //  EditLog.setBackUpLogStartNum(numOfCompleteFiles / 2);
  //  log = new EditLog(journalPath, false, 0, mMasterTachyonConf);
  //  int numOfCompleteFilesLeft = numOfCompleteFiles - numOfCompleteFiles / 2 + 1;
  //  Assert.assertEquals(numOfCompleteFilesLeft, ufs.list(completedStr).length);
  //  for (int i = 0; i < numOfCompleteFilesLeft; i ++) {
  //    Assert.assertTrue(ufs.exists(completedStr + i + ".editLog"));
  //  }
  //  EditLog.setBackUpLogStartNum(-1);
  //  log.close();
  //  ufs.delete(journalPrefix, true);
  //  if (ufs != null) {
  //    ufs.close();
  //  }
  //}

  /**
   * Test file and folder creation, and rename;
   *
   * @throws Exception
   */
  @Test
  public void renameTest() throws Exception {
    for (int i = 0; i < 10; i ++) {
      mTfs.mkdir(new TachyonURI("/i" + i));
      for (int j = 0; j < 10; j ++) {
        OutStreamOptions option =
            new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSize((i + j + 1) * 64).build();
        TachyonURI path = new TachyonURI("/i" + i + "/j" + j);
        mTfs.getOutStream(path, option).close();
        mTfs.rename(mTfs.open(path), new TachyonURI("/i" + i + "/jj" + j));
      }
      mTfs.rename(mTfs.open(new TachyonURI("/i" + i)), new TachyonURI("/ii" + i));
    }
    mLocalTachyonCluster.stopTFS();
    renameTestUtil();
    deleteFsMasterJournalLogs();
    renameTestUtil();
  }

  private void renameTestUtil()
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != -1);
    Assert.assertEquals(10, fsMaster.getFileInfoList(rootId).size());
    for (int i = 0; i < 10; i ++) {
      for (int j = 0; j < 10; j ++) {
        Assert.assertTrue(fsMaster.getFileId(new TachyonURI("/ii" + i + "/jj" + j)) != -1);
      }
    }
    fsMaster.stop();
  }

  // TODO(cc) Add these back when there is new RawTable client API.
  ///**
  // * Test folder creation.
  // *
  // * @throws Exception
  // */
  //@Test
  //public void TableTest() throws Exception {
  //  mTfs.createRawTable(new TachyonURI("/xyz"), 10);
  //  FileInfo fInfo =
  //      mLocalTachyonCluster.getMasterInfo().getClientFileInfo(new TachyonURI("/xyz"));
  //  mLocalTachyonCluster.stopTFS();
  //  TableTest(fInfo);
  //  String editLogPath = mLocalTachyonCluster.getEditLogPath();
  //  UnderFileSystem.get(editLogPath, mMasterTachyonConf).delete(editLogPath, true);
  //  TableTest(fInfo);
  //}

  //private void TableTest(FileInfo fileInfo) throws IOException, InvalidPathException,
  //    FileDoesNotExistException {
  //  String masterJournal = mMasterTachyonConf.get(Constants.MASTER_JOURNAL_FOLDER);
  //  Journal journal = new Journal(masterJournal, "image.data", "log.data", mMasterTachyonConf);
  //  MasterInfo info = new MasterInfo(new InetSocketAddress(9999), journal, mExecutorService,
  //      mMasterTachyonConf);
  //  info.init();
  //  Assert.assertEquals(12, info.ls(mRootUri, true).size());
  //  Assert.assertTrue(info.getFileId(mRootUri) != -1);
  //  Assert.assertTrue(info.getFileId(new TachyonURI("/xyz")) != -1);
  //  Assert.assertEquals(fileInfo, info.getClientFileInfo(info.getFileId(new TachyonURI("/xyz"))));
  //  info.stop();
  //}
}
