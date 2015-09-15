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
import tachyon.client.ClientOptions;
import tachyon.client.TachyonFSTestUtils;
import tachyon.client.TachyonStorageType;
import tachyon.client.UnderStorageType;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.journal.Journal;
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
  public void AddBlockTest() throws Exception {
    TachyonURI uri = new TachyonURI("/xyz");
    ClientOptions options = new ClientOptions.Builder(mMasterTachyonConf).setBlockSize(64).build();
    FileOutStream os = mTfs.getOutStream(uri, options);
    for (int k = 0; k < 1000; k ++) {
      os.write(k);
    }
    os.close();
    FileInfo fInfo = mTfs.getInfo(mTfs.open(uri));
    mLocalTachyonCluster.stopTFS();
    AddBlockTestUtil(fInfo);
  }

  private FileSystemMaster createFsMasterFromJournal() throws IOException {
    return MasterTestUtils.createFileSystemMasterFromJournal(mMasterTachyonConf);
  }

  private void deleteFsMasterJournalLogs() throws IOException {
    String journalFolder = mLocalTachyonCluster.getMaster().getJournalFolder();
    Journal journal = new Journal(PathUtils.concatPath(journalFolder,
        Constants.FILE_SYSTEM_MASTER_SERVICE_NAME), mMasterTachyonConf);
    UnderFileSystem.get(journalFolder, mMasterTachyonConf).delete(journal.getCurrentLogFilePath(),
        true);
  }

  private void AddBlockTestUtil(FileInfo fileInfo) throws IOException, InvalidPathException,
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
  public void AddCheckpointTest() throws Exception {
    ClientOptions options =
        new ClientOptions.Builder(mMasterTachyonConf).setStorageTypes(TachyonStorageType
            .NO_STORE, UnderStorageType.PERSIST).build();
    TachyonFSTestUtils.createByteFile(mTfs, "/xyz", options, 10);
    FileInfo fInfo = mTfs.getInfo(mTfs.open(new TachyonURI("/xyz")));
    TachyonURI ckPath = new TachyonURI("/xyz_ck");
    // TODO(cc): What's the counterpart in the new client API for this?
    mTfs.loadFileInfoFromUfs(new TachyonURI("/xyz_ck"), new TachyonURI(fInfo.getUfsPath()), true);
    FileInfo ckFileInfo = mTfs.getInfo(mTfs.open(ckPath));
    mLocalTachyonCluster.stopTFS();
    AddCheckpointTestUtil(fInfo, ckFileInfo);
    deleteFsMasterJournalLogs();
    AddCheckpointTestUtil(fInfo, ckFileInfo);
  }

  private void AddCheckpointTestUtil(FileInfo fileInfo, FileInfo ckFileInfo)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != -1);
    Assert.assertEquals(2, fsMaster.getFileInfoList(rootId).size());
    Assert.assertTrue(fsMaster.getFileId(new TachyonURI("/xyz")) != -1);
    Assert.assertTrue(fsMaster.getFileId(new TachyonURI("/xyz_ck")) != -1);
    Assert.assertEquals(fileInfo, fsMaster.getFileInfo(fsMaster.getFileId(new TachyonURI("/xyz"))));
    Assert.assertEquals(ckFileInfo,
        fsMaster.getFileInfo(fsMaster.getFileId(new TachyonURI("/xyz_ck"))));
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
    TachyonConf conf = new TachyonConf();
    conf.set(Constants.MASTER_JOURNAL_MAX_LOG_SIZE_BYTES, Integer.toString(Constants.KB));
    mLocalTachyonCluster.start(conf);
    mTfs = mLocalTachyonCluster.getClient();
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
  }

  /**
   * Test completed Editlog deletion
   *
   * @throws Exception
   */
  @Test
  public void CompletedEditLogDeletionTest() throws Exception {
    for (int i = 0; i < 124; i ++) {
      mTfs.getOutStream(new TachyonURI("/a" + i), new ClientOptions.Builder(mMasterTachyonConf)
        .setBlockSize((i + 10) / 10 * 64).build()).close();
    }
    mLocalTachyonCluster.stopTFS();

    String journalFolder =
        FileSystemMaster.getJournalDirectory(mLocalTachyonCluster.getMaster().getJournalFolder());
    Journal journal = new Journal(journalFolder, mMasterTachyonConf);
    String completedPath = journal.getCompletedDirectory();
    Assert.assertTrue(UnderFileSystem.get(completedPath,
        mMasterTachyonConf).list(completedPath).length > 1);
    MultiEditLogTestUtil();
    Assert.assertTrue(UnderFileSystem.get(completedPath,
        mMasterTachyonConf).list(completedPath).length <= 1);
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
      String dirPath = "/i" + i;
      mTfs.mkdirs(new TachyonURI(dirPath));
      for (int j = 0; j < 10; j ++) {
        ClientOptions option =
            new ClientOptions.Builder(mMasterTachyonConf).setBlockSize((i + j + 1) * 64).build();
        String filePath = dirPath + "/j" + j;
        mTfs.getOutStream(new TachyonURI(filePath), option).close();
        if (j >= 5) {
          mTfs.delete(mTfs.open(new TachyonURI(filePath)));
        }
      }
      if (i >= 5) {
        mTfs.delete(mTfs.open(new TachyonURI(dirPath)));
      }
    }
    mLocalTachyonCluster.stopTFS();
    DeleteTestUtil();
    deleteFsMasterJournalLogs();
    DeleteTestUtil();
  }

  private void DeleteTestUtil() throws IOException, InvalidPathException,
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
  public void EmptyImageTest() throws Exception {
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
  public void FileFolderTest() throws Exception {
    for (int i = 0; i < 10; i ++) {
      mTfs.mkdirs(new TachyonURI("/i" + i));
      for (int j = 0; j < 10; j ++) {
        ClientOptions option =
            new ClientOptions.Builder(mMasterTachyonConf).setBlockSize((i + j + 1) * 64).build();
        mTfs.getOutStream(new TachyonURI("/i" + i + "/j" + j), option).close();
      }
    }
    mLocalTachyonCluster.stopTFS();
    FileFolderUtil();
    deleteFsMasterJournalLogs();
    FileFolderUtil();
  }

  private void FileFolderUtil() throws IOException, InvalidPathException,
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
  public void FileTest() throws Exception {
    ClientOptions option = new ClientOptions.Builder(mMasterTachyonConf).setBlockSize(64).build();
    TachyonURI filePath = new TachyonURI("/xyz");
    mTfs.getOutStream(filePath, option).close();
    FileInfo fInfo = mTfs.getInfo(mTfs.open(filePath));
    mLocalTachyonCluster.stopTFS();
    FileTestUtil(fInfo);
    deleteFsMasterJournalLogs();
    FileTestUtil(fInfo);
  }

  private void FileTestUtil(FileInfo fileInfo) throws IOException, InvalidPathException,
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
  public void PinTest() throws Exception {
    mTfs.mkdirs(new TachyonURI("/myFolder"));
    TachyonFile folder = mTfs.open(new TachyonURI("/myFolder"));
    mTfs.setPin(folder, true);

    TachyonURI file0Path = new TachyonURI("/myFolder/file0");
    ClientOptions op = new ClientOptions.Builder(mMasterTachyonConf).setBlockSize(64).build();
    mTfs.getOutStream(file0Path, op).close();
    TachyonFile file0 = mTfs.open(file0Path);
    mTfs.setPin(file0, false);

    TachyonURI file1Path = new TachyonURI("/myFolder/file1");
    mTfs.getOutStream(file1Path, op).close();

    FileInfo folderInfo = mTfs.getInfo(folder);
    FileInfo file0Info = mTfs.getInfo(file0);
    FileInfo file1Info = mTfs.getInfo(mTfs.open(file1Path));

    mLocalTachyonCluster.stopTFS();

    PinTestUtil(folderInfo, file0Info, file1Info);
    deleteFsMasterJournalLogs();
    PinTestUtil(folderInfo, file0Info, file1Info);
  }

  private void PinTestUtil(FileInfo folder, FileInfo file0, FileInfo file1)
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
  public void FolderTest() throws Exception {
    TachyonURI folderPath = new TachyonURI("/xyz");
    mTfs.mkdirs(folderPath);
    FileInfo fInfo = mTfs.getInfo(mTfs.open(folderPath));
    mLocalTachyonCluster.stopTFS();
    FolderTest(fInfo);
    deleteFsMasterJournalLogs();
    FolderTest(fInfo);
  }

  private void FolderTest(FileInfo fileInfo) throws IOException, InvalidPathException,
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
  public void ManyFileTest() throws Exception {
    for (int i = 0; i < 10; i ++) {
      ClientOptions option = new ClientOptions.Builder(mMasterTachyonConf).setBlockSize(
          (i + 1) * 64).build();
      mTfs.getOutStream(new TachyonURI("/a" + i), option).close();
    }
    mLocalTachyonCluster.stopTFS();
    ManyFileTestUtil();
    deleteFsMasterJournalLogs();
    ManyFileTestUtil();
  }

  private void ManyFileTestUtil() throws IOException, InvalidPathException,
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
  public void MultiEditLogTest() throws Exception {
    for (int i = 0; i < 124; i ++) {
      ClientOptions op = new ClientOptions.Builder(mMasterTachyonConf).setBlockSize(
          (i + 10) / 10 * 64).build();
      mTfs.getOutStream(new TachyonURI("/a" + i), op);
    }
    mLocalTachyonCluster.stopTFS();
    MultiEditLogTestUtil();
    deleteFsMasterJournalLogs();
    MultiEditLogTestUtil();
  }

  private void MultiEditLogTestUtil() throws IOException, InvalidPathException,
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
  //  ufs.mkdirs(journalPrefix, true);
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
  public void RenameTest() throws Exception {
    for (int i = 0; i < 10; i ++) {
      mTfs.mkdirs(new TachyonURI("/i" + i));
      for (int j = 0; j < 10; j ++) {
        ClientOptions option = new ClientOptions.Builder(mMasterTachyonConf).setBlockSize(
            (i + j + 1) * 64).build();
        TachyonURI path = new TachyonURI("/i" + i + "/j" + j);
        mTfs.getOutStream(path, option).close();
        mTfs.rename(mTfs.open(path), new TachyonURI("/i" + i + "/jj" + j));
      }
      mTfs.rename(mTfs.open(new TachyonURI("/i" + i)), new TachyonURI("/ii" + i));
    }
    mLocalTachyonCluster.stopTFS();
    RenameTestUtil();
    deleteFsMasterJournalLogs();
    RenameTestUtil();
  }

  private void RenameTestUtil()
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
