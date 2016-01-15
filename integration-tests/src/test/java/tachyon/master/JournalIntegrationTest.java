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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.client.ClientContext;
import tachyon.client.UnderStorageType;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.DeleteOptions;
import tachyon.client.file.options.MkdirOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.client.file.options.SetStateOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.master.file.FileSystemMaster;
import tachyon.master.journal.Journal;
import tachyon.master.journal.ReadWriteJournal;
import tachyon.thrift.FileInfo;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.IdUtils;
import tachyon.util.io.PathUtils;

/**
 * Test master journal, including checkpoint and entry log. Most tests will test entry log first,
 * followed by the checkpoint.
 */
public class JournalIntegrationTest {
  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(Constants.GB, 100, Constants.GB,
          Constants.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, Integer.toString(Constants.KB));

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private TachyonFileSystem mTfs = null;
  private TachyonURI mRootUri = new TachyonURI(TachyonURI.SEPARATOR);
  private final ExecutorService mExecutorService = Executors.newFixedThreadPool(2);
  private TachyonConf mMasterTachyonConf = null;

  /**
   * Test add block
   *
   * @throws Exception
   */
  @Test
  public void addBlockTest() throws Exception {
    TachyonURI uri = new TachyonURI("/xyz");
    OutStreamOptions options =
        new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSizeBytes(64).build();
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
        PathUtils.concatPath(journalFolder, Constants.FILE_SYSTEM_MASTER_NAME));
    UnderFileSystem.get(journalFolder, mMasterTachyonConf).delete(journal.getCurrentLogFilePath(),
        true);
  }

  private void addBlockTestUtil(FileInfo fileInfo)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.getFileInfoList(rootId).size());
    long xyzId = fsMaster.getFileId(new TachyonURI("/xyz"));
    Assert.assertTrue(xyzId != IdUtils.INVALID_FILE_ID);
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
    String ufsRoot = PathUtils
        .concatPath(mLocalTachyonCluster.getMasterTachyonConf().get(Constants.UNDERFS_ADDRESS));
    UnderFileSystem ufs = UnderFileSystem.get(ufsRoot, mLocalTachyonCluster.getMasterTachyonConf());
    ufs.create(ufsRoot + "/xyz");
    FileInfo fileInfo = mTfs.getInfo(mTfs.open(new TachyonURI("/xyz")));
    mLocalTachyonCluster.stopTFS();
    loadMetadataTestUtil(fileInfo);
    deleteFsMasterJournalLogs();
    loadMetadataTestUtil(fileInfo);
  }

  private void loadMetadataTestUtil(FileInfo fileInfo)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.getFileInfoList(rootId).size());
    Assert.assertTrue(fsMaster.getFileId(new TachyonURI("/xyz")) != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(fileInfo, fsMaster.getFileInfo(fsMaster.getFileId(new TachyonURI("/xyz"))));
    fsMaster.stop();
  }

  /**
   * @throws Exception
   */
  @After
  public final void after() throws Exception {
    mExecutorService.shutdown();
  }

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster = mLocalTachyonClusterResource.get();
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
          new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSizeBytes((i + 10) / 10 * 64)
              .build()).close();
    }
    mLocalTachyonCluster.stopTFS();

    String journalFolder =
        FileSystemMaster.getJournalDirectory(mLocalTachyonCluster.getMaster().getJournalFolder());
    Journal journal = new ReadWriteJournal(journalFolder);
    String completedPath = journal.getCompletedDirectory();
    Assert.assertTrue(
        UnderFileSystem.get(completedPath, mMasterTachyonConf).list(completedPath).length > 1);
    multiEditLogTestUtil();
    Assert.assertTrue(
        UnderFileSystem.get(completedPath, mMasterTachyonConf).list(completedPath).length <= 1);
    multiEditLogTestUtil();
  }

  /**
   * Test file and directory creation and deletion;
   *
   * @throws Exception
   */
  @Test
  public void deleteTest() throws Exception {
    MkdirOptions recMkdir = new MkdirOptions.Builder(new TachyonConf()).setRecursive(true).build();
    DeleteOptions recDelete =
        new DeleteOptions.Builder().setRecursive(true).build();
    for (int i = 0; i < 10; i ++) {
      String dirPath = "/i" + i;
      mTfs.mkdir(new TachyonURI(dirPath), recMkdir);
      for (int j = 0; j < 10; j ++) {
        OutStreamOptions option =
            new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSizeBytes((i + j + 1) * 64)
                .build();
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

  private void deleteTestUtil()
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(5, fsMaster.getFileInfoList(rootId).size());
    for (int i = 0; i < 5; i ++) {
      for (int j = 0; j < 5; j ++) {
        Assert.assertTrue(
            fsMaster.getFileId(new TachyonURI("/i" + i + "/j" + j)) != IdUtils.INVALID_FILE_ID);
      }
    }
    fsMaster.stop();
  }

  @Test
  public void emptyImageTest() throws Exception {
    mLocalTachyonCluster.stopTFS();
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(0, fsMaster.getFileInfoList(rootId).size());
    fsMaster.stop();
  }

  /**
   * Test file and directory creation.
   *
   * @throws Exception
   */
  @Test
  public void fileDirectoryTest() throws Exception {
    for (int i = 0; i < 10; i ++) {
      mTfs.mkdir(new TachyonURI("/i" + i));
      for (int j = 0; j < 10; j ++) {
        OutStreamOptions option =
            new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSizeBytes((i + j + 1) * 64)
                .build();
        mTfs.getOutStream(new TachyonURI("/i" + i + "/j" + j), option).close();
      }
    }
    mLocalTachyonCluster.stopTFS();
    fileDirectoryTestUtil();
    deleteFsMasterJournalLogs();
    fileDirectoryTestUtil();
  }

  private void fileDirectoryTestUtil()
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(10, fsMaster.getFileInfoList(rootId).size());
    for (int i = 0; i < 10; i ++) {
      for (int j = 0; j < 10; j ++) {
        Assert.assertTrue(
            fsMaster.getFileId(new TachyonURI("/i" + i + "/j" + j)) != IdUtils.INVALID_FILE_ID);
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
        new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSizeBytes(64).build();
    TachyonURI filePath = new TachyonURI("/xyz");
    mTfs.getOutStream(filePath, option).close();
    FileInfo fInfo = mTfs.getInfo(mTfs.open(filePath));
    mLocalTachyonCluster.stopTFS();
    fileTestUtil(fInfo);
    deleteFsMasterJournalLogs();
    fileTestUtil(fInfo);
  }

  private void fileTestUtil(FileInfo fileInfo)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.getFileInfoList(rootId).size());
    long fileId = fsMaster.getFileId(new TachyonURI("/xyz"));
    Assert.assertTrue(fileId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(fileInfo, fsMaster.getFileInfo(fileId));
    fsMaster.stop();
  }

  /**
   * Test journalling of inodes being pinned.
   */
  @Test
  public void pinTest() throws Exception {
    SetStateOptions setPinned =
        new SetStateOptions.Builder().setPinned(true).build();
    SetStateOptions setUnpinned =
        new SetStateOptions.Builder().setPinned(false).build();
    mTfs.mkdir(new TachyonURI("/myFolder"));
    TachyonFile directory = mTfs.open(new TachyonURI("/myFolder"));
    mTfs.setState(directory, setPinned);

    TachyonURI file0Path = new TachyonURI("/myFolder/file0");
    OutStreamOptions op =
        new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSizeBytes(64).build();
    mTfs.getOutStream(file0Path, op).close();
    TachyonFile file0 = mTfs.open(file0Path);
    mTfs.setState(file0, setUnpinned);

    TachyonURI file1Path = new TachyonURI("/myFolder/file1");
    mTfs.getOutStream(file1Path, op).close();

    FileInfo directoryInfo = mTfs.getInfo(directory);
    FileInfo file0Info = mTfs.getInfo(file0);
    FileInfo file1Info = mTfs.getInfo(mTfs.open(file1Path));

    mLocalTachyonCluster.stopTFS();

    pinTestUtil(directoryInfo, file0Info, file1Info);
    deleteFsMasterJournalLogs();
    pinTestUtil(directoryInfo, file0Info, file1Info);
  }

  private void pinTestUtil(FileInfo directory, FileInfo file0, FileInfo file1)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    FileInfo info = fsMaster.getFileInfo(fsMaster.getFileId(new TachyonURI("/myFolder")));
    Assert.assertEquals(directory, info);
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
   * Test directory creation.
   *
   * @throws Exception
   */
  @Test
  public void directoryTest() throws Exception {
    TachyonURI directoryPath = new TachyonURI("/xyz");
    mTfs.mkdir(directoryPath);
    FileInfo fInfo = mTfs.getInfo(mTfs.open(directoryPath));
    mLocalTachyonCluster.stopTFS();
    directoryTestUtil(fInfo);
    deleteFsMasterJournalLogs();
    directoryTestUtil(fInfo);
  }

  private void directoryTestUtil(FileInfo fileInfo)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.getFileInfoList(rootId).size());
    long fileId = fsMaster.getFileId(new TachyonURI("/xyz"));
    Assert.assertTrue(fileId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(fileInfo, fsMaster.getFileInfo(fileId));
    fsMaster.stop();
  }

  @Test
  public void persistDirectoryLaterTest() throws Exception {
    String[] directories = new String[] {
        "/d11", "/d11/d21", "/d11/d22",
        "/d12", "/d12/d21", "/d12/d22",
    };

    MkdirOptions.Builder builder = new MkdirOptions.Builder(ClientContext.getConf())
        .setRecursive(true).setUnderStorageType(UnderStorageType.NO_PERSIST);
    for (String directory : directories) {
      mTfs.mkdir(new TachyonURI(directory), builder.build());
    }

    builder.setUnderStorageType(UnderStorageType.SYNC_PERSIST);
    for (String directory : directories) {
      mTfs.mkdir(new TachyonURI(directory), builder.build());
    }

    Map<String, FileInfo> dInfos = Maps.newHashMap();
    for (String directory : directories) {
      dInfos.put(directory, mTfs.getInfo(mTfs.open(new TachyonURI(directory))));
    }
    mLocalTachyonCluster.stopTFS();
    persistDirectoryLaterTestUtil(dInfos);
    deleteFsMasterJournalLogs();
    persistDirectoryLaterTestUtil(dInfos);
  }

  private void persistDirectoryLaterTestUtil(Map<String, FileInfo> dInfos) throws Exception {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    for (Map.Entry<String, FileInfo> dInfo : dInfos.entrySet()) {
      Assert.assertEquals(dInfo.getValue(), fsMaster.getFileInfo(fsMaster.getFileId(
          new TachyonURI(dInfo.getKey()))));
    }
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
          new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSizeBytes((i + 1) * 64).build();
      mTfs.getOutStream(new TachyonURI("/a" + i), option).close();
    }
    mLocalTachyonCluster.stopTFS();
    manyFileTestUtil();
    deleteFsMasterJournalLogs();
    manyFileTestUtil();
  }

  private void manyFileTestUtil()
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(10, fsMaster.getFileInfoList(rootId).size());
    for (int k = 0; k < 10; k ++) {
      Assert.assertTrue(fsMaster.getFileId(new TachyonURI("/a" + k)) != IdUtils.INVALID_FILE_ID);
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
          new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSizeBytes((i + 10) / 10 * 64)
              .build();
      mTfs.getOutStream(new TachyonURI("/a" + i), op);
    }
    mLocalTachyonCluster.stopTFS();
    multiEditLogTestUtil();
    deleteFsMasterJournalLogs();
    multiEditLogTestUtil();
  }

  private void multiEditLogTestUtil()
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(124, fsMaster.getFileInfoList(rootId).size());
    for (int k = 0; k < 124; k ++) {
      Assert.assertTrue(fsMaster.getFileId(new TachyonURI("/a" + k)) != IdUtils.INVALID_FILE_ID);
    }
    fsMaster.stop();
  }

  // TODO(cc) The edit log behavior this test was testing no longer exists, do we need to add it
  // back?
  /// **
  // * Test renaming completed edit logs.
  // *
  // * @throws Exception
  // */
  // @Test
  // public void RenameEditLogTest() throws Exception {
  // String journalPrefix = "/tmp/JournalDir" + String.valueOf(System.currentTimeMillis());
  // Journal journal = new Journal(journalPrefix, mMasterTachyonConf);
  // UnderFileSystem ufs = UnderFileSystem.get(journalPrefix, mMasterTachyonConf);
  // ufs.delete(journalPrefix, true);
  // ufs.mkdir(journalPrefix, true);
  // OutputStream ops = ufs.create(journal.getCurrentLogFilePath());
  // if (ops != null) {
  // ops.close();
  // }
  // if (ufs != null) {
  // ufs.close();
  // }

  // // Write operation and flush them to completed directory.
  // JournalWriter journalWriter = journal.getNewWriter();
  // journalWriter.setMaxLogSize(100);
  // JournalOutputStream entryOs = journalWriter.getEntryOutputStream();
  // for (int i = 0; i < 124; i ++) {
  // entryOs.writeEntry(new InodeFileEntry(System.currentTimeMillis(), i, "/sth" + i, 0L, false,
  // System.currentTimeMillis(), Constants.DEFAULT_BLOCK_SIZE_BYTE, 10, false, false,
  // "/sth" + i, Lists.newArrayList(1L)));
  // entryOs.flush();
  // }
  // entryOs.close();

  // // Rename completed edit logs when loading them.
  // String completedDir = journal.getCompletedDirectory();
  // ufs = UnderFileSystem.get(completedDir, mMasterTachyonConf);
  // int numOfCompleteFiles = ufs.list(completedDir).length;
  // Assert.assertTrue(numOfCompleteFiles > 0);
  // EditLog.setBackUpLogStartNum(numOfCompleteFiles / 2);
  // log = new EditLog(journalPath, false, 0, mMasterTachyonConf);
  // int numOfCompleteFilesLeft = numOfCompleteFiles - numOfCompleteFiles / 2 + 1;
  // Assert.assertEquals(numOfCompleteFilesLeft, ufs.list(completedStr).length);
  // for (int i = 0; i < numOfCompleteFilesLeft; i ++) {
  // Assert.assertTrue(ufs.exists(completedStr + i + ".editLog"));
  // }
  // EditLog.setBackUpLogStartNum(-1);
  // log.close();
  // ufs.delete(journalPrefix, true);
  // if (ufs != null) {
  // ufs.close();
  // }
  // }

  /**
   * Test file and directory creation, and rename;
   *
   * @throws Exception
   */
  @Test
  public void renameTest() throws Exception {
    for (int i = 0; i < 10; i ++) {
      mTfs.mkdir(new TachyonURI("/i" + i));
      for (int j = 0; j < 10; j ++) {
        OutStreamOptions option =
            new OutStreamOptions.Builder(mMasterTachyonConf).setBlockSizeBytes((i + j + 1) * 64)
                .build();
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
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(10, fsMaster.getFileInfoList(rootId).size());
    for (int i = 0; i < 10; i ++) {
      for (int j = 0; j < 10; j ++) {
        Assert.assertTrue(
            fsMaster.getFileId(new TachyonURI("/ii" + i + "/jj" + j)) != IdUtils.INVALID_FILE_ID);
      }
    }
    fsMaster.stop();
  }

  private List<FileInfo> lsr(FileSystemMaster fsMaster, long fileId)
      throws FileDoesNotExistException {
    List<FileInfo> files = fsMaster.getFileInfoList(fileId);
    List<FileInfo> ret = Lists.newArrayList(files);
    for (FileInfo file : files) {
      ret.addAll(lsr(fsMaster, file.getFileId()));
    }
    return ret;
  }

  private void rawTableTestUtil(FileInfo fileInfo) throws IOException, InvalidPathException,
      FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    long fileId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(fileId != -1);
    // "ls -r /" should return 11 FileInfos, one is table root "/xyz", the others are 10 columns.
    Assert.assertEquals(11, lsr(fsMaster, fileId).size());

    fileId = fsMaster.getFileId(new TachyonURI("/xyz"));
    Assert.assertTrue(fileId != -1);
    Assert.assertEquals(fileInfo, fsMaster.getFileInfo(fileId));

    fsMaster.stop();
  }
}
