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
import tachyon.client.WriteType;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.FileSystem;
import tachyon.client.file.URIStatus;
import tachyon.client.file.options.CreateDirectoryOptions;
import tachyon.client.file.options.CreateFileOptions;
import tachyon.client.file.options.DeleteOptions;
import tachyon.client.file.options.SetAttributeOptions;
import tachyon.client.table.TachyonRawTables;
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
  private FileSystem mTfs = null;
  private TachyonRawTables mTachyonRawTables = null;
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
    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(64);
    FileOutStream os = mTfs.createFile(uri, options);
    for (int k = 0; k < 1000; k ++) {
      os.write(k);
    }
    os.close();
    URIStatus status = mTfs.getStatus(uri);
    mLocalTachyonCluster.stopTFS();
    addBlockTestUtil(status);
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

  private void addBlockTestUtil(URIStatus status)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.getFileInfoList(rootId).size());
    long xyzId = fsMaster.getFileId(new TachyonURI("/xyz"));
    Assert.assertTrue(xyzId != IdUtils.INVALID_FILE_ID);
    FileInfo fsMasterInfo = fsMaster.getFileInfo(xyzId);
    Assert.assertEquals(0, fsMaster.getFileInfo(xyzId).getInMemoryPercentage());
    Assert.assertEquals(status.getBlockIds(), fsMasterInfo.getBlockIds());
    Assert.assertEquals(status.getBlockSizeBytes(), fsMasterInfo.getBlockSizeBytes());
    Assert.assertEquals(status.getLength(), fsMasterInfo.getLength());
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
    URIStatus status = mTfs.getStatus(new TachyonURI("/xyz"));
    mLocalTachyonCluster.stopTFS();
    loadMetadataTestUtil(status);
    deleteFsMasterJournalLogs();
    loadMetadataTestUtil(status);
  }

  private void loadMetadataTestUtil(URIStatus status)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.getFileInfoList(rootId).size());
    Assert.assertTrue(fsMaster.getFileId(new TachyonURI("/xyz")) != IdUtils.INVALID_FILE_ID);
    FileInfo fsMasterInfo = fsMaster.getFileInfo(fsMaster.getFileId(new TachyonURI("/xyz")));
    Assert.assertEquals(status, new URIStatus(fsMasterInfo));
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
    mTachyonRawTables = TachyonRawTables.TachyonRawTablesFactory.get();
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
      mTfs.createFile(new TachyonURI("/a" + i),
          CreateFileOptions.defaults().setBlockSizeBytes((i + 10) / 10 * 64)).close();
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
    CreateDirectoryOptions recMkdir = CreateDirectoryOptions.defaults().setRecursive(true);
    DeleteOptions recDelete =
        new DeleteOptions.Builder().setRecursive(true).build();
    for (int i = 0; i < 10; i ++) {
      String dirPath = "/i" + i;
      mTfs.createDirectory(new TachyonURI(dirPath), recMkdir);
      for (int j = 0; j < 10; j ++) {
        CreateFileOptions option = CreateFileOptions.defaults().setBlockSizeBytes((i + j + 1) * 64);
        String filePath = dirPath + "/j" + j;
        mTfs.createFile(new TachyonURI(filePath), option).close();
        if (j >= 5) {
          mTfs.delete(new TachyonURI(filePath), recDelete);
        }
      }
      if (i >= 5) {
        mTfs.delete(new TachyonURI(dirPath), recDelete);
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
      mTfs.createDirectory(new TachyonURI("/i" + i));
      for (int j = 0; j < 10; j ++) {
        CreateFileOptions option = CreateFileOptions.defaults().setBlockSizeBytes((i + j + 1) * 64);
        mTfs.createFile(new TachyonURI("/i" + i + "/j" + j), option).close();
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
    CreateFileOptions option = CreateFileOptions.defaults().setBlockSizeBytes(64);
    TachyonURI filePath = new TachyonURI("/xyz");
    mTfs.createFile(filePath, option).close();
    URIStatus status = mTfs.getStatus(filePath);
    mLocalTachyonCluster.stopTFS();
    fileTestUtil(status);
    deleteFsMasterJournalLogs();
    fileTestUtil(status);
  }

  private void fileTestUtil(URIStatus status)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.getFileInfoList(rootId).size());
    long fileId = fsMaster.getFileId(new TachyonURI("/xyz"));
    Assert.assertTrue(fileId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(status, new URIStatus(fsMaster.getFileInfo(fileId)));
    fsMaster.stop();
  }

  /**
   * Test journalling of inodes being pinned.
   */
  @Test
  public void pinTest() throws Exception {
    SetAttributeOptions setPinned = SetAttributeOptions.defaults().setPinned(true);
    SetAttributeOptions setUnpinned = SetAttributeOptions.defaults().setPinned(false);
    TachyonURI dirUri = new TachyonURI("/myFolder");
    mTfs.createDirectory(dirUri);
    mTfs.setAttribute(dirUri, setPinned);

    TachyonURI file0Path = new TachyonURI("/myFolder/file0");
    CreateFileOptions op = CreateFileOptions.defaults().setBlockSizeBytes(64);
    mTfs.createFile(file0Path, op).close();
    mTfs.setAttribute(file0Path, setUnpinned);

    TachyonURI file1Path = new TachyonURI("/myFolder/file1");
    mTfs.createFile(file1Path, op).close();

    URIStatus directoryStatus = mTfs.getStatus(dirUri);
    URIStatus file0Status = mTfs.getStatus(file0Path);
    URIStatus file1Status = mTfs.getStatus(file1Path);

    mLocalTachyonCluster.stopTFS();

    pinTestUtil(directoryStatus, file0Status, file1Status);
    deleteFsMasterJournalLogs();
    pinTestUtil(directoryStatus, file0Status, file1Status);
  }

  private void pinTestUtil(URIStatus directory, URIStatus file0, URIStatus file1)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    FileInfo info = fsMaster.getFileInfo(fsMaster.getFileId(new TachyonURI("/myFolder")));
    Assert.assertEquals(directory, new URIStatus(info));
    Assert.assertTrue(info.isPinned);

    info = fsMaster.getFileInfo(fsMaster.getFileId(new TachyonURI("/myFolder/file0")));
    Assert.assertEquals(file0, new URIStatus(info));
    Assert.assertFalse(info.isPinned);

    info = fsMaster.getFileInfo(fsMaster.getFileId(new TachyonURI("/myFolder/file1")));
    Assert.assertEquals(file1, new URIStatus(info));
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
    mTfs.createDirectory(directoryPath);
    URIStatus status = mTfs.getStatus(directoryPath);
    mLocalTachyonCluster.stopTFS();
    directoryTestUtil(status);
    deleteFsMasterJournalLogs();
    directoryTestUtil(status);
  }

  private void directoryTestUtil(URIStatus status)
      throws IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.getFileInfoList(rootId).size());
    long fileId = fsMaster.getFileId(new TachyonURI("/xyz"));
    Assert.assertTrue(fileId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(status, new URIStatus(fsMaster.getFileInfo(fileId)));
    fsMaster.stop();
  }

  @Test
  public void persistDirectoryLaterTest() throws Exception {
    String[] directories = new String[] {
        "/d11", "/d11/d21", "/d11/d22",
        "/d12", "/d12/d21", "/d12/d22",
    };

    CreateDirectoryOptions options =
        CreateDirectoryOptions.defaults().setRecursive(true).setWriteType(WriteType.MUST_CACHE);
    for (String directory : directories) {
      mTfs.createDirectory(new TachyonURI(directory), options);
    }

    options.setWriteType(WriteType.CACHE_THROUGH);
    for (String directory : directories) {
      mTfs.createDirectory(new TachyonURI(directory), options);
    }

    Map<String, URIStatus> directoryStatuses = Maps.newHashMap();
    for (String directory : directories) {
      directoryStatuses.put(directory, mTfs.getStatus(new TachyonURI(directory)));
    }
    mLocalTachyonCluster.stopTFS();
    persistDirectoryLaterTestUtil(directoryStatuses);
    deleteFsMasterJournalLogs();
    persistDirectoryLaterTestUtil(directoryStatuses);
  }

  private void persistDirectoryLaterTestUtil(Map<String, URIStatus> directoryStatuses)
      throws Exception {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    for (Map.Entry<String, URIStatus> directoryStatus : directoryStatuses.entrySet()) {
      Assert.assertEquals(directoryStatus.getValue(), fsMaster.getFileInfo(fsMaster.getFileId(
          new TachyonURI(directoryStatus.getKey()))));
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
      CreateFileOptions option = CreateFileOptions.defaults().setBlockSizeBytes((i + 1) * 64);
      mTfs.createFile(new TachyonURI("/a" + i), option).close();
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
      CreateFileOptions op = CreateFileOptions.defaults().setBlockSizeBytes((i + 10) / 10 * 64);
      mTfs.createFile(new TachyonURI("/a" + i), op);
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
      mTfs.createDirectory(new TachyonURI("/i" + i));
      for (int j = 0; j < 10; j ++) {
        CreateFileOptions option = CreateFileOptions.defaults().setBlockSizeBytes((i + j + 1) * 64);
        TachyonURI path = new TachyonURI("/i" + i + "/j" + j);
        mTfs.createFile(path, option).close();
        mTfs.rename(path, new TachyonURI("/i" + i + "/jj" + j));
      }
      mTfs.rename(new TachyonURI("/i" + i), new TachyonURI("/ii" + i));
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

  /**
   * Test raw table creation.
   *
   * @throws Exception
   */
  @Test
  public void rawTableTest() throws Exception {
    mTachyonRawTables.create(new TachyonURI("/xyz"), 10);
    URIStatus status = mTfs.getStatus(new TachyonURI("/xyz"));
    mLocalTachyonCluster.stopTFS();
    rawTableTestUtil(status);
    deleteFsMasterJournalLogs();
    rawTableTestUtil(status);
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

  private void rawTableTestUtil(URIStatus status) throws IOException, InvalidPathException,
      FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    long fileId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(fileId != -1);
    // "ls -r /" should return 11 FileInfos, one is table root "/xyz", the others are 10 columns.
    Assert.assertEquals(11, lsr(fsMaster, fileId).size());

    fileId = fsMaster.getFileId(new TachyonURI("/xyz"));
    Assert.assertTrue(fileId != -1);
    Assert.assertEquals(status, new URIStatus(fsMaster.getFileInfo(fileId)));

    fsMaster.stop();
  }
}
