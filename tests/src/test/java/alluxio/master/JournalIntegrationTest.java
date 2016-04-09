/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.client.ClientContext;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.journal.Journal;
import alluxio.master.journal.ReadWriteJournal;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.group.GroupMappingService;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test master journal, including checkpoint and entry log. Most tests will test entry log first,
 * followed by the checkpoint.
 */
public class JournalIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource(Constants.GB, Constants.GB,
          Constants.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, Integer.toString(Constants.KB));

  private LocalAlluxioCluster mLocalAlluxioCluster = null;
  private FileSystem mFileSystem = null;
  private AlluxioURI mRootUri = new AlluxioURI(AlluxioURI.SEPARATOR);
  private Configuration mMasterConfiguration = null;

  /**
   * Tests adding a block.
   */
  @Test
  public void addBlockTest() throws Exception {
    AlluxioURI uri = new AlluxioURI("/xyz");
    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(64);
    FileOutStream os = mFileSystem.createFile(uri, options);
    for (int k = 0; k < 1000; k++) {
      os.write(k);
    }
    os.close();
    URIStatus status = mFileSystem.getStatus(uri);
    mLocalAlluxioCluster.stopFS();
    addBlockTestUtil(status);
  }

  private FileSystemMaster createFsMasterFromJournal() throws IOException {
    return MasterTestUtils.createFileSystemMasterFromJournal(mMasterConfiguration);
  }

  private void deleteFsMasterJournalLogs() throws IOException {
    String journalFolder = mLocalAlluxioCluster.getMaster().getJournalFolder();
    Journal journal = new ReadWriteJournal(
        PathUtils.concatPath(journalFolder, Constants.FILE_SYSTEM_MASTER_NAME));
    UnderFileSystem.get(journalFolder, mMasterConfiguration).delete(journal.getCurrentLogFilePath(),
        true);
  }

  private void addBlockTestUtil(URIStatus status)
      throws AccessControlException, IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.getFileInfoList(mRootUri).size());
    long xyzId = fsMaster.getFileId(new AlluxioURI("/xyz"));
    Assert.assertTrue(xyzId != IdUtils.INVALID_FILE_ID);
    FileInfo fsMasterInfo = fsMaster.getFileInfo(xyzId);
    Assert.assertEquals(0, fsMaster.getFileInfo(xyzId).getInMemoryPercentage());
    Assert.assertEquals(status.getBlockIds(), fsMasterInfo.getBlockIds());
    Assert.assertEquals(status.getBlockSizeBytes(), fsMasterInfo.getBlockSizeBytes());
    Assert.assertEquals(status.getLength(), fsMasterInfo.getLength());
    fsMaster.stop();
  }

  /**
   * Tests loading metadata.
   */
  @Test
  public void loadMetadataTest() throws Exception {
    String ufsRoot = PathUtils
        .concatPath(mLocalAlluxioCluster.getMasterConf().get(Constants.UNDERFS_ADDRESS));
    UnderFileSystem ufs = UnderFileSystem.get(ufsRoot, mLocalAlluxioCluster.getMasterConf());
    ufs.create(ufsRoot + "/xyz");
    mFileSystem.loadMetadata(new AlluxioURI("/xyz"));
    URIStatus status = mFileSystem.getStatus(new AlluxioURI("/xyz"));
    mLocalAlluxioCluster.stopFS();
    loadMetadataTestUtil(status);
    deleteFsMasterJournalLogs();
    loadMetadataTestUtil(status);
  }

  private void loadMetadataTestUtil(URIStatus status)
      throws AccessControlException, IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.getFileInfoList(mRootUri).size());
    Assert.assertTrue(fsMaster.getFileId(new AlluxioURI("/xyz")) != IdUtils.INVALID_FILE_ID);
    FileInfo fsMasterInfo = fsMaster.getFileInfo(fsMaster.getFileId(new AlluxioURI("/xyz")));
    Assert.assertEquals(status, new URIStatus(fsMasterInfo));
    fsMaster.stop();
  }

  @Before
  public final void before() throws Exception {
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
    mFileSystem = mLocalAlluxioCluster.getClient();
    mMasterConfiguration = mLocalAlluxioCluster.getMasterConf();
  }

  /**
   * Tests completed edit log deletion.
   */
  @Test
  public void completedEditLogDeletionTest() throws Exception {
    for (int i = 0; i < 124; i++) {
      mFileSystem.createFile(new AlluxioURI("/a" + i),
          CreateFileOptions.defaults().setBlockSizeBytes((i + 10) / 10 * 64)).close();
    }
    mLocalAlluxioCluster.stopFS();

    String journalFolder =
        FileSystemMaster.getJournalDirectory(mLocalAlluxioCluster.getMaster().getJournalFolder());
    Journal journal = new ReadWriteJournal(journalFolder);
    String completedPath = journal.getCompletedDirectory();
    Assert.assertTrue(
        UnderFileSystem.get(completedPath, mMasterConfiguration).list(completedPath).length > 1);
    multiEditLogTestUtil();
    Assert.assertTrue(
        UnderFileSystem.get(completedPath, mMasterConfiguration).list(completedPath).length <= 1);
    multiEditLogTestUtil();
  }

  /**
   * Tests file and directory creation and deletion.
   */
  @Test
  public void deleteTest() throws Exception {
    CreateDirectoryOptions recMkdir = CreateDirectoryOptions.defaults().setRecursive(true);
    DeleteOptions recDelete = DeleteOptions.defaults().setRecursive(true);
    for (int i = 0; i < 10; i++) {
      String dirPath = "/i" + i;
      mFileSystem.createDirectory(new AlluxioURI(dirPath), recMkdir);
      for (int j = 0; j < 10; j++) {
        CreateFileOptions option = CreateFileOptions.defaults().setBlockSizeBytes((i + j + 1) * 64);
        String filePath = dirPath + "/j" + j;
        mFileSystem.createFile(new AlluxioURI(filePath), option).close();
        if (j >= 5) {
          mFileSystem.delete(new AlluxioURI(filePath), recDelete);
        }
      }
      if (i >= 5) {
        mFileSystem.delete(new AlluxioURI(dirPath), recDelete);
      }
    }
    mLocalAlluxioCluster.stopFS();
    deleteTestUtil();
    deleteFsMasterJournalLogs();
    deleteTestUtil();
  }

  private void deleteTestUtil() throws Exception {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(5, fsMaster.getFileInfoList(mRootUri).size());
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        Assert.assertTrue(
            fsMaster.getFileId(new AlluxioURI("/i" + i + "/j" + j)) != IdUtils.INVALID_FILE_ID);
      }
    }
    fsMaster.stop();
  }

  @Test
  public void emptyImageTest() throws Exception {
    mLocalAlluxioCluster.stopFS();
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(0, fsMaster.getFileInfoList(mRootUri).size());
    fsMaster.stop();
  }

  /**
   * Tests file and directory creation.
   */
  @Test
  public void fileDirectoryTest() throws Exception {
    for (int i = 0; i < 10; i++) {
      mFileSystem.createDirectory(new AlluxioURI("/i" + i));
      for (int j = 0; j < 10; j++) {
        CreateFileOptions option = CreateFileOptions.defaults().setBlockSizeBytes((i + j + 1) * 64);
        mFileSystem.createFile(new AlluxioURI("/i" + i + "/j" + j), option).close();
      }
    }
    mLocalAlluxioCluster.stopFS();
    fileDirectoryTestUtil();
    deleteFsMasterJournalLogs();
    fileDirectoryTestUtil();
  }

  private void fileDirectoryTestUtil() throws Exception {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(10, fsMaster.getFileInfoList(mRootUri).size());
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        Assert.assertTrue(
            fsMaster.getFileId(new AlluxioURI("/i" + i + "/j" + j)) != IdUtils.INVALID_FILE_ID);
      }
    }
    fsMaster.stop();
  }

  /**
   * Tests file creation.
   */
  @Test
  public void fileTest() throws Exception {
    CreateFileOptions option = CreateFileOptions.defaults().setBlockSizeBytes(64);
    AlluxioURI filePath = new AlluxioURI("/xyz");
    mFileSystem.createFile(filePath, option).close();
    URIStatus status = mFileSystem.getStatus(filePath);
    mLocalAlluxioCluster.stopFS();
    fileTestUtil(status);
    deleteFsMasterJournalLogs();
    fileTestUtil(status);
  }

  private void fileTestUtil(URIStatus status)
      throws AccessControlException, IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.getFileInfoList(mRootUri).size());
    long fileId = fsMaster.getFileId(new AlluxioURI("/xyz"));
    Assert.assertTrue(fileId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(status, new URIStatus(fsMaster.getFileInfo(fileId)));
    fsMaster.stop();
  }

  /**
   * Tests journalling of inodes being pinned.
   */
  @Test
  public void pinTest() throws Exception {
    SetAttributeOptions setPinned = SetAttributeOptions.defaults().setPinned(true);
    SetAttributeOptions setUnpinned = SetAttributeOptions.defaults().setPinned(false);
    AlluxioURI dirUri = new AlluxioURI("/myFolder");
    mFileSystem.createDirectory(dirUri);
    mFileSystem.setAttribute(dirUri, setPinned);

    AlluxioURI file0Path = new AlluxioURI("/myFolder/file0");
    CreateFileOptions op = CreateFileOptions.defaults().setBlockSizeBytes(64);
    mFileSystem.createFile(file0Path, op).close();
    mFileSystem.setAttribute(file0Path, setUnpinned);

    AlluxioURI file1Path = new AlluxioURI("/myFolder/file1");
    mFileSystem.createFile(file1Path, op).close();

    URIStatus directoryStatus = mFileSystem.getStatus(dirUri);
    URIStatus file0Status = mFileSystem.getStatus(file0Path);
    URIStatus file1Status = mFileSystem.getStatus(file1Path);

    mLocalAlluxioCluster.stopFS();

    pinTestUtil(directoryStatus, file0Status, file1Status);
    deleteFsMasterJournalLogs();
    pinTestUtil(directoryStatus, file0Status, file1Status);
  }

  private void pinTestUtil(URIStatus directory, URIStatus file0, URIStatus file1)
      throws AccessControlException, IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    FileInfo info = fsMaster.getFileInfo(fsMaster.getFileId(new AlluxioURI("/myFolder")));
    Assert.assertEquals(directory, new URIStatus(info));
    Assert.assertTrue(info.isPinned());

    info = fsMaster.getFileInfo(fsMaster.getFileId(new AlluxioURI("/myFolder/file0")));
    Assert.assertEquals(file0, new URIStatus(info));
    Assert.assertFalse(info.isPinned());

    info = fsMaster.getFileInfo(fsMaster.getFileId(new AlluxioURI("/myFolder/file1")));
    Assert.assertEquals(file1, new URIStatus(info));
    Assert.assertTrue(info.isPinned());

    fsMaster.stop();
  }

  /**
   * Tests directory creation.
   */
  @Test
  public void directoryTest() throws Exception {
    AlluxioURI directoryPath = new AlluxioURI("/xyz");
    mFileSystem.createDirectory(directoryPath);
    URIStatus status = mFileSystem.getStatus(directoryPath);
    mLocalAlluxioCluster.stopFS();
    directoryTestUtil(status);
    deleteFsMasterJournalLogs();
    directoryTestUtil(status);
  }

  private void directoryTestUtil(URIStatus status)
      throws AccessControlException, IOException, InvalidPathException, FileDoesNotExistException {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.getFileInfoList(mRootUri).size());
    long fileId = fsMaster.getFileId(new AlluxioURI("/xyz"));
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
      mFileSystem.createDirectory(new AlluxioURI(directory), options);
    }

    options.setWriteType(WriteType.CACHE_THROUGH);
    for (String directory : directories) {
      mFileSystem.createDirectory(new AlluxioURI(directory), options);
    }

    Map<String, URIStatus> directoryStatuses = Maps.newHashMap();
    for (String directory : directories) {
      directoryStatuses.put(directory, mFileSystem.getStatus(new AlluxioURI(directory)));
    }
    mLocalAlluxioCluster.stopFS();
    persistDirectoryLaterTestUtil(directoryStatuses);
    deleteFsMasterJournalLogs();
    persistDirectoryLaterTestUtil(directoryStatuses);
  }

  private void persistDirectoryLaterTestUtil(Map<String, URIStatus> directoryStatuses)
      throws Exception {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    for (Map.Entry<String, URIStatus> directoryStatus : directoryStatuses.entrySet()) {
      Assert.assertEquals(
          directoryStatus.getValue(),
          new URIStatus(fsMaster.getFileInfo(fsMaster.getFileId(new AlluxioURI(directoryStatus
              .getKey())))));
    }
  }

  /**
   * Tests files creation.
   */
  @Test
  public void manyFileTest() throws Exception {
    for (int i = 0; i < 10; i++) {
      CreateFileOptions option = CreateFileOptions.defaults().setBlockSizeBytes((i + 1) * 64);
      mFileSystem.createFile(new AlluxioURI("/a" + i), option).close();
    }
    mLocalAlluxioCluster.stopFS();
    manyFileTestUtil();
    deleteFsMasterJournalLogs();
    manyFileTestUtil();
  }

  private void manyFileTestUtil() throws Exception {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(10, fsMaster.getFileInfoList(mRootUri).size());
    for (int k = 0; k < 10; k++) {
      Assert.assertTrue(fsMaster.getFileId(new AlluxioURI("/a" + k)) != IdUtils.INVALID_FILE_ID);
    }
    fsMaster.stop();
  }

  /**
   * Tests reading multiple edit logs.
   */
  @Test
  public void multiEditLogTest() throws Exception {
    for (int i = 0; i < 124; i++) {
      CreateFileOptions op = CreateFileOptions.defaults().setBlockSizeBytes((i + 10) / 10 * 64);
      mFileSystem.createFile(new AlluxioURI("/a" + i), op);
    }
    mLocalAlluxioCluster.stopFS();
    multiEditLogTestUtil();
    deleteFsMasterJournalLogs();
    multiEditLogTestUtil();
  }

  private void multiEditLogTestUtil() throws Exception {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(124, fsMaster.getFileInfoList(mRootUri).size());
    for (int k = 0; k < 124; k++) {
      Assert.assertTrue(fsMaster.getFileId(new AlluxioURI("/a" + k)) != IdUtils.INVALID_FILE_ID);
    }
    fsMaster.stop();
  }

  /**
   * Tests file and directory creation, and rename.
   */
  @Test
  public void renameTest() throws Exception {
    for (int i = 0; i < 10; i++) {
      mFileSystem.createDirectory(new AlluxioURI("/i" + i));
      for (int j = 0; j < 10; j++) {
        CreateFileOptions option = CreateFileOptions.defaults().setBlockSizeBytes((i + j + 1) * 64);
        AlluxioURI path = new AlluxioURI("/i" + i + "/j" + j);
        mFileSystem.createFile(path, option).close();
        mFileSystem.rename(path, new AlluxioURI("/i" + i + "/jj" + j));
      }
      mFileSystem.rename(new AlluxioURI("/i" + i), new AlluxioURI("/ii" + i));
    }
    mLocalAlluxioCluster.stopFS();
    renameTestUtil();
    deleteFsMasterJournalLogs();
    renameTestUtil();
  }

  private void renameTestUtil() throws Exception {
    FileSystemMaster fsMaster = createFsMasterFromJournal();
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(10, fsMaster.getFileInfoList(mRootUri).size());
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        Assert.assertTrue(
            fsMaster.getFileId(new AlluxioURI("/ii" + i + "/jj" + j)) != IdUtils.INVALID_FILE_ID);
      }
    }
    fsMaster.stop();
  }

  private List<FileInfo> lsr(FileSystemMaster fsMaster, AlluxioURI uri)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException {
    List<FileInfo> files = fsMaster.getFileInfoList(uri);
    List<FileInfo> ret = Lists.newArrayList(files);
    for (FileInfo file : files) {
      ret.addAll(lsr(fsMaster, new AlluxioURI(file.getPath())));
    }
    return ret;
  }

  private void rawTableTestUtil(FileInfo fileInfo) throws Exception {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    long fileId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(fileId != -1);
    // "ls -r /" should return 11 FileInfos, one is table root "/xyz", the others are 10 columns.
    Assert.assertEquals(11, lsr(fsMaster, mRootUri).size());

    fileId = fsMaster.getFileId(new AlluxioURI("/xyz"));
    Assert.assertTrue(fileId != -1);
    Assert.assertEquals(fileInfo, fsMaster.getFileInfo(fileId));

    fsMaster.stop();
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      Constants.SECURITY_AUTHENTICATION_TYPE, "SIMPLE",
      Constants.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
      Constants.SECURITY_GROUP_MAPPING, FakeUserGroupsMapping.FULL_CLASS_NAME})
  public void setAclTest() throws Exception {
    AlluxioURI filePath = new AlluxioURI("/file");

    ClientContext.getConf().set(Constants.SECURITY_LOGIN_USERNAME, "alluxio");
    CreateFileOptions op =
        CreateFileOptions.defaults().setBlockSizeBytes(64);
    mFileSystem.createFile(filePath, op).close();

    mFileSystem.setAttribute(filePath,
        SetAttributeOptions.defaults().setOwner("user1").setRecursive(false));
    mFileSystem.setAttribute(filePath,
        SetAttributeOptions.defaults().setGroup("group1").setRecursive(false));
    mFileSystem.setAttribute(filePath,
        SetAttributeOptions.defaults().setPermission((short) 0400).setRecursive(false));

    URIStatus status = mFileSystem.getStatus(filePath);

    mLocalAlluxioCluster.stopFS();

    aclTestUtil(status);
    deleteFsMasterJournalLogs();
    aclTestUtil(status);
  }

  private void aclTestUtil(URIStatus status) throws Exception {
    FileSystemMaster fsMaster = createFsMasterFromJournal();

    AuthenticatedClientUser.set("user1");
    FileInfo info = fsMaster.getFileInfo(new AlluxioURI("/file"));
    Assert.assertEquals(status, new URIStatus(info));

    fsMaster.stop();
  }

  public static class FakeUserGroupsMapping implements GroupMappingService {
    // The fullly qualified class name of this group mapping service. This is needed to configure
    // the alluxio cluster
    public static final String FULL_CLASS_NAME =
        "alluxio.master.JournalIntegrationTest$FakeUserGroupsMapping";

    private HashMap<String, String> mUserGroups = new HashMap<String, String>();

    public FakeUserGroupsMapping() {
      mUserGroups.put("alluxio", "supergroup");
      mUserGroups.put("user1", "group1");
      mUserGroups.put("others", "anygroup");
    }

    @Override
    public List<String> getGroups(String user) throws IOException {
      if (mUserGroups.containsKey(user)) {
        return Lists.newArrayList(mUserGroups.get(user).split(","));
      }
      return Lists.newArrayList(mUserGroups.get("others").split(","));
    }

    @Override
    public void setConf(Configuration conf) throws IOException {
      // no-op
    }
  }
}
