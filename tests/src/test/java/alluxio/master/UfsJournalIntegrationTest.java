/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.UnderFileSystemSpy;
import alluxio.client.WriteType;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.DeleteOptions;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.journal.JournalWriter;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsMutableJournal;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.security.group.GroupMappingService;
import alluxio.underfs.UnderFileStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;
import alluxio.wire.LoadMetadataType;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test master journal, including checkpoint and entry log. Most tests will test entry log first,
 * followed by the checkpoint.
 */
public class UfsJournalIntegrationTest {
  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
        .setProperty(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, Integer.toString(Constants.KB))
        .setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false")
        .build();

  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  private LocalAlluxioCluster mLocalAlluxioCluster;
  private FileSystem mFileSystem;
  private AlluxioURI mRootUri = new AlluxioURI(AlluxioURI.SEPARATOR);

  @Before
  public final void before() throws Exception {
    mLocalAlluxioCluster = mLocalAlluxioClusterResource.get();
    mFileSystem = mLocalAlluxioCluster.getClient();
  }

  /**
   * Tests adding a block.
   */
  @Test
  public void addBlock() throws Exception {
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

  private void addBlockTestUtil(URIStatus status) throws Exception {
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);

    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.listStatus(mRootUri,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)).size());
    long xyzId = fsMaster.getFileId(new AlluxioURI("/xyz"));
    Assert.assertTrue(xyzId != IdUtils.INVALID_FILE_ID);
    FileInfo fsMasterInfo = fsMaster.getFileInfo(xyzId);
    Assert.assertEquals(0, fsMaster.getFileInfo(xyzId).getInMemoryPercentage());
    Assert.assertEquals(status.getBlockIds(), fsMasterInfo.getBlockIds());
    Assert.assertEquals(status.getBlockSizeBytes(), fsMasterInfo.getBlockSizeBytes());
    Assert.assertEquals(status.getLength(), fsMasterInfo.getLength());
    registry.stop();
  }

  /**
   * Tests flushing the journal multiple times, without writing any data.
   */
  @Test
  public void multipleFlush() throws Exception {
    // Set the max log size to 0 to force a flush to write a new file.
    String existingMax = Configuration.get(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX);
    Configuration.set(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "0");
    try {
      String journalFolder = mLocalAlluxioCluster.getMaster().getJournalFolder();
      UfsMutableJournal journal = new UfsMutableJournal(
          new URI(PathUtils.concatPath(journalFolder, Constants.FILE_SYSTEM_MASTER_NAME)));
      JournalWriter writer = journal.getWriter();
      writer.getCheckpointOutputStream(0).close();
      // Flush multiple times, without writing to the log.
      writer.flush();
      writer.flush();
      writer.flush();
      UnderFileStatus[] paths = UnderFileSystem.Factory.get(journalFolder)
          .listStatus(journal.getCompletedLocation().toString());
      // Make sure no new empty files were created.
      Assert.assertTrue(paths == null || paths.length == 0);
    } finally {
      // Reset the max log size.
      Configuration.set(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, existingMax);
    }
  }

  /**
   * Tests loading metadata.
   */
  @Test
  public void loadMetadata() throws Exception {
    String ufsRoot = PathUtils.concatPath(Configuration.get(PropertyKey.UNDERFS_ADDRESS));
    UnderFileSystem ufs = UnderFileSystem.Factory.get(ufsRoot);
    ufs.create(ufsRoot + "/xyz").close();
    mFileSystem.loadMetadata(new AlluxioURI("/xyz"));
    URIStatus status = mFileSystem.getStatus(new AlluxioURI("/xyz"));
    mLocalAlluxioCluster.stopFS();
    loadMetadataTestUtil(status);
    deleteFsMasterJournalLogs();
    loadMetadataTestUtil(status);
  }

  private void loadMetadataTestUtil(URIStatus status) throws Exception {
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);

    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.listStatus(mRootUri,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)).size());
    Assert.assertTrue(fsMaster.getFileId(new AlluxioURI("/xyz")) != IdUtils.INVALID_FILE_ID);
    FileInfo fsMasterInfo = fsMaster.getFileInfo(fsMaster.getFileId(new AlluxioURI("/xyz")));
    Assert.assertEquals(status, new URIStatus(fsMasterInfo));
    registry.stop();
  }

  /**
   * Tests completed edit log deletion.
   */
  @Test
  public void completedEditLogDeletion() throws Exception {
    for (int i = 0; i < 124; i++) {
      mFileSystem.createFile(new AlluxioURI("/a" + i),
          CreateFileOptions.defaults().setBlockSizeBytes((i + 10) / 10 * 64)).close();
    }
    mLocalAlluxioCluster.stopFS();

    String journalFolder = PathUtils.concatPath(mLocalAlluxioCluster.getMaster().getJournalFolder(),
        Constants.FILE_SYSTEM_MASTER_NAME);
    UfsJournal journal = new UfsMutableJournal(new URI(journalFolder));
    URI completedLocation = journal.getCompletedLocation();
    Assert.assertTrue(UnderFileSystem.Factory.get(completedLocation.toString())
        .listStatus(completedLocation.toString()).length > 1);
    multiEditLogTestUtil();
    Assert.assertTrue(UnderFileSystem.Factory.get(completedLocation.toString())
        .listStatus(completedLocation.toString()).length <= 1);
    multiEditLogTestUtil();
  }

  /**
   * Tests file and directory creation and deletion.
   */
  @Test
  public void delete() throws Exception {
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
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(5, fsMaster.listStatus(mRootUri,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)).size());
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        Assert.assertTrue(
            fsMaster.getFileId(new AlluxioURI("/i" + i + "/j" + j)) != IdUtils.INVALID_FILE_ID);
      }
    }
    registry.stop();
  }

  @Test
  public void emptyImage() throws Exception {
    Assert.assertEquals(0, mFileSystem.listStatus(mRootUri).size());
    mLocalAlluxioCluster.stopFS();
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(0, fsMaster.listStatus(mRootUri,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)).size());
    registry.stop();
  }

  /**
   * Tests file and directory creation.
   */
  @Test
  public void fileDirectory() throws Exception {
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
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(10, fsMaster.listStatus(mRootUri,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)).size());
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        Assert.assertTrue(
            fsMaster.getFileId(new AlluxioURI("/i" + i + "/j" + j)) != IdUtils.INVALID_FILE_ID);
      }
    }
    registry.stop();
  }

  /**
   * Tests file creation.
   */
  @Test
  public void file() throws Exception {
    CreateFileOptions option = CreateFileOptions.defaults().setBlockSizeBytes(64);
    AlluxioURI filePath = new AlluxioURI("/xyz");
    mFileSystem.createFile(filePath, option).close();
    URIStatus status = mFileSystem.getStatus(filePath);
    mLocalAlluxioCluster.stopFS();
    fileTestUtil(status);
    deleteFsMasterJournalLogs();
    fileTestUtil(status);
  }

  private void fileTestUtil(URIStatus status) throws Exception {
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.listStatus(mRootUri,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)).size());
    long fileId = fsMaster.getFileId(new AlluxioURI("/xyz"));
    Assert.assertTrue(fileId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(status, new URIStatus(fsMaster.getFileInfo(fileId)));
    registry.stop();
  }

  /**
   * Tests journalling of inodes being pinned.
   */
  @Test
  public void pin() throws Exception {
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

  private void pinTestUtil(URIStatus directory, URIStatus file0, URIStatus file1) throws Exception {
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);

    FileInfo info = fsMaster.getFileInfo(fsMaster.getFileId(new AlluxioURI("/myFolder")));
    Assert.assertEquals(directory, new URIStatus(info));
    Assert.assertTrue(info.isPinned());

    info = fsMaster.getFileInfo(fsMaster.getFileId(new AlluxioURI("/myFolder/file0")));
    Assert.assertEquals(file0, new URIStatus(info));
    Assert.assertFalse(info.isPinned());

    info = fsMaster.getFileInfo(fsMaster.getFileId(new AlluxioURI("/myFolder/file1")));
    Assert.assertEquals(file1, new URIStatus(info));
    Assert.assertTrue(info.isPinned());

    registry.stop();
  }

  /**
   * Tests directory creation.
   */
  @Test
  public void directory() throws Exception {
    AlluxioURI directoryPath = new AlluxioURI("/xyz");
    mFileSystem.createDirectory(directoryPath);
    URIStatus status = mFileSystem.getStatus(directoryPath);
    mLocalAlluxioCluster.stopFS();
    directoryTestUtil(status);
    deleteFsMasterJournalLogs();
    directoryTestUtil(status);
  }

  private void directoryTestUtil(URIStatus status) throws Exception {
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(1, fsMaster.listStatus(mRootUri,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)).size());
    long fileId = fsMaster.getFileId(new AlluxioURI("/xyz"));
    Assert.assertTrue(fileId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(status, new URIStatus(fsMaster.getFileInfo(fileId)));
    registry.stop();
  }

  @Test
  public void persistDirectoryLater() throws Exception {
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

    Map<String, URIStatus> directoryStatuses = new HashMap<>();
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
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
    for (Map.Entry<String, URIStatus> directoryStatus : directoryStatuses.entrySet()) {
      Assert.assertEquals(
          directoryStatus.getValue(),
          new URIStatus(fsMaster.getFileInfo(fsMaster.getFileId(new AlluxioURI(directoryStatus
              .getKey())))));
    }
    registry.stop();
  }

  /**
   * Tests files creation.
   */
  @Test
  public void manyFile() throws Exception {
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
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(10, fsMaster.listStatus(mRootUri,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)).size());
    for (int k = 0; k < 10; k++) {
      Assert.assertTrue(fsMaster.getFileId(new AlluxioURI("/a" + k)) != IdUtils.INVALID_FILE_ID);
    }
    registry.stop();
  }

  /**
   * Tests the situation where a checkpoint mount entry is replayed by a standby master.
   *
   * @throws Exception on error
   */
  @Test
  public void mountEntryCheckpoint() throws Exception {
    final AlluxioURI mountUri = new AlluxioURI("/local_mnt/");
    final AlluxioURI ufsUri = new AlluxioURI(mTestFolder.newFolder("test_ufs").getAbsolutePath());

    // Create a mount point, which will journal a mount entry.
    mFileSystem.mount(mountUri, ufsUri);
    mLocalAlluxioCluster.stopFS();

    // Start a leader master, which will create a new checkpoint, with a mount entry.
    MasterTestUtils.createLeaderFileSystemMasterFromJournal().stop();

    // Start a standby master, which will replay the mount entry from the checkpoint.
    MasterRegistry registry = MasterTestUtils.createStandbyFileSystemMasterFromJournal();
    final FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
    try {
      CommonUtils.waitFor("standby journal checkpoint replay", new Function<Void, Boolean>() {
        @Override
        public Boolean apply(Void input) {
          try {
            fsMaster.listStatus(mountUri, ListStatusOptions.defaults());
            return true;
          } catch (Exception e) {
            return false;
          }
        }
      }, WaitForOptions.defaults().setTimeout(60 * Constants.SECOND_MS));
    } finally {
      registry.stop();
    }
  }

  /**
   * Tests reading multiple edit logs.
   */
  @Test
  public void multiEditLog() throws Exception {
    for (int i = 0; i < 124; i++) {
      CreateFileOptions op = CreateFileOptions.defaults().setBlockSizeBytes((i + 10) / 10 * 64);
      mFileSystem.createFile(new AlluxioURI("/a" + i), op).close();
    }
    mLocalAlluxioCluster.stopFS();
    multiEditLogTestUtil();
    deleteFsMasterJournalLogs();
    multiEditLogTestUtil();
  }

  private void multiEditLogTestUtil() throws Exception {
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(124, fsMaster.listStatus(mRootUri,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)).size());
    for (int k = 0; k < 124; k++) {
      Assert.assertTrue(fsMaster.getFileId(new AlluxioURI("/a" + k)) != IdUtils.INVALID_FILE_ID);
    }
    registry.stop();
  }

  /**
   * Tests file and directory creation, and rename.
   */
  @Test
  public void rename() throws Exception {
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
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
    long rootId = fsMaster.getFileId(mRootUri);
    Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
    Assert.assertEquals(10, fsMaster.listStatus(mRootUri,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)).size());
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        Assert.assertTrue(
            fsMaster.getFileId(new AlluxioURI("/ii" + i + "/jj" + j)) != IdUtils.INVALID_FILE_ID);
      }
    }
    registry.stop();
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE",
      PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
      PropertyKey.Name.SECURITY_GROUP_MAPPING_CLASS, FakeUserGroupsMapping.FULL_CLASS_NAME})
  public void setAcl() throws Exception {
    AlluxioURI filePath = new AlluxioURI("/file");

    String user = "alluxio";
    Configuration.set(PropertyKey.SECURITY_LOGIN_USERNAME, user);
    CreateFileOptions op = CreateFileOptions.defaults().setBlockSizeBytes(64);
    mFileSystem.createFile(filePath, op).close();

    // TODO(chaomin): also setOwner and setGroup once there's a way to fake the owner/group in UFS.
    mFileSystem.setAttribute(filePath,
        SetAttributeOptions.defaults().setMode(new Mode((short) 0400)).setRecursive(false));

    URIStatus status = mFileSystem.getStatus(filePath);

    mLocalAlluxioCluster.stopFS();

    aclTestUtil(status, user);
    deleteFsMasterJournalLogs();
    aclTestUtil(status, user);
  }

  @Test
  public void failDuringCheckpointRename() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    mFileSystem.createFile(file).close();
    // Restart the master once so that it creates a checkpoint file.
    mLocalAlluxioCluster.stopFS();
    createFsMasterFromJournal();
    AlluxioURI journal = new AlluxioURI(Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER));
    try (UnderFileSystemSpy ufsSpy = new UnderFileSystemSpy(journal)) {
      doThrow(new RuntimeException("Failed to rename")).when(ufsSpy.get())
          .renameFile(Mockito.contains("FileSystemMaster/checkpoint.data.tmp"), anyString());
      try {
        // Restart the master again, but with renaming the checkpoint file failing.
        mLocalAlluxioCluster.stopFS();
        createFsMasterFromJournal();
        Assert.fail("Should have failed during rename");
      } catch (RuntimeException e) {
        Assert.assertEquals("Failed to rename", e.getMessage());
      }
    }
    // We shouldn't lose track of the fact that the file is loaded into memory.
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
    Assert.assertTrue(fsMaster.getInMemoryFiles().contains(file));
    registry.stop();
  }

  @Test
  public void failDuringCheckpointDelete() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    mFileSystem.createFile(file).close();
    // Restart the master once so that it creates a checkpoint file.
    mLocalAlluxioCluster.stopFS();
    createFsMasterFromJournal();
    AlluxioURI journal = new AlluxioURI(Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER));
    try (UnderFileSystemSpy ufsSpy = new UnderFileSystemSpy(journal)) {
      doThrow(new RuntimeException("Failed to delete")).when(ufsSpy.get())
          .deleteFile(Mockito.contains("FileSystemMaster/checkpoint.data"));
      try {
        // Restart the master again, but with deleting the checkpoint file failing.
        mLocalAlluxioCluster.stopFS();
        createFsMasterFromJournal();
        Assert.fail("Should have failed during delete");
      } catch (RuntimeException e) {
        Assert.assertEquals("Failed to delete", e.getMessage());
      }
    }
    // We shouldn't lose track of the fact that the file is loaded into memory.
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
    Assert.assertTrue(fsMaster.getInMemoryFiles().contains(file));
    registry.stop();
  }

  @Test
  public void failWhileDeletingCompletedLogs() throws Exception {
    AlluxioURI file = new AlluxioURI("/file");
    mFileSystem.createFile(file).close();
    AlluxioURI journal = new AlluxioURI(Configuration.get(PropertyKey.MASTER_JOURNAL_FOLDER));
    try (UnderFileSystemSpy ufsSpy = new UnderFileSystemSpy(journal)) {
      doThrow(new RuntimeException("Failed to delete completed log")).when(ufsSpy.get())
          .deleteFile(Mockito.contains("FileSystemMaster/completed"));
      try {
        // Restart the master again, but with deleting the checkpoint file failing.
        mLocalAlluxioCluster.stopFS();
        createFsMasterFromJournal();
        Assert.fail("Should have failed during delete");
      } catch (RuntimeException e) {
        Assert.assertEquals("Failed to delete completed log", e.getMessage());
      }
    }
    // We shouldn't lose track of the fact that the file is loaded into memory.
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
    Assert.assertTrue(fsMaster.getInMemoryFiles().contains(file));
    registry.stop();
  }

  private void aclTestUtil(URIStatus status, String user) throws Exception {
    MasterRegistry registry = createFsMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
    AuthenticatedClientUser.set(user);
    FileInfo info = fsMaster.getFileInfo(new AlluxioURI("/file"));
    Assert.assertEquals(status, new URIStatus(info));
    registry.stop();
  }

  private MasterRegistry createFsMasterFromJournal() throws Exception {
    return MasterTestUtils.createLeaderFileSystemMasterFromJournal();
  }

  private void deleteFsMasterJournalLogs() throws Exception {
    String journalFolder = mLocalAlluxioCluster.getMaster().getJournalFolder();
    UfsJournal journal = new UfsMutableJournal(
        new URI(PathUtils.concatPath(journalFolder, Constants.FILE_SYSTEM_MASTER_NAME)));
    UnderFileSystem.Factory.get(journalFolder).deleteFile(journal.getCurrentLog().toString());
  }

  /**
   * Test class implements {@link GroupMappingService} providing user-to-groups mapping.
   */
  public static class FakeUserGroupsMapping implements GroupMappingService {
    // The fullly qualified class name of this group mapping service. This is needed to configure
    // the alluxio cluster
    public static final String FULL_CLASS_NAME =
        "alluxio.master.UfsJournalIntegrationTest$FakeUserGroupsMapping";

    private HashMap<String, String> mUserGroups = new HashMap<>();

    /**
     * Constructor of {@link FakeUserGroupsMapping} to put the user and groups in user-to-groups
     * HashMap.
     */
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
  }
}
