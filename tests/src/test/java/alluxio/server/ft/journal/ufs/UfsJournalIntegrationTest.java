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

package alluxio.server.ft.journal.ufs;

import alluxio.AlluxioTestDirectory;
import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.WritePType;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.NoopMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.journal.JournalType;
import alluxio.master.journal.ufs.UfsJournal;
import alluxio.master.journal.ufs.UfsJournalSnapshot;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.security.group.GroupMappingService;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.master.FsMasterResource;
import alluxio.testutils.master.MasterTestUtils;
import alluxio.underfs.UfsStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.IdUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test master journal, including checkpoint and entry log.
 */
public class UfsJournalIntegrationTest extends BaseIntegrationTest {

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.MASTER_JOURNAL_TYPE, JournalType.UFS.toString())
          .setProperty(PropertyKey.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX,
              Integer.toString(Constants.KB))
          .setProperty(PropertyKey.MASTER_JOURNAL_CHECKPOINT_PERIOD_ENTRIES, "2")
          .setProperty(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "false")
          .setProperty(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, "CACHE_THROUGH")
          .setProperty(PropertyKey.MASTER_METASTORE_DIR,
              AlluxioTestDirectory.createTemporaryDirectory("meta"))
          .setProperty(PropertyKey.MASTER_FILE_ACCESS_TIME_JOURNAL_FLUSH_INTERVAL, "0s")
          .build();

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
    CreateFilePOptions options =
        CreateFilePOptions.newBuilder().setBlockSizeBytes(64).setRecursive(true).build();
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
    try (FsMasterResource masterResource = createFsMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);

      long rootId = fsMaster.getFileId(mRootUri);
      Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
      Assert.assertEquals(1, fsMaster.listStatus(mRootUri, ListStatusContext
          .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)))
          .size());
      long xyzId = fsMaster.getFileId(new AlluxioURI("/xyz"));
      Assert.assertTrue(xyzId != IdUtils.INVALID_FILE_ID);
      FileInfo fsMasterInfo = fsMaster.getFileInfo(xyzId);
      Assert.assertEquals(0, fsMaster.getFileInfo(xyzId).getInMemoryPercentage());
      Assert.assertEquals(status.getBlockIds(), fsMasterInfo.getBlockIds());
      Assert.assertEquals(status.getBlockSizeBytes(), fsMasterInfo.getBlockSizeBytes());
      Assert.assertEquals(status.getLength(), fsMasterInfo.getLength());
    }
  }

  /**
   * Tests flushing the journal multiple times, without writing any data.
   */
  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.MASTER_JOURNAL_LOG_SIZE_BYTES_MAX, "0"})
  public void multipleFlush() throws Exception {
    String journalFolder = mLocalAlluxioCluster.getLocalAlluxioMaster().getJournalFolder();
    mLocalAlluxioCluster.stop();
    UfsJournal journal = new UfsJournal(
        new URI(PathUtils.concatPath(journalFolder, Constants.FILE_SYSTEM_MASTER_NAME)),
        new NoopMaster(), 0, Collections::emptySet);
    journal.start();
    journal.gainPrimacy();

    UfsStatus[] paths = UnderFileSystem.Factory.create(journalFolder, ServerConfiguration.global())
        .listStatus(journal.getLogDir().toString());
    int expectedSize = paths == null ? 0 : paths.length;

    journal.flush();
    journal.flush();
    journal.flush();
    journal.close();
    paths = UnderFileSystem.Factory.create(journalFolder, ServerConfiguration.global())
        .listStatus(journal.getLogDir().toString());
    int actualSize = paths == null ? 0 : paths.length;
    // No new files are created.
    Assert.assertEquals(expectedSize, actualSize);
  }

  /**
   * Tests loading metadata.
   */
  @Test
  public void loadMetadata() throws Exception {
    String ufsRoot = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    UnderFileSystem ufs = UnderFileSystem.Factory.createForRoot(ServerConfiguration.global());
    ufs.create(ufsRoot + "/xyz").close();
    URIStatus status = mFileSystem.getStatus(new AlluxioURI("/xyz"));
    mLocalAlluxioCluster.stopFS();
    loadMetadataTestUtil(status);
    deleteFsMasterJournalLogs();
    loadMetadataTestUtil(status);
  }

  private void loadMetadataTestUtil(URIStatus status) throws Exception {
    try (FsMasterResource masterResource = createFsMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);

      long rootId = fsMaster.getFileId(mRootUri);
      Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
      Assert.assertEquals(1, fsMaster.listStatus(mRootUri, ListStatusContext
          .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)))
          .size());
      Assert.assertTrue(fsMaster.getFileId(new AlluxioURI("/xyz")) != IdUtils.INVALID_FILE_ID);
      FileInfo fsMasterInfo = fsMaster.getFileInfo(fsMaster.getFileId(new AlluxioURI("/xyz")));
      Assert.assertEquals(status, new URIStatus(fsMasterInfo.setMountId(status.getMountId())));
    }
  }

  /**
   * Tests completed edit log deletion.
   */
  @Test
  public void completedEditLogDeletion() throws Exception {
    for (int i = 0; i < 124; i++) {
      mFileSystem
          .createFile(new AlluxioURI("/a" + i),
              CreateFilePOptions.newBuilder().setBlockSizeBytes((i + 10) / 10 * 64).build())
          .close();
    }
    mLocalAlluxioCluster.stopFS();

    String journalFolder = PathUtils
        .concatPath(mLocalAlluxioCluster.getLocalAlluxioMaster().getJournalFolder(),
            Constants.FILE_SYSTEM_MASTER_NAME);
    UfsJournal journal =
        new UfsJournal(new URI(journalFolder), new NoopMaster(), 0, Collections::emptySet);
    URI completedLocation = journal.getLogDir();
    Assert.assertTrue(UnderFileSystem.Factory.create(completedLocation.toString(),
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global()))
        .listStatus(completedLocation.toString()).length > 1);
    multiEditLogTestUtil();
    Assert.assertTrue(UnderFileSystem.Factory.create(completedLocation.toString(),
        UnderFileSystemConfiguration.defaults(ServerConfiguration.global()))
        .listStatus(completedLocation.toString()).length > 1);
    multiEditLogTestUtil();
  }

  /**
   * Tests file and directory creation and deletion.
   */
  @Test
  public void delete() throws Exception {
    CreateDirectoryPOptions recMkdir =
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build();
    DeletePOptions recDelete = DeletePOptions.newBuilder().setRecursive(true).build();
    for (int i = 0; i < 10; i++) {
      String dirPath = "/i" + i;
      mFileSystem.createDirectory(new AlluxioURI(dirPath), recMkdir);
      for (int j = 0; j < 10; j++) {
        CreateFilePOptions option =
            CreateFilePOptions.newBuilder().setBlockSizeBytes((i + j + 1) * 64).build();
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
    try (FsMasterResource masterResource = createFsMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);
      long rootId = fsMaster.getFileId(mRootUri);
      Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
      Assert.assertEquals(5, fsMaster.listStatus(mRootUri, ListStatusContext
          .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)))
          .size());
      for (int i = 0; i < 5; i++) {
        for (int j = 0; j < 5; j++) {
          Assert.assertTrue(
              fsMaster.getFileId(new AlluxioURI("/i" + i + "/j" + j)) != IdUtils.INVALID_FILE_ID);
        }
      }
    }
  }

  @Test
  public void emptyFileSystem() throws Exception {
    Assert.assertEquals(0, mFileSystem.listStatus(mRootUri).size());
    mLocalAlluxioCluster.stopFS();
    try (FsMasterResource masterResource = createFsMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);
      long rootId = fsMaster.getFileId(mRootUri);
      Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
      Assert.assertEquals(0, fsMaster.listStatus(mRootUri, ListStatusContext
          .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)))
          .size());
    }
  }

  /**
   * Tests file and directory creation.
   */
  @Test
  public void fileDirectory() throws Exception {
    for (int i = 0; i < 10; i++) {
      mFileSystem.createDirectory(new AlluxioURI("/i" + i));
      for (int j = 0; j < 10; j++) {
        CreateFilePOptions option =
            CreateFilePOptions.newBuilder().setBlockSizeBytes((i + j + 1) * 64).build();
        mFileSystem.createFile(new AlluxioURI("/i" + i + "/j" + j), option).close();
      }
    }
    mLocalAlluxioCluster.stopFS();
    fileDirectoryTestUtil();
    deleteFsMasterJournalLogs();
    fileDirectoryTestUtil();
  }

  private void fileDirectoryTestUtil() throws Exception {
    try (FsMasterResource masterResource = createFsMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);
      long rootId = fsMaster.getFileId(mRootUri);
      Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
      Assert.assertEquals(10, fsMaster.listStatus(mRootUri, ListStatusContext
          .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)))
          .size());
      for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 10; j++) {
          Assert.assertTrue(
              fsMaster.getFileId(new AlluxioURI("/i" + i + "/j" + j)) != IdUtils.INVALID_FILE_ID);
        }
      }
    }
  }

  /**
   * Tests file creation.
   */
  @Test
  public void file() throws Exception {
    CreateFilePOptions option = CreateFilePOptions.newBuilder().setBlockSizeBytes(64).build();
    AlluxioURI filePath = new AlluxioURI("/xyz");
    mFileSystem.createFile(filePath, option).close();
    URIStatus status = mFileSystem.getStatus(filePath);
    mLocalAlluxioCluster.stopFS();
    fileTestUtil(status);
    deleteFsMasterJournalLogs();
    fileTestUtil(status);
  }

  private void fileTestUtil(URIStatus status) throws Exception {
    try (FsMasterResource masterResource = createFsMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);
      long rootId = fsMaster.getFileId(mRootUri);
      Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
      Assert.assertEquals(1, fsMaster.listStatus(mRootUri, ListStatusContext
          .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)))
          .size());
      long fileId = fsMaster.getFileId(new AlluxioURI("/xyz"));
      Assert.assertTrue(fileId != IdUtils.INVALID_FILE_ID);
      Assert.assertEquals(status,
          new URIStatus(fsMaster.getFileInfo(fileId).setMountId(status.getMountId())));
    }
  }

  /**
   * Tests journalling of inodes being pinned.
   */
  @Test
  public void pin() throws Exception {
    SetAttributePOptions setPinned = SetAttributePOptions.newBuilder().setPinned(true).build();
    SetAttributePOptions setUnpinned = SetAttributePOptions.newBuilder().setPinned(false).build();
    AlluxioURI dirUri = new AlluxioURI("/myFolder");
    mFileSystem.createDirectory(dirUri);
    mFileSystem.setAttribute(dirUri, setPinned);

    AlluxioURI file0Path = new AlluxioURI("/myFolder/file0");
    CreateFilePOptions op = CreateFilePOptions.newBuilder().setBlockSizeBytes(64).build();
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
    try (FsMasterResource masterResource = createFsMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);

      FileInfo info = fsMaster.getFileInfo(fsMaster.getFileId(new AlluxioURI("/myFolder")));
      Assert.assertEquals(directory, new URIStatus(info.setMountId(directory.getMountId())));
      Assert.assertTrue(info.isPinned());

      info = fsMaster.getFileInfo(fsMaster.getFileId(new AlluxioURI("/myFolder/file0")));
      Assert.assertEquals(file0, new URIStatus(info.setMountId(file0.getMountId())));
      Assert.assertFalse(info.isPinned());

      info = fsMaster.getFileInfo(fsMaster.getFileId(new AlluxioURI("/myFolder/file1")));
      Assert.assertEquals(file1, new URIStatus(info.setMountId(file1.getMountId())));
      Assert.assertTrue(info.isPinned());
    }
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
    try (FsMasterResource masterResource = createFsMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);
      long rootId = fsMaster.getFileId(mRootUri);
      Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
      Assert.assertEquals(1, fsMaster.listStatus(mRootUri, ListStatusContext
          .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)))
          .size());
      long fileId = fsMaster.getFileId(new AlluxioURI("/xyz"));
      Assert.assertTrue(fileId != IdUtils.INVALID_FILE_ID);
      Assert.assertEquals(status,
          new URIStatus(fsMaster.getFileInfo(fileId).setMountId(status.getMountId())));
    }
  }

  /**
   * Tests journalling of creating directories with MUST_CACHE, then creating the same directories
   * again with CACHE_THROUGH and AllowExists=true.
   */
  @Test
  public void persistDirectoryLater() throws Exception {
    String[] directories = new String[] {
        "/d11", "/d11/d21", "/d11/d22",
        "/d12", "/d12/d21", "/d12/d22",
    };

    CreateDirectoryPOptions options = CreateDirectoryPOptions.newBuilder().setRecursive(true)
        .setWriteType(WritePType.MUST_CACHE).build();
    for (String directory : directories) {
      mFileSystem.createDirectory(new AlluxioURI(directory), options);
    }

    options = options.toBuilder().setWriteType(WritePType.CACHE_THROUGH).setAllowExists(true)
        .build();
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
    try (FsMasterResource masterResource = createFsMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);
      for (Map.Entry<String, URIStatus> directoryStatus : directoryStatuses.entrySet()) {
        Assert.assertEquals(directoryStatus.getValue(), new URIStatus(
            fsMaster.getFileInfo(fsMaster.getFileId(new AlluxioURI(directoryStatus.getKey())))
                .setMountId(directoryStatus.getValue().getMountId())));
      }
    }
  }

  /**
   * Tests files creation.
   */
  @Test
  public void manyFile() throws Exception {
    for (int i = 0; i < 10; i++) {
      CreateFilePOptions option =
          CreateFilePOptions.newBuilder().setBlockSizeBytes((i + 1) * 64).build();
      mFileSystem.createFile(new AlluxioURI("/a" + i), option).close();
    }
    mLocalAlluxioCluster.stopFS();
    manyFileTestUtil();
    deleteFsMasterJournalLogs();
    manyFileTestUtil();
  }

  private void manyFileTestUtil() throws Exception {
    try (FsMasterResource masterResource = createFsMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);
      long rootId = fsMaster.getFileId(mRootUri);
      Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
      Assert.assertEquals(10, fsMaster.listStatus(mRootUri, ListStatusContext
          .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)))
          .size());
      for (int k = 0; k < 10; k++) {
        Assert.assertTrue(fsMaster.getFileId(new AlluxioURI("/a" + k)) != IdUtils.INVALID_FILE_ID);
      }
    }
  }

  /**
   * Tests reading multiple edit logs.
   */
  @Test
  public void multiEditLog() throws Exception {
    for (int i = 0; i < 124; i++) {
      CreateFilePOptions op =
          CreateFilePOptions.newBuilder().setBlockSizeBytes((i + 10) / 10 * 64).build();
      mFileSystem.createFile(new AlluxioURI("/a" + i), op).close();
    }
    mLocalAlluxioCluster.stopFS();
    multiEditLogTestUtil();
    deleteFsMasterJournalLogs();
    multiEditLogTestUtil();
  }

  private void multiEditLogTestUtil() throws Exception {
    try (FsMasterResource masterResource = createFsMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);
      long rootId = fsMaster.getFileId(mRootUri);
      Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
      Assert.assertEquals(124, fsMaster.listStatus(mRootUri, ListStatusContext
          .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)))
          .size());
      for (int k = 0; k < 124; k++) {
        Assert.assertTrue(fsMaster.getFileId(new AlluxioURI("/a" + k)) != IdUtils.INVALID_FILE_ID);
      }
    }
  }

  /**
   * Tests file and directory creation, and rename.
   */
  @Test
  public void rename() throws Exception {
    for (int i = 0; i < 10; i++) {
      mFileSystem.createDirectory(new AlluxioURI("/i" + i));
      for (int j = 0; j < 10; j++) {
        CreateFilePOptions option =
            CreateFilePOptions.newBuilder().setBlockSizeBytes((i + j + 1) * 64).build();
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
    try (FsMasterResource masterResource = createFsMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);
      long rootId = fsMaster.getFileId(mRootUri);
      Assert.assertTrue(rootId != IdUtils.INVALID_FILE_ID);
      Assert.assertEquals(10, fsMaster.listStatus(mRootUri, ListStatusContext
          .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)))
          .size());
      for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 10; j++) {
          Assert.assertTrue(
              fsMaster.getFileId(new AlluxioURI("/ii" + i + "/jj" + j)) != IdUtils.INVALID_FILE_ID);
        }
      }
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(confParams = {
      PropertyKey.Name.SECURITY_AUTHENTICATION_TYPE, "SIMPLE",
      PropertyKey.Name.SECURITY_AUTHORIZATION_PERMISSION_ENABLED, "true",
      PropertyKey.Name.SECURITY_GROUP_MAPPING_CLASS, FakeUserGroupsMapping.FULL_CLASS_NAME})
  public void setAcl() throws Exception {
    AlluxioURI filePath = new AlluxioURI("/file");

    String user = "alluxio";
    ServerConfiguration.set(PropertyKey.SECURITY_LOGIN_USERNAME, user);
    CreateFilePOptions op = CreateFilePOptions.newBuilder().setBlockSizeBytes(64).build();
    mFileSystem.createFile(filePath, op).close();

    // TODO(chaomin): also setOwner and setGroup once there's a way to fake the owner/group in UFS.
    mFileSystem.setAttribute(filePath, SetAttributePOptions.newBuilder()
        .setMode(new Mode((short) 0400).toProto()).setRecursive(false).build());

    URIStatus status = mFileSystem.getStatus(filePath);

    mLocalAlluxioCluster.stopFS();

    aclTestUtil(status, user);
    deleteFsMasterJournalLogs();
    aclTestUtil(status, user);
  }

  private void aclTestUtil(URIStatus status, String user) throws Exception {
    try (FsMasterResource masterResource = createFsMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);
      AuthenticatedClientUser.set(user);
      FileInfo info = fsMaster.getFileInfo(new AlluxioURI("/file"), GetStatusContext.defaults());
      Assert.assertEquals(status, new URIStatus(info.setMountId(status.getMountId())));
    }
  }

  private FsMasterResource createFsMasterFromJournal() throws Exception {
    return MasterTestUtils.createLeaderFileSystemMasterFromJournal();
  }

  private void deleteFsMasterJournalLogs() throws Exception {
    String journalFolder = mLocalAlluxioCluster.getLocalAlluxioMaster().getJournalFolder();
    UfsJournal journal = new UfsJournal(
        new URI(PathUtils.concatPath(journalFolder, Constants.FILE_SYSTEM_MASTER_NAME)),
        new NoopMaster(), 0, Collections::emptySet);
    if (UfsJournalSnapshot.getCurrentLog(journal) != null) {
      UnderFileSystem.Factory.create(journalFolder, ServerConfiguration.global())
          .deleteFile(UfsJournalSnapshot.getCurrentLog(journal).getLocation().toString());
    }
  }

  /**
   * Test class implements {@link GroupMappingService} providing user-to-groups mapping.
   */
  public static class FakeUserGroupsMapping implements GroupMappingService {
    // The fullly qualified class name of this group mapping service. This is needed to configure
    // the alluxio cluster
    public static final String FULL_CLASS_NAME =
        "alluxio.server.ft.journal.ufs.UfsJournalIntegrationTest$FakeUserGroupsMapping";

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
