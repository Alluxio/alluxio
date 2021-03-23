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

package alluxio.client.fs;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.client.WriteType;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AccessControlException;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.GetStatusContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.MountContext;
import alluxio.master.file.meta.TtlIntervalRule;
import alluxio.security.authorization.Mode;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.master.FsMasterResource;
import alluxio.testutils.master.MasterTestUtils;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UfsMode;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.wire.FileInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test behavior of {@link FileSystemMaster}, with master restarts.
 */
public class FileSystemMasterRestartIntegrationTest extends BaseIntegrationTest {
  // Modify current time so that implementations can't accidentally pass unit tests by ignoring
  // this specified time and always using System.currentTimeMillis()
  private static final long TEST_TIME_MS = Long.MAX_VALUE;
  private static final long TTL_CHECKER_INTERVAL_MS = 100;
  private static final String TEST_USER = "test";

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.MASTER_TTL_CHECK);

  @ClassRule
  public static TtlIntervalRule sTtlIntervalRule = new TtlIntervalRule(TTL_CHECKER_INTERVAL_MS);

  @Rule
  public Timeout mGlobalTimeout = Timeout.seconds(60);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false)
          .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS,
              String.valueOf(TTL_CHECKER_INTERVAL_MS))
          .setProperty(PropertyKey.WORKER_RAMDISK_SIZE, 1000)
          .setProperty(PropertyKey.MASTER_FILE_ACCESS_TIME_UPDATE_PRECISION, 0)
          .setProperty(PropertyKey.SECURITY_LOGIN_USERNAME, TEST_USER).build();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER,
      ServerConfiguration.global());

  private FileSystemMaster mFsMaster;

  @Before
  public final void before() throws Exception {
    mFsMaster = mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class);
  }

  private FsMasterResource createFileSystemMasterFromJournal() throws Exception {
    return MasterTestUtils.createLeaderFileSystemMasterFromJournalCopy();
  }

  @Test
  public void syncReplay() throws Exception {
    AlluxioURI root = new AlluxioURI("/");
    AlluxioURI alluxioFile = new AlluxioURI("/in_alluxio");

    // Create a persisted Alluxio file (but no ufs file).
    mFsMaster.createFile(alluxioFile, CreateFileContext.defaults()
        .setWriteType(WriteType.CACHE_THROUGH));
    mFsMaster.completeFile(alluxioFile,
        CompleteFileContext.mergeFrom(CompleteFilePOptions.newBuilder().setUfsLength(0))
            .setOperationTimeMs(TEST_TIME_MS));

    // List what is in Alluxio, without syncing.
    List<FileInfo> files = mFsMaster.listStatus(root,
        ListStatusContext.mergeFrom(ListStatusPOptions.newBuilder()
            .setLoadMetadataType(LoadMetadataPType.NEVER)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1))));
    Assert.assertEquals(1, files.size());
    Assert.assertEquals(alluxioFile.getName(), files.get(0).getName());

    // Add ufs only paths
    String ufs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Files.createDirectory(Paths.get(ufs, "ufs_dir"));
    Files.createFile(Paths.get(ufs, "ufs_file"));

    // List with syncing, which will remove alluxio only path, and add ufs only paths.
    files = mFsMaster.listStatus(root,
        ListStatusContext.mergeFrom(ListStatusPOptions.newBuilder()
            .setLoadMetadataType(LoadMetadataPType.NEVER)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0))));
    Assert.assertEquals(2, files.size());
    Set<String> filenames = files.stream().map(FileInfo::getName).collect(Collectors.toSet());
    Assert.assertTrue(filenames.contains("ufs_dir"));
    Assert.assertTrue(filenames.contains("ufs_file"));

    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    try (FsMasterResource masterResource = createFileSystemMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);

      // List what is in Alluxio, without syncing. Should match the last state.
      files = fsMaster.listStatus(root, ListStatusContext.mergeFrom(
          ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)
              .setCommonOptions(
                  FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1))));
      Assert.assertEquals(2, files.size());
      filenames = files.stream().map(FileInfo::getName).collect(Collectors.toSet());
      Assert.assertTrue(filenames.contains("ufs_dir"));
      Assert.assertTrue(filenames.contains("ufs_file"));
    }
  }

  @Test
  public void syncDirReplay() throws Exception {
    AlluxioURI dir = new AlluxioURI("/dir/");

    // Add ufs nested file.
    String ufs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Files.createDirectory(Paths.get(ufs, "dir"));
    Files.createFile(Paths.get(ufs, "dir", "file"));

    File ufsDir = new File(Paths.get(ufs, "dir").toString());
    Assert.assertTrue(ufsDir.setReadable(true, false));
    Assert.assertTrue(ufsDir.setWritable(true, false));
    Assert.assertTrue(ufsDir.setExecutable(true, false));

    // List dir with syncing
    FileInfo info = mFsMaster.getFileInfo(dir,
        GetStatusContext.mergeFrom(GetStatusPOptions.newBuilder()
            .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(
                FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).build())));
    Assert.assertNotNull(info);
    Assert.assertEquals("dir", info.getName());
    // Retrieve the mode
    int mode = info.getMode();

    // Update mode of the ufs dir
    Assert.assertTrue(ufsDir.setExecutable(false, false));

    // List dir with syncing, should update the mode
    info = mFsMaster.getFileInfo(dir,
        GetStatusContext.mergeFrom(GetStatusPOptions.newBuilder()
            .setLoadMetadataType(LoadMetadataPType.NEVER).setCommonOptions(
                FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0).build())));
    Assert.assertNotNull(info);
    Assert.assertEquals("dir", info.getName());
    Assert.assertNotEquals(mode, info.getMode());
    // update the expected mode
    mode = info.getMode();

    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    try (FsMasterResource masterResource = createFileSystemMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);

      // List what is in Alluxio, without syncing.
      info = fsMaster.getFileInfo(dir, GetStatusContext.mergeFrom(
          GetStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)
              .setCommonOptions(
                  FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(-1).build())));
      Assert.assertNotNull(info);
      Assert.assertEquals("dir", info.getName());
      Assert.assertEquals(mode, info.getMode());
    }
  }

  @Test
  public void unavailableUfsRecursiveCreate() throws Exception {
    String ufsBase = "test://test/";

    UnderFileSystemFactory mockUfsFactory = Mockito.mock(UnderFileSystemFactory.class);
    Mockito.when(mockUfsFactory.supportsPath(Matchers.anyString(), Matchers.any()))
        .thenReturn(Boolean.FALSE);
    Mockito.when(mockUfsFactory.supportsPath(Matchers.eq(ufsBase), Matchers.any()))
        .thenReturn(Boolean.TRUE);

    UnderFileSystem mockUfs = Mockito.mock(UnderFileSystem.class);
    UfsDirectoryStatus ufsStatus = new
        UfsDirectoryStatus("test", "owner", "group", (short) 511);
    Mockito.when(mockUfsFactory.create(Matchers.eq(ufsBase), Matchers.any())).thenReturn(mockUfs);
    Mockito.when(mockUfs.isDirectory(ufsBase)).thenReturn(true);
    Mockito.when(mockUfs.resolveUri(new AlluxioURI(ufsBase), ""))
        .thenReturn(new AlluxioURI(ufsBase));
    Mockito.when(mockUfs.resolveUri(new AlluxioURI(ufsBase), "/dir1"))
        .thenReturn(new AlluxioURI(ufsBase + "/dir1"));
    Mockito.when(mockUfs.getExistingDirectoryStatus(ufsBase))
        .thenReturn(ufsStatus);
    Mockito.when(mockUfs.mkdirs(Matchers.eq(ufsBase + "/dir1"), Matchers.any()))
        .thenThrow(new IOException("ufs unavailable"));
    Mockito.when(mockUfs.getStatus(ufsBase))
        .thenReturn(ufsStatus);

    UnderFileSystemFactoryRegistry.register(mockUfsFactory);

    mFsMaster.mount(new AlluxioURI("/mnt"), new AlluxioURI(ufsBase), MountContext.defaults());

    AlluxioURI root = new AlluxioURI("/mnt/");
    AlluxioURI alluxioFile = new AlluxioURI("/mnt/dir1/dir2/file");

    // Create a persisted Alluxio file (but no ufs file).
    try {
      mFsMaster.createFile(alluxioFile, CreateFileContext
          .mergeFrom(CreateFilePOptions.newBuilder().setRecursive(true))
          .setWriteType(WriteType.CACHE_THROUGH));
      Assert.fail("persisted create should fail, when UFS is unavailable");
    } catch (Exception e) {
      // expected, ignore
    }

    List<FileInfo> files =
        mFsMaster.listStatus(root, ListStatusContext.defaults());

    Assert.assertTrue(files.isEmpty());

    try {
      // should not exist
      files = mFsMaster.listStatus(new AlluxioURI("/mnt/dir1/"),
          ListStatusContext.defaults());
      Assert.fail("dir should not exist, when UFS is unavailable");
    } catch (Exception e) {
      // expected, ignore
    }

    try {
      // should not exist
      mFsMaster.delete(new AlluxioURI("/mnt/dir1/"),
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
      Assert.fail("cannot delete non-existing directory, when UFS is unavailable");
    } catch (Exception e) {
      // expected, ignore
      files = null;
    }

    files = mFsMaster.listStatus(new AlluxioURI("/mnt/"),
        ListStatusContext.defaults());
    Assert.assertTrue(files.isEmpty());

    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    try (FsMasterResource masterResource = MasterTestUtils
        .createLeaderFileSystemMasterFromJournal()) {
      FileSystemMaster newFsMaster = masterResource.getRegistry().get(FileSystemMaster.class);

      files = newFsMaster.listStatus(new AlluxioURI("/mnt/"),
          ListStatusContext.defaults());
      Assert.assertTrue(files.isEmpty());
    }
  }

  @Test
  public void ufsModeReplay() throws Exception {
    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.NO_ACCESS);

    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    try (FsMasterResource masterResource = createFileSystemMasterFromJournal()) {
      FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);

      AlluxioURI alluxioFile = new AlluxioURI("/in_alluxio");
      // Create file should throw an Exception even after restart
      mThrown.expect(AccessControlException.class);
      fsMaster.createFile(alluxioFile, CreateFileContext.defaults()
          .setWriteType(WriteType.CACHE_THROUGH));
    }
  }

  /**
   * Tests journal is updated with access time asynchronously before master is stopped.
   */
  @Test
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.MASTER_METASTORE, "HEAP"})
  public void updateAccessTimeAsyncFlush() throws Exception {
    String parentName = "d1";
    AlluxioURI parentPath = new AlluxioURI("/" + parentName);
    long parentId = mFsMaster.createDirectory(parentPath,
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)
            .setMode(new Mode((short) 0700).toProto())));
    long oldAccessTime = mFsMaster.getFileInfo(parentId).getLastAccessTimeMs();
    Thread.sleep(100);
    mFsMaster.listStatus(parentPath, ListStatusContext.defaults());
    long newAccessTime = mFsMaster.getFileInfo(parentId).getLastAccessTimeMs();
    // time is changed in master
    Assert.assertNotEquals(newAccessTime, oldAccessTime);
    try (FsMasterResource masterResource = createFileSystemMasterFromJournal()) {
      FileSystemMaster fsm = masterResource.getRegistry().get(FileSystemMaster.class);
      long journaledAccessTime = fsm.getFileInfo(parentId).getLastAccessTimeMs();
      // time is not flushed to journal
      Assert.assertEquals(journaledAccessTime, oldAccessTime);
    }
    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    try (FsMasterResource masterResource = createFileSystemMasterFromJournal()) {
      FileSystemMaster fsm = masterResource.getRegistry().get(FileSystemMaster.class);
      long journaledAccessTimeAfterStop = fsm.getFileInfo(parentId).getLastAccessTimeMs();
      // time is now flushed to journal
      Assert.assertEquals(journaledAccessTimeAfterStop, newAccessTime);
    }
  }

  /**
   * Tests journal is not updated with access time asynchronously after delete.
   */
  @Test
  @LocalAlluxioClusterResource.Config(confParams = {PropertyKey.Name.MASTER_METASTORE, "HEAP"})
  public void updateAccessTimeAsyncFlushAfterDelete() throws Exception {
    String parentName = "d1";
    AlluxioURI parentPath = new AlluxioURI("/" + parentName);
    long parentId = mFsMaster.createDirectory(parentPath,
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)
            .setMode(new Mode((short) 0700).toProto())));
    long oldAccessTime = mFsMaster.getFileInfo(parentId).getLastAccessTimeMs();
    Thread.sleep(100);
    mFsMaster.listStatus(parentPath, ListStatusContext.defaults());
    long newAccessTime = mFsMaster.getFileInfo(parentId).getLastAccessTimeMs();
    // time is changed in master
    Assert.assertNotEquals(newAccessTime, oldAccessTime);
    try (FsMasterResource masterResource = createFileSystemMasterFromJournal()) {
      FileSystemMaster fsm = masterResource.getRegistry().get(FileSystemMaster.class);
      long journaledAccessTime = fsm.getFileInfo(parentId).getLastAccessTimeMs();
      // time is not flushed to journal
      Assert.assertEquals(journaledAccessTime, oldAccessTime);
      // delete the directory
      mFsMaster.delete(parentPath, DeleteContext.defaults());
    }
    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    try (FsMasterResource masterResource = createFileSystemMasterFromJournal()) {
      FileSystemMaster fsm = masterResource.getRegistry().get(FileSystemMaster.class);
      Assert.assertEquals(fsm.getFileId(parentPath), -1);
    }
  }
}
