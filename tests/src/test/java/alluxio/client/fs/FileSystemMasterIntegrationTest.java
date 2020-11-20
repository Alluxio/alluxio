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

import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeFalse;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedUserRule;
import alluxio.ClientContext;
import alluxio.Constants;
import alluxio.client.WriteType;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AccessControlException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.FreePOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.TtlAction;
import alluxio.grpc.WritePType;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.MasterClientContext;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.CompleteFileContext;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.FreeContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.master.file.contexts.RenameContext;
import alluxio.master.file.contexts.SetAttributeContext;
import alluxio.master.file.meta.TtlIntervalRule;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.master.FsMasterResource;
import alluxio.testutils.master.MasterTestUtils;
import alluxio.underfs.UfsMode;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.util.ShellUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.wire.FileInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * Test behavior of {@link FileSystemMaster}.
 *
 * For example, (concurrently) creating/deleting/renaming files.
 */
public class FileSystemMasterIntegrationTest extends BaseIntegrationTest {
  private static final int DEPTH = 6;
  private static final int FILES_PER_NODE = 4;
  private static final int CONCURRENCY_DEPTH = 3;
  private static final AlluxioURI ROOT_PATH = new AlluxioURI("/root");
  private static final AlluxioURI ROOT_PATH2 = new AlluxioURI("/root2");
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

  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_METRICS_COLLECTION_ENABLED, false)
          .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS,
              String.valueOf(TTL_CHECKER_INTERVAL_MS))
          .setProperty(PropertyKey.WORKER_RAMDISK_SIZE, "10mb")
          .setProperty(PropertyKey.MASTER_FILE_ACCESS_TIME_UPDATE_PRECISION, 0)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, "1kb")
          .setProperty(PropertyKey.SECURITY_LOGIN_USERNAME, TEST_USER).build();

  @Rule
  public TestRule mResetRule = sLocalAlluxioClusterResource.getResetResource();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER,
      ServerConfiguration.global());

  private FileSystemMaster mFsMaster;

  @Before
  public final void before() throws Exception {
    mFsMaster = sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class);
  }

  /**
   * Tests the {@link FileInfo} of a directory.
   */
  @Test
  public void clientFileInfoDirectory() throws Exception {
    AlluxioURI path = new AlluxioURI("/testFolder");
    mFsMaster.createDirectory(path, CreateDirectoryContext.defaults());
    long fileId = mFsMaster.getFileId(path);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("testFolder", fileInfo.getName());
    Assert.assertEquals(0, fileInfo.getLength());
    assertFalse(fileInfo.isCacheable());
    Assert.assertTrue(fileInfo.isCompleted());
    Assert.assertTrue(fileInfo.isFolder());
    assertFalse(fileInfo.isPersisted());
    assertFalse(fileInfo.isPinned());
    Assert.assertEquals(TEST_USER, fileInfo.getOwner());
    Assert.assertEquals(0755, (short) fileInfo.getMode());
  }

  /**
   * Tests the {@link FileInfo} of an empty file.
   */
  @Test
  public void clientFileInfoEmptyFile() throws Exception {
    FileInfo fileInfo =
        mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileContext.defaults());
    Assert.assertEquals("testFile", fileInfo.getName());
    Assert.assertEquals(0, fileInfo.getLength());
    Assert.assertTrue(fileInfo.isCacheable());
    assertFalse(fileInfo.isCompleted());
    assertFalse(fileInfo.isFolder());
    assertFalse(fileInfo.isPersisted());
    assertFalse(fileInfo.isPinned());
    Assert.assertEquals(Constants.NO_TTL, fileInfo.getTtl());
    Assert.assertEquals(TtlAction.DELETE, fileInfo.getTtlAction());
    Assert.assertEquals(TEST_USER, fileInfo.getOwner());
    Assert.assertEquals(0644, (short) fileInfo.getMode());
  }

  private FsMasterResource createFileSystemMasterFromJournal() throws Exception {
    return MasterTestUtils.createLeaderFileSystemMasterFromJournalCopy();
  }

  // TODO(calvin): This test currently relies on the fact the HDFS client is a cached instance to
  // avoid invalid lease exception. This should be fixed.
  @Ignore
  @Test
  public void concurrentCreateJournal() throws Exception {
    // Makes sure the file id's are the same between a master info and the journal it creates
    for (int i = 0; i < 5; i++) {
      ConcurrentCreator concurrentCreator =
          new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
      concurrentCreator.call();

      try (FsMasterResource masterResource = createFileSystemMasterFromJournal()) {
        FileSystemMaster fsMaster = masterResource.getRegistry().get(FileSystemMaster.class);
        for (FileInfo info : mFsMaster
            .listStatus(new AlluxioURI("/"), ListStatusContext.defaults())) {
          AlluxioURI path = new AlluxioURI(info.getPath());
          Assert.assertEquals(mFsMaster.getFileId(path), fsMaster.getFileId(path));
        }
      }
      before();
    }
  }

  /**
   * Tests concurrent create of files.
   */
  @Test
  public void concurrentCreate() throws Exception {
    ConcurrentCreator concurrentCreator =
        new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentCreator.call();
  }

  /**
   * Tests concurrent delete of files.
   */
  @Test
  public void concurrentDelete() throws Exception {
    ConcurrentCreator concurrentCreator =
        new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentCreator.call();

    ConcurrentDeleter concurrentDeleter =
        new ConcurrentDeleter(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentDeleter.call();

    Assert.assertEquals(0, mFsMaster
        .listStatus(new AlluxioURI("/"), ListStatusContext.defaults()).size());
  }

  /**
   * Tests concurrent free of files.
   */
  @Test
  public void concurrentFree() throws Exception {
    ConcurrentCreator concurrentCreator = new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH,
        CreateFileContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    concurrentCreator.call();

    ConcurrentFreer concurrentFreer = new ConcurrentFreer(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentFreer.call();
  }

  /**
   * Tests concurrent rename of files.
   */
  @Test
  public void concurrentRename() throws Exception {
    ConcurrentCreator concurrentCreator =
        new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentCreator.call();

    int numFiles =
        mFsMaster.listStatus(ROOT_PATH, ListStatusContext.defaults()).size();

    ConcurrentRenamer concurrentRenamer = new ConcurrentRenamer(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH,
        ROOT_PATH2, AlluxioURI.EMPTY_URI);
    concurrentRenamer.call();

    Assert.assertEquals(numFiles,
        mFsMaster.listStatus(ROOT_PATH2, ListStatusContext.defaults()).size());
  }

  @Test
  public void createAlreadyExistFile() throws Exception {
    mThrown.expect(FileAlreadyExistsException.class);
    mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileContext.defaults());
    mFsMaster.createDirectory(new AlluxioURI("/testFile"), CreateDirectoryContext.defaults());
  }

  @Test
  public void createDirectory() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryContext.defaults());
    FileInfo fileInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertTrue(fileInfo.isFolder());
    Assert.assertEquals(TEST_USER, fileInfo.getOwner());
    Assert.assertEquals(0755, (short) fileInfo.getMode());
  }

  @Test
  public void createFileInvalidPath() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mFsMaster.createFile(new AlluxioURI("testFile"), CreateFileContext.defaults());
  }

  @Test
  public void createFileInvalidPathTest2() throws Exception {
    mThrown.expect(FileAlreadyExistsException.class);
    mFsMaster.createFile(new AlluxioURI("/"), CreateFileContext.defaults());
  }

  @Test
  public void createFileInvalidPathTest3() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mFsMaster.createFile(new AlluxioURI("/testFile1"), CreateFileContext.defaults());
    mFsMaster.createFile(new AlluxioURI("/testFile1/testFile2"), CreateFileContext.defaults());
  }

  @Test
  public void createFilePerf() throws Exception {
    for (int k = 0; k < 200; k++) {
      CreateDirectoryContext context = CreateDirectoryContext
          .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true));
      mFsMaster.createDirectory(
          new AlluxioURI("/testFile").join(Constants.MASTER_COLUMN_FILE_PREFIX + k).join("0"),
          context);
    }
    for (int k = 0; k < 200; k++) {
      mFsMaster.getFileInfo(mFsMaster.getFileId(
          new AlluxioURI("/testFile").join(Constants.MASTER_COLUMN_FILE_PREFIX + k).join("0")));
    }
  }

  @Test
  public void createFile() throws Exception {
    mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileContext.defaults());
    FileInfo fileInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFile")));
    assertFalse(fileInfo.isFolder());
    Assert.assertEquals(TEST_USER, fileInfo.getOwner());
    Assert.assertEquals(0644, (short) fileInfo.getMode());
  }

  @Test
  public void deleteUnsyncedDirectory() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"),
        CreateDirectoryContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    mFsMaster.createDirectory(new AlluxioURI("/testFolder/child"),
        CreateDirectoryContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    String ufs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Files.createDirectory(Paths.get(ufs, "testFolder", "ufsOnlyDir"));
    try {
      mFsMaster.delete(new AlluxioURI("/testFolder"), DeleteContext
          .mergeFrom(DeletePOptions.newBuilder().setUnchecked(false).setRecursive(true)));
      Assert.fail("Expected deleting an out of sync directory to fail");
    } catch (IOException e) {
      // Expected
    }
    // Make sure the root folder still exists in Alluxio space.
    mFsMaster.listStatus(new AlluxioURI("/testFolder"), ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.NEVER)));
    // The child was in sync, so it should be deleted both from Alluxio and the UFS.
    mThrown.expect(FileDoesNotExistException.class);
    mFsMaster.listStatus(new AlluxioURI("/testFolder/child"), ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ALWAYS)));
  }

  @Test
  public void deleteDirectoryWithDirectories() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryContext.defaults());
    mFsMaster.createDirectory(new AlluxioURI("/testFolder/testFolder2"),
        CreateDirectoryContext.defaults());
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"),
        CreateFileContext.defaults()).getFileId();
    long fileId2 = mFsMaster.createFile(new AlluxioURI("/testFolder/testFolder2/testFile2"),
        CreateFileContext.defaults()).getFileId();
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    Assert.assertEquals(fileId2,
        mFsMaster.getFileId(new AlluxioURI("/testFolder/testFolder2/testFile2")));
    mFsMaster.delete(new AlluxioURI("/testFolder"),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID,
        mFsMaster.getFileId(new AlluxioURI("/testFolder/testFolder2/testFile2")));
  }

  @Test
  public void deleteDirectoryWithDirectoriesTest2() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryContext.defaults());
    mFsMaster.createDirectory(new AlluxioURI("/testFolder/testFolder2"),
        CreateDirectoryContext.defaults());
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"),
        CreateFileContext.defaults()).getFileId();
    long fileId2 = mFsMaster.createFile(new AlluxioURI("/testFolder/testFolder2/testFile2"),
        CreateFileContext.defaults()).getFileId();
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    Assert.assertEquals(fileId2,
        mFsMaster.getFileId(new AlluxioURI("/testFolder/testFolder2/testFile2")));
    try {
      mFsMaster.delete(new AlluxioURI("/testFolder/testFolder2"),
          DeleteContext.defaults());
      Assert.fail("Deleting a nonempty directory nonrecursively should fail");
    } catch (DirectoryNotEmptyException e) {
      Assert.assertEquals(
          ExceptionMessage.DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE.getMessage("testFolder2"),
          e.getMessage());
    }
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    Assert.assertEquals(fileId2,
        mFsMaster.getFileId(new AlluxioURI("/testFolder/testFolder2/testFile2")));
  }

  @Test
  public void deleteDirectoryWithFiles() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryContext.defaults());
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"),
        CreateFileContext.defaults()).getFileId();
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    mFsMaster.delete(new AlluxioURI("/testFolder"),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID,
        mFsMaster.getFileId(new AlluxioURI("/testFolder")));
  }

  @Test
  public void deleteDirectoryWithFilesTest2() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryContext.defaults());
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"),
        CreateFileContext.defaults()).getFileId();
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    try {
      mFsMaster.delete(new AlluxioURI("/testFolder"), DeleteContext.defaults());
      Assert.fail("Deleting a nonempty directory nonrecursively should fail");
    } catch (DirectoryNotEmptyException e) {
      Assert.assertEquals(
          ExceptionMessage.DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE.getMessage("testFolder"),
          e.getMessage());
    }
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
  }

  @Test
  public void deleteEmptyDirectory() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryContext.defaults());
    mFsMaster.delete(new AlluxioURI("/testFolder"),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID,
        mFsMaster.getFileId(new AlluxioURI("/testFolder")));
  }

  @Test
  public void deleteDirectoryWithPersistedAndNotPersistedSubfolders() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"),
        CreateDirectoryContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    mFsMaster.createDirectory(new AlluxioURI("/testFolder/persisted"),
        CreateDirectoryContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    mFsMaster.createDirectory(new AlluxioURI("/testFolder/inAlluxio1"),
        CreateDirectoryContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    mFsMaster.createDirectory(new AlluxioURI("/testFolder/inAlluxio1/inAlluxio2"),
        CreateDirectoryContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    mFsMaster.delete(new AlluxioURI("/testFolder"),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID,
        mFsMaster.getFileId(new AlluxioURI("/testFolder")));
  }

  @Test
  public void deleteDirectoryRecursive() throws Exception {
    AlluxioURI dir = new AlluxioURI("/testFolder");
    mFsMaster.createDirectory(dir, CreateDirectoryContext.defaults());
    FileSystem fs = sLocalAlluxioClusterResource.get().getClient();
    for (int i = 0; i < 3; i++) {
      FileSystemTestUtils.createByteFile(fs, PathUtils.concatPath(dir, "file" + i), 100,
          CreateFilePOptions.newBuilder().setWriteType(WritePType.MUST_CACHE).build());
    }
    fs.delete(dir, DeletePOptions.newBuilder().setRecursive(true).build());
    assertFalse(fs.exists(dir));
    // Make sure that the blocks are cleaned up
    BlockMasterClient blockClient =
        BlockMasterClient.Factory.create(MasterClientContext
            .newBuilder(ClientContext.create(ServerConfiguration.global())).build());
    CommonUtils.waitFor("data to be deleted", () -> {
      try {
        return blockClient.getUsedBytes() == 0;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, WaitForOptions.defaults().setTimeoutMs(10 * Constants.SECOND_MS));
  }

  @Test
  public void deleteFile() throws Exception {
    long fileId =
        mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileContext.defaults()).getFileId();
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFile")));
    mFsMaster.delete(new AlluxioURI("/testFile"),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFsMaster.getFileId(new AlluxioURI("/testFile")));
  }

  @Test
  public void deleteRoot() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage(ExceptionMessage.DELETE_ROOT_DIRECTORY.getMessage());
    mFsMaster.delete(new AlluxioURI("/"),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
  }

  @Test
  public void getCapacityBytes() {
    BlockMaster blockMaster =
        sLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
            .getMaster(BlockMaster.class);
    Assert.assertEquals(10 * Constants.MB, blockMaster.getCapacityBytes());
  }

  @Test
  public void lastModificationTimeCompleteFile() throws Exception {
    long fileId =
        mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileContext.defaults()).getFileId();
    long opTimeMs = TEST_TIME_MS;
    mFsMaster.completeFile(new AlluxioURI("/testFile"), CompleteFileContext
        .mergeFrom(CompleteFilePOptions.newBuilder().setUfsLength(0)).setOperationTimeMs(opTimeMs));
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals(opTimeMs, fileInfo.getLastModificationTimeMs());
    Assert.assertEquals(opTimeMs, fileInfo.getLastAccessTimeMs());
  }

  @Test
  public void lastModificationTimeCreateFile() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryContext.defaults());
    long opTimeMs = TEST_TIME_MS;
    CreateFileContext context = CreateFileContext.defaults().setOperationTimeMs(opTimeMs);
    mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"), context);
    FileInfo folderInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertEquals(opTimeMs, folderInfo.getLastModificationTimeMs());
    Assert.assertEquals(opTimeMs, folderInfo.getLastAccessTimeMs());
  }

  /**
   * Tests that deleting a file from a folder updates the folder's last modification time.
   */
  @Test
  public void lastModificationTimeDelete() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryContext.defaults());
    mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"), CreateFileContext.defaults());
    long folderId = mFsMaster.getFileId(new AlluxioURI("/testFolder"));
    long modificationTimeBeforeDelete = mFsMaster.getFileInfo(folderId).getLastModificationTimeMs();
    long accessTimeBeforeDelete = mFsMaster.getFileInfo(folderId).getLastAccessTimeMs();
    CommonUtils.sleepMs(2);
    mFsMaster.delete(new AlluxioURI("/testFolder/testFile"),
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
    long modificationTimeAfterDelete = mFsMaster.getFileInfo(folderId).getLastModificationTimeMs();
    long accessTimeAfterDelete = mFsMaster.getFileInfo(folderId).getLastAccessTimeMs();
    Assert.assertTrue(modificationTimeBeforeDelete < modificationTimeAfterDelete);
    Assert.assertTrue(accessTimeBeforeDelete < accessTimeAfterDelete);
  }

  @Test
  public void lastModificationTimeRename() throws Exception {
    AlluxioURI srcPath = new AlluxioURI("/testFolder/testFile1");
    AlluxioURI dstPath = new AlluxioURI("/testFolder/testFile2");
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryContext.defaults());
    mFsMaster.createFile(srcPath, CreateFileContext.defaults());
    mFsMaster.rename(srcPath, dstPath, RenameContext.defaults().setOperationTimeMs(TEST_TIME_MS));
    FileInfo folderInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertEquals(TEST_TIME_MS, folderInfo.getLastModificationTimeMs());
    Assert.assertEquals(TEST_TIME_MS, folderInfo.getLastAccessTimeMs());
  }

  @Test
  public void listFiles() throws Exception {
    CreateFileContext options =
        CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder().setBlockSizeBytes(64));

    HashSet<Long> ids = new HashSet<>();
    HashSet<Long> dirIds = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      AlluxioURI dir = new AlluxioURI("/i" + i);
      mFsMaster.createDirectory(dir, CreateDirectoryContext.defaults());
      dirIds.add(mFsMaster.getFileId(dir));
      for (int j = 0; j < 10; j++) {
        ids.add(mFsMaster.createFile(dir.join("j" + j), options).getFileId());
      }
    }
    HashSet<Long> listedIds = new HashSet<>();
    HashSet<Long> listedDirIds = new HashSet<>();
    List<FileInfo> infoList =
        mFsMaster.listStatus(new AlluxioURI("/"), ListStatusContext.defaults());
    for (FileInfo info : infoList) {
      long id = info.getFileId();
      listedDirIds.add(id);
      for (FileInfo fileInfo : mFsMaster.listStatus(new AlluxioURI(info.getPath()),
          ListStatusContext.defaults())) {
        listedIds.add(fileInfo.getFileId());
      }
    }
    Assert.assertEquals(ids, listedIds);
    Assert.assertEquals(dirIds, listedDirIds);
  }

  @Test
  public void listStatus() throws Exception {
    CreateFileContext options =
        CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder().setBlockSizeBytes(64));

    for (int i = 0; i < 10; i++) {
      mFsMaster.createDirectory(new AlluxioURI("/i" + i), CreateDirectoryContext.defaults());
      for (int j = 0; j < 10; j++) {
        mFsMaster.createFile(new AlluxioURI("/i" + i + "/j" + j), options);
      }
    }

    Assert.assertEquals(1,
        mFsMaster
            .listStatus(new AlluxioURI("/i0/j0"), ListStatusContext.defaults())
            .size());
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(10,
          mFsMaster
              .listStatus(new AlluxioURI("/i" + i), ListStatusContext.defaults())
              .size());
    }
    Assert.assertEquals(10, mFsMaster
        .listStatus(new AlluxioURI("/"), ListStatusContext.defaults()).size());
  }

  @Test
  public void notFileCompletion() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mFsMaster.createDirectory(new AlluxioURI("/testFile"), CreateDirectoryContext.defaults());
    mFsMaster.completeFile(new AlluxioURI("/testFile"), CompleteFileContext.defaults());
  }

  @Test
  public void renameExistingDst() throws Exception {
    mFsMaster.createFile(new AlluxioURI("/testFile1"), CreateFileContext.defaults());
    mFsMaster.createFile(new AlluxioURI("/testFile2"), CreateFileContext.defaults());
    try {
      mFsMaster.rename(new AlluxioURI("/testFile1"), new AlluxioURI("/testFile2"),
          RenameContext.defaults());
      Assert.fail("Should not be able to rename to an existing file");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void renameNonexistent() throws Exception {
    mFsMaster.createFile(new AlluxioURI("/testFile1"), CreateFileContext.defaults());
    Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFsMaster.getFileId(new AlluxioURI("/testFile2")));
  }

  @Test
  public void renameToDeeper() throws Exception {
    CreateFileContext createFileOptions =
        CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder().setRecursive(true));
    CreateDirectoryContext createDirectoryContext = CreateDirectoryContext
        .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true));
    mThrown.expect(InvalidPathException.class);
    mFsMaster.createDirectory(new AlluxioURI("/testDir1/testDir2"), createDirectoryContext);
    mFsMaster.createFile(new AlluxioURI("/testDir1/testDir2/testDir3/testFile3"),
        createFileOptions);
    mFsMaster.rename(new AlluxioURI("/testDir1/testDir2"),
        new AlluxioURI("/testDir1/testDir2/testDir3/testDir4"), RenameContext.defaults());
  }

  @Test
  public void ttlCreateFile() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryContext.defaults());
    long ttl = 100;
    CreateFileContext context = CreateFileContext
        .mergeFrom(CreateFilePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setTtl(ttl).setTtlAction(alluxio.grpc.TtlAction.FREE)));
    mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"), context);
    FileInfo folderInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    Assert.assertEquals(ttl, folderInfo.getTtl());
    Assert.assertEquals(TtlAction.FREE, folderInfo.getTtlAction());
  }

  @Test
  public void ttlExpiredCreateFile() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryContext.defaults());
    long ttl = 1;
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(ttl)));
    long fileId =
        mFsMaster.createFile(new AlluxioURI("/testFolder/testFile1"), context).getFileId();
    FileInfo folderInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile1")));
    Assert.assertEquals(fileId, folderInfo.getFileId());
    Assert.assertEquals(ttl, folderInfo.getTtl());
    // Sleep for the ttl expiration.
    CommonUtils.sleepMs(2 * TTL_CHECKER_INTERVAL_MS);
    HeartbeatScheduler.execute(HeartbeatContext.MASTER_TTL_CHECK);
    mThrown.expect(FileDoesNotExistException.class);
    mFsMaster.getFileInfo(fileId);
  }

  @Test
  public void ttlExpiredCreateFileWithFreeAction() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryContext.defaults());
    long ttl = 1;
    CreateFileContext context = CreateFileContext
        .mergeFrom(CreateFilePOptions.newBuilder().setCommonOptions(FileSystemMasterCommonPOptions
            .newBuilder().setTtl(ttl).setTtlAction(alluxio.grpc.TtlAction.FREE)))
        .setWriteType(WriteType.CACHE_THROUGH);
    long fileId =
        mFsMaster.createFile(new AlluxioURI("/testFolder/testFile1"), context).getFileId();
    FileInfo folderInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile1")));
    Assert.assertEquals(fileId, folderInfo.getFileId());
    Assert.assertEquals(ttl, folderInfo.getTtl());
    Assert.assertEquals(TtlAction.FREE, folderInfo.getTtlAction());
    // Sleep for the ttl expiration.
    CommonUtils.sleepMs(2 * TTL_CHECKER_INTERVAL_MS);
    HeartbeatScheduler.await(HeartbeatContext.MASTER_TTL_CHECK, 10, TimeUnit.SECONDS);
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_TTL_CHECK);
    HeartbeatScheduler.await(HeartbeatContext.MASTER_TTL_CHECK, 10, TimeUnit.SECONDS);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals(Constants.NO_TTL, fileInfo.getTtl());
    Assert.assertEquals(TtlAction.DELETE, fileInfo.getTtlAction());
  }

  @Test
  public void ttlRename() throws Exception {
    AlluxioURI srcPath = new AlluxioURI("/testFolder/testFile1");
    AlluxioURI dstPath = new AlluxioURI("/testFolder/testFile2");
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryContext.defaults());
    long ttl = 1;
    CreateFileContext context = CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
        .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(ttl)));
    mFsMaster.createFile(srcPath, context);
    mFsMaster.rename(srcPath, dstPath, RenameContext.defaults().setOperationTimeMs(TEST_TIME_MS));
    FileInfo folderInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile2")));
    Assert.assertEquals(ttl, folderInfo.getTtl());
  }

  @Test
  public void ufsModeCreateFile() throws Exception {
    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.READ_ONLY);

    // Alluxio only should not be affected
    mFsMaster.createFile(new AlluxioURI("/in_alluxio"),
        CreateFileContext.defaults().setWriteType(WriteType.MUST_CACHE));
    // Ufs file creation should throw an exception
    mThrown.expect(AccessControlException.class);
    mFsMaster.createFile(new AlluxioURI("/in_ufs"),
        CreateFileContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
  }

  @Test
  public void ufsModeCreateDirectory() throws Exception {
    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.READ_ONLY);

    // Alluxio only should not be affected
    mFsMaster.createDirectory(new AlluxioURI("/in_alluxio"),
        CreateDirectoryContext.defaults().setWriteType(WriteType.MUST_CACHE));
    // Ufs file creation should throw an exception
    mThrown.expect(AccessControlException.class);
    mFsMaster.createDirectory(new AlluxioURI("/in_ufs"),
        CreateDirectoryContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
  }

  @Test
  public void ufsModeDeleteFile() throws Exception {
    AlluxioURI alluxioFile = new AlluxioURI("/in_alluxio");
    mFsMaster.createFile(alluxioFile, CreateFileContext.defaults()
        .setWriteType(WriteType.CACHE_THROUGH));

    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.READ_ONLY);

    mThrown.expect(FailedPreconditionException.class);
    mFsMaster.delete(alluxioFile, DeleteContext.defaults());
  }

  @Test
  public void ufsModeDeleteDirectory() throws Exception {
    AlluxioURI alluxioDirectory = new AlluxioURI("/in_ufs_dir");
    mFsMaster.createDirectory(alluxioDirectory,
        CreateDirectoryContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    AlluxioURI alluxioFile = new AlluxioURI("/in_ufs_dir/in_ufs_file");
    mFsMaster.createFile(alluxioFile, CreateFileContext.defaults()
        .setWriteType(WriteType.CACHE_THROUGH));

    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.READ_ONLY);

    mThrown.expect(FailedPreconditionException.class);
    mFsMaster.delete(alluxioDirectory,
        DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));

    // Check Alluxio entries exist after failed delete
    long dirId = mFsMaster.getFileId(alluxioDirectory);
    Assert.assertNotEquals(-1, dirId);
    long fileId = mFsMaster.getFileId(alluxioFile);
    Assert.assertNotEquals(-1, fileId);
  }

  @Test
  public void ufsModeRenameFile() throws Exception {
    mFsMaster.createFile(new AlluxioURI("/in_ufs_src"),
        CreateFileContext.defaults().setWriteType(WriteType.CACHE_THROUGH));

    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.READ_ONLY);

    mThrown.expect(AccessControlException.class);
    mFsMaster.rename(new AlluxioURI("/in_ufs_src"), new AlluxioURI("/in_ufs_dst"),
        RenameContext.defaults());
  }

  @Test
  public void ufsModeRenameDirectory() throws Exception {
    AlluxioURI alluxioDirectory = new AlluxioURI("/in_ufs_dir");
    mFsMaster.createDirectory(alluxioDirectory,
        CreateDirectoryContext.defaults().setWriteType(WriteType.CACHE_THROUGH));
    AlluxioURI alluxioFile = new AlluxioURI("/in_ufs_dir/in_ufs_file");
    mFsMaster.createFile(alluxioFile, CreateFileContext.defaults()
        .setWriteType(WriteType.CACHE_THROUGH));

    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.READ_ONLY);

    mThrown.expect(AccessControlException.class);
    mFsMaster.rename(alluxioDirectory, new AlluxioURI("/in_ufs_dst"), RenameContext.defaults());

    // Check Alluxio entries exist after failed rename
    long dirId = mFsMaster.getFileId(alluxioDirectory);
    Assert.assertNotEquals(-1, dirId);
    long fileId = mFsMaster.getFileId(alluxioFile);
    Assert.assertNotEquals(-1, fileId);
  }

  @Test
  public void ufsModeSetAttribute() throws Exception {
    AlluxioURI alluxioFile = new AlluxioURI("/in_alluxio");
    mFsMaster.createFile(alluxioFile, CreateFileContext.defaults()
        .setWriteType(WriteType.CACHE_THROUGH));

    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.READ_ONLY);
    long opTimeMs = TEST_TIME_MS;
    mFsMaster.completeFile(alluxioFile, CompleteFileContext
        .mergeFrom(CompleteFilePOptions.newBuilder().setUfsLength(0))
        .setOperationTimeMs(opTimeMs));

    mThrown.expect(AccessControlException.class);
    mFsMaster.setAttribute(alluxioFile, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
  }

  @Test
  public void setModeOwnerNoWritePermission() throws Exception {
    AlluxioURI root = new AlluxioURI("/");
    mFsMaster.setAttribute(root, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    try (AutoCloseable closeable =
             new AuthenticatedUserRule("foo", ServerConfiguration.global()).toResource()) {
      AlluxioURI alluxioFile = new AlluxioURI("/in_alluxio");
      FileInfo file = mFsMaster.createFile(alluxioFile, CreateFileContext.defaults());

      long opTimeMs = TEST_TIME_MS;
      mFsMaster.completeFile(alluxioFile, CompleteFileContext
          .mergeFrom(CompleteFilePOptions.newBuilder().setUfsLength(0))
          .setOperationTimeMs(opTimeMs));
      mFsMaster.setAttribute(alluxioFile, SetAttributeContext
          .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0407).toProto())));
      Assert.assertEquals(0407, mFsMaster.getFileInfo(file.getFileId()).getMode());
      mFsMaster.setAttribute(alluxioFile, SetAttributeContext
          .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
      Assert.assertEquals(0777, mFsMaster.getFileInfo(file.getFileId()).getMode());
    }
  }

  @Test
  public void setModeNoOwner() throws Exception {
    AlluxioURI root = new AlluxioURI("/");
    mFsMaster.setAttribute(root, SetAttributeContext
        .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
    AlluxioURI alluxioFile = new AlluxioURI("/in_alluxio");
    try (AutoCloseable closeable =
             new AuthenticatedUserRule("foo", ServerConfiguration.global()).toResource()) {
      FileInfo file = mFsMaster.createFile(alluxioFile, CreateFileContext.defaults());

      long opTimeMs = TEST_TIME_MS;
      mFsMaster.completeFile(alluxioFile, CompleteFileContext
          .mergeFrom(CompleteFilePOptions.newBuilder().setUfsLength(0))
          .setOperationTimeMs(opTimeMs));
      mFsMaster.setAttribute(alluxioFile, SetAttributeContext
          .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0777).toProto())));
      Assert.assertEquals(0777, mFsMaster.getFileInfo(file.getFileId()).getMode());
    }
    mThrown.expect(AccessControlException.class);
    try (AutoCloseable closeable =
             new AuthenticatedUserRule("bar", ServerConfiguration.global()).toResource()) {
      mFsMaster.setAttribute(alluxioFile, SetAttributeContext
          .mergeFrom(SetAttributePOptions.newBuilder().setMode(new Mode((short) 0677).toProto())));
    }
  }

  /**
   * Tests creating a directory in a nested directory load the UFS status of Inodes on the path.
   */
  @Test
  public void createDirectoryInNestedDirectories() throws Exception {
    String ufs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    String targetPath = Paths.get(ufs, "d1", "d2", "d3").toString();
    FileUtils.createDir(targetPath);
    FileUtils.changeLocalFilePermission(targetPath, new Mode((short) 0755).toString());
    String parentPath = Paths.get(ufs, "d1").toString();
    FileUtils.changeLocalFilePermission(parentPath, new Mode((short) 0700).toString());
    AlluxioURI path = new AlluxioURI(Paths.get("/d1", "d2", "d3", "d4").toString());

    mFsMaster.createDirectory(path,
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)
            .setMode(new Mode((short) 0755).toProto())).setWriteType(WriteType.CACHE_THROUGH));

    long fileId = mFsMaster.getFileId(new AlluxioURI("/d1"));
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("d1", fileInfo.getName());
    Assert.assertTrue(fileInfo.isFolder());
    Assert.assertTrue(fileInfo.isPersisted());
    Assert.assertEquals(0700, (short) fileInfo.getMode());
  }

  /**
   * Tests listing a directory in a nested directory load the UFS status of Inodes on the path.
   */
  @Test
  public void loadMetadataInNestedDirectories() throws Exception {
    String ufs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    String targetPath = Paths.get(ufs, "d1", "d2", "d3").toString();
    FileUtils.createDir(targetPath);
    FileUtils.changeLocalFilePermission(targetPath, new Mode((short) 0755).toString());
    String parentPath = Paths.get(ufs, "d1").toString();
    FileUtils.changeLocalFilePermission(parentPath, new Mode((short) 0700).toString());
    AlluxioURI path = new AlluxioURI(Paths.get("/d1", "d2", "d3").toString());

    mFsMaster.listStatus(path, ListStatusContext
        .mergeFrom(ListStatusPOptions.newBuilder().setLoadMetadataType(LoadMetadataPType.ONCE)));

    long fileId = mFsMaster.getFileId(new AlluxioURI("/d1"));
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("d1", fileInfo.getName());
    Assert.assertTrue(fileInfo.isFolder());
    Assert.assertTrue(fileInfo.isPersisted());
    Assert.assertEquals(0700, (short) fileInfo.getMode());
  }

  /**
   * Tests creating nested directories.
   */
  @Test
  public void createNestedDirectories() throws Exception {
    String ufs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    String parentPath = Paths.get(ufs, "d1").toString();
    FileUtils.createDir(parentPath);
    FileUtils.changeLocalFilePermission(parentPath, new Mode((short) 0755).toString());
    AlluxioURI path = new AlluxioURI(Paths.get("/d1", "d2", "d3", "d4").toString());
    String ufsPath = Paths.get(ufs, "d1", "d2", "d3", "d4").toString();
    mFsMaster.createDirectory(path,
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)
            .setMode(new Mode((short) 0700).toProto())).setWriteType(WriteType.CACHE_THROUGH));
    long fileId = mFsMaster.getFileId(path);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("d4", fileInfo.getName());
    Assert.assertTrue(fileInfo.isFolder());
    Assert.assertTrue(fileInfo.isPersisted());
    Assert.assertEquals(0700, (short) fileInfo.getMode());
    Assert.assertTrue(FileUtils.exists(ufsPath));
    Assert.assertEquals(0700, FileUtils.getLocalFileMode(ufsPath));
  }

  /**
   * Tests creating a directory in a nested directory without execute permission.
   */
  @Test(expected = IOException.class)
  public void createDirectoryInNestedDirectoriesWithoutExecutePermission() throws Exception {
    // Assume the user is not root. This test doesn't work as root because root *is* allowed to
    // create subdirectories even without execute permission on the parent directory.
    assumeFalse(ShellUtils.execCommand("id", "-u").trim().equals("0"));
    String ufs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    String parentPath = Paths.get(ufs, "d1").toString();
    FileUtils.createDir(parentPath);
    FileUtils.changeLocalFilePermission(parentPath, new Mode((short) 0600).toString());
    AlluxioURI path = new AlluxioURI(Paths.get("/d1", "d2", "d3", "d4").toString());

    // this should fail
    mFsMaster.createDirectory(path,
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)
            .setMode(new Mode((short) 0755).toProto())).setWriteType(WriteType.CACHE_THROUGH));
  }

  /**
   * Tests loading ufs last modified time for directories.
   */
  @Test
  public void loadDirectoryTimestamps() throws Exception {
    String name = "d1";
    String ufs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    String ufsPath = Paths.get(ufs, name).toString();
    FileUtils.createDir(ufsPath);
    File file = new File(ufsPath);
    long ufsTime = file.lastModified();
    Thread.sleep(100);
    AlluxioURI path = new AlluxioURI("/" + name);

    long fileId = mFsMaster.getFileId(path);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    long alluxioTime = fileInfo.getLastModificationTimeMs();

    Assert.assertEquals(name, fileInfo.getName());
    Assert.assertTrue(fileInfo.isFolder());
    Assert.assertTrue(fileInfo.isPersisted());
    Assert.assertEquals(ufsTime, alluxioTime);
  }

  /**
   * Tests loading ufs last modified time for files.
   */
  @Test
  public void loadFileTimestamps() throws Exception {
    String name = "f1";
    String ufs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    String ufsPath = Paths.get(ufs, name).toString();
    FileUtils.createFile(ufsPath);
    File file = new File(ufsPath);
    long actualTime = file.lastModified();
    Thread.sleep(100);
    AlluxioURI path = new AlluxioURI("/" + name);

    long fileId = mFsMaster.getFileId(path);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    long alluxioTime = fileInfo.getLastModificationTimeMs();

    Assert.assertEquals(name, fileInfo.getName());
    Assert.assertFalse(fileInfo.isFolder());
    Assert.assertTrue(fileInfo.isPersisted());
    Assert.assertEquals(actualTime, alluxioTime);
  }

  /**
   * Tests loading ufs last modified time for parent directories.
   */
  @Test
  public void loadParentDirectoryTimestamps() throws Exception {
    String parentName = "d1";
    String childName = "d2";
    String ufs = ServerConfiguration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    String parentUfsPath = Paths.get(ufs, parentName).toString();
    FileUtils.createDir(parentUfsPath);
    File file = new File(parentUfsPath);
    long actualTime = file.lastModified();
    Thread.sleep(100);
    AlluxioURI parentPath = new AlluxioURI("/" + parentName);
    AlluxioURI path = new AlluxioURI("/" + Paths.get(parentName, childName));
    mFsMaster.createDirectory(path,
        CreateDirectoryContext.mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true)
            .setMode(new Mode((short) 0700).toProto())).setWriteType(WriteType.CACHE_THROUGH));
    long fileId = mFsMaster.getFileId(path);

    // calls getFileInfo on child to load metadata of parent
    mFsMaster.getFileInfo(fileId);
    long parentId = mFsMaster.getFileId(parentPath);
    FileInfo parentInfo = mFsMaster.getFileInfo(parentId);
    long alluxioTime = parentInfo.getLastModificationTimeMs();

    Assert.assertEquals(parentName, parentInfo.getName());
    Assert.assertTrue(parentInfo.isFolder());
    Assert.assertTrue(parentInfo.isPersisted());
    Assert.assertEquals(actualTime, alluxioTime);
    Assert.assertTrue(FileUtils.exists(parentUfsPath));
  }

  /**
   * This class provides multiple concurrent threads to create all files in one directory.
   */
  class ConcurrentCreator implements Callable<Void> {
    private int mDepth;
    private int mConcurrencyDepth;
    private AlluxioURI mInitPath;
    private CreateFileContext mCreateFileContext;

    /**
     * Constructs the concurrent creator.
     *
     * @param depth the depth of files to be created in one directory
     * @param concurrencyDepth the concurrency depth of files to be created in one directory
     * @param initPath the directory of files to be created in
     */
    ConcurrentCreator(int depth, int concurrencyDepth, AlluxioURI initPath) {
      this(depth, concurrencyDepth, initPath, CreateFileContext.defaults());
    }

    /**
     * Constructs the concurrent creator.
     *
     * @param depth the depth of files to be created in one directory
     * @param concurrencyDepth the concurrency depth of files to be created in one directory
     * @param initPath the directory of files to be created in
     * @param context method context
     */
    ConcurrentCreator(int depth, int concurrencyDepth, AlluxioURI initPath,
        CreateFileContext context) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mInitPath = initPath;
      mCreateFileContext = context;
    }

    /**
     * Authenticates the client user named TEST_USER and executes the process of creating all
     * files in one directory by multiple concurrent threads.
     *
     * @return null
     */
    @Override
    @Nullable
    public Void call() throws Exception {
      AuthenticatedClientUser.set(TEST_USER);
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

    /**
     * Executes the process of creating all files in one directory by multiple concurrent threads.
     *
     * @param depth the depth of files to be created in one directory
     * @param concurrencyDepth the concurrency depth of files to be created in one directory
     * @param path the directory of files to be created in
     */
    public void exec(int depth, int concurrencyDepth, AlluxioURI path) throws Exception {
      if (depth < 1) {
        return;
      } else if (depth == 1) {
        long fileId = mFsMaster.createFile(path, mCreateFileContext).getFileId();
        Assert.assertEquals(fileId, mFsMaster.getFileId(path));
        // verify the user permission for file
        FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
        Assert.assertEquals(TEST_USER, fileInfo.getOwner());
        Assert.assertEquals(0644, (short) fileInfo.getMode());
      } else {
        mFsMaster.createDirectory(path, CreateDirectoryContext.defaults());
        Assert.assertNotNull(mFsMaster.getFileId(path));
        long dirId = mFsMaster.getFileId(path);
        Assert.assertNotEquals(-1, dirId);
        FileInfo dirInfo = mFsMaster.getFileInfo(dirId);
        Assert.assertEquals(TEST_USER, dirInfo.getOwner());
        Assert.assertEquals(0755, (short) dirInfo.getMode());
      }

      if (concurrencyDepth > 0) {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
          ArrayList<Future<Void>> futures = new ArrayList<>(FILES_PER_NODE);
          for (int i = 0; i < FILES_PER_NODE; i++) {
            Callable<Void> call = (new ConcurrentCreator(depth - 1, concurrencyDepth - 1,
                path.join(Integer.toString(i)), mCreateFileContext));
            futures.add(executor.submit(call));
          }
          for (Future<Void> f : futures) {
            f.get();
          }
        } finally {
          executor.shutdown();
        }
      } else {
        for (int i = 0; i < FILES_PER_NODE; i++) {
          exec(depth - 1, concurrencyDepth, path.join(Integer.toString(i)));
        }
      }
    }
  }

  /**
   * This class provides multiple concurrent threads to free all files in one directory.
   */
  class ConcurrentFreer implements Callable<Void> {
    private int mDepth;
    private int mConcurrencyDepth;
    private AlluxioURI mInitPath;

    /**
     * Constructs the concurrent freer.
     *
     * @param depth the depth of files to be freed
     * @param concurrencyDepth the concurrency depth of files to be freed
     * @param initPath the directory of files to be freed
     */
    ConcurrentFreer(int depth, int concurrencyDepth, AlluxioURI initPath) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mInitPath = initPath;
    }

    @Override
    @Nullable
    public Void call() throws Exception {
      AuthenticatedClientUser.set(TEST_USER);
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

    private void doFree(AlluxioURI path) throws Exception {
      mFsMaster.free(path,
          FreeContext.mergeFrom(FreePOptions.newBuilder().setForced(true).setRecursive(true)));
      Assert.assertNotEquals(IdUtils.INVALID_FILE_ID, mFsMaster.getFileId(path));
    }

    /**
     * Executes the process of freeing all files in one directory by multiple concurrent threads.
     *
     * @param depth the depth of files to be freed in one directory
     * @param concurrencyDepth the concurrency depth of files to be freed in one directory
     * @param path the directory of files to be freed in
     */
    public void exec(int depth, int concurrencyDepth, AlluxioURI path) throws Exception {
      if (depth < 1) {
        return;
      } else if (depth == 1 || (path.hashCode() % 10 == 0)) {
        // Sometimes we want to try freeing a path when we're not all the way down, which is what
        // the second condition is for.
        doFree(path);
      } else {
        if (concurrencyDepth > 0) {
          ExecutorService executor = Executors.newCachedThreadPool();
          try {
            ArrayList<Future<Void>> futures = new ArrayList<>(FILES_PER_NODE);
            for (int i = 0; i < FILES_PER_NODE; i++) {
              Callable<Void> call = (new ConcurrentDeleter(depth - 1, concurrencyDepth - 1,
                  path.join(Integer.toString(i))));
              futures.add(executor.submit(call));
            }
            for (Future<Void> f : futures) {
              f.get();
            }
          } finally {
            executor.shutdown();
          }
        } else {
          for (int i = 0; i < FILES_PER_NODE; i++) {
            exec(depth - 1, concurrencyDepth, path.join(Integer.toString(i)));
          }
        }
        doFree(path);
      }
    }
  }

  /**
   * This class provides multiple concurrent threads to delete all files in one directory.
   */
  class ConcurrentDeleter implements Callable<Void> {
    private int mDepth;
    private int mConcurrencyDepth;
    private AlluxioURI mInitPath;

    /**
     * Constructs the concurrent deleter.
     *
     * @param depth the depth of files to be deleted in one directory
     * @param concurrencyDepth the concurrency depth of files to be deleted in one directory
     * @param initPath the directory of files to be deleted in
     */
    ConcurrentDeleter(int depth, int concurrencyDepth, AlluxioURI initPath) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mInitPath = initPath;
    }

    @Override
    @Nullable
    public Void call() throws Exception {
      AuthenticatedClientUser.set(TEST_USER);
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

    private void doDelete(AlluxioURI path) throws Exception {
      mFsMaster.delete(path,
          DeleteContext.mergeFrom(DeletePOptions.newBuilder().setRecursive(true)));
      Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFsMaster.getFileId(path));
    }

    /**
     * Executes the process of deleting all files in one directory by multiple concurrent threads.
     *
     * @param depth the depth of files to be deleted in one directory
     * @param concurrencyDepth the concurrency depth of files to be deleted in one directory
     * @param path the directory of files to be deleted in
     */
    public void exec(int depth, int concurrencyDepth, AlluxioURI path) throws Exception {
      if (depth < 1) {
        return;
      } else if (depth == 1 || (path.hashCode() % 10 == 0)) {
        // Sometimes we want to try deleting a path when we're not all the way down, which is what
        // the second condition is for.
        doDelete(path);
      } else {
        if (concurrencyDepth > 0) {
          ExecutorService executor = Executors.newCachedThreadPool();
          try {
            ArrayList<Future<Void>> futures = new ArrayList<>(FILES_PER_NODE);
            for (int i = 0; i < FILES_PER_NODE; i++) {
              Callable<Void> call = (new ConcurrentDeleter(depth - 1, concurrencyDepth - 1,
                  path.join(Integer.toString(i))));
              futures.add(executor.submit(call));
            }
            for (Future<Void> f : futures) {
              f.get();
            }
          } finally {
            executor.shutdown();
          }
        } else {
          for (int i = 0; i < FILES_PER_NODE; i++) {
            exec(depth - 1, concurrencyDepth, path.join(Integer.toString(i)));
          }
        }
        doDelete(path);
      }
    }
  }

  /**
   * This class runs multiple concurrent threads to rename all files in one directory.
   */
  class ConcurrentRenamer implements Callable<Void> {
    private int mDepth;
    private int mConcurrencyDepth;
    private AlluxioURI mRootPath;
    private AlluxioURI mRootPath2;
    private AlluxioURI mInitPath;

    /**
     * Constructs the concurrent renamer.
     *
     * @param depth the depth of files to be renamed in one directory
     * @param concurrencyDepth the concurrency depth of files to be renamed in one directory
     * @param rootPath  the source root path of the files to be renamed
     * @param rootPath2 the destination root path of the files to be renamed
     * @param initPath the directory of files to be renamed in under root path
     */
    ConcurrentRenamer(int depth, int concurrencyDepth, AlluxioURI rootPath, AlluxioURI rootPath2,
        AlluxioURI initPath) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mRootPath = rootPath;
      mRootPath2 = rootPath2;
      mInitPath = initPath;
    }

    @Override
    @Nullable
    public Void call() throws Exception {
      AuthenticatedClientUser.set(TEST_USER);
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

    /**
     * Renames all files in one directory using multiple concurrent threads.
     *
     * @param depth the depth of files to be renamed in one directory
     * @param concurrencyDepth the concurrency depth of files to be renamed in one directory
     * @param path the directory of files to be renamed in under root path
     */
    public void exec(int depth, int concurrencyDepth, AlluxioURI path) throws Exception {
      if (depth < 1) {
        return;
      } else if (depth == 1 || (depth < mDepth && path.hashCode() % 10 < 3)) {
        // Sometimes we want to try renaming a path when we're not all the way down, which is what
        // the second condition is for. We have to create the path in the destination up till what
        // we're renaming. This might already exist, so createFile could throw a
        // FileAlreadyExistsException, which we silently handle.
        AlluxioURI srcPath = mRootPath.join(path);
        AlluxioURI dstPath = mRootPath2.join(path);
        long fileId = mFsMaster.getFileId(srcPath);
        try {
          CreateDirectoryContext context = CreateDirectoryContext
              .mergeFrom(CreateDirectoryPOptions.newBuilder().setRecursive(true));
          mFsMaster.createDirectory(dstPath.getParent(), context);
        } catch (FileAlreadyExistsException | InvalidPathException e) {
          // FileAlreadyExistsException: This is an acceptable exception to get, since we
          // don't know if the parent has been created yet by another thread.
          // InvalidPathException: This could happen if we are renaming something that's a child of
          // the root.
        }
        mFsMaster.rename(srcPath, dstPath, RenameContext.defaults());
        Assert.assertEquals(fileId, mFsMaster.getFileId(dstPath));
      } else if (concurrencyDepth > 0) {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
          ArrayList<Future<Void>> futures = new ArrayList<>(FILES_PER_NODE);
          for (int i = 0; i < FILES_PER_NODE; i++) {
            Callable<Void> call = (new ConcurrentRenamer(depth - 1, concurrencyDepth - 1, mRootPath,
                mRootPath2, path.join(Integer.toString(i))));
            futures.add(executor.submit(call));
          }
          for (Future<Void> f : futures) {
            f.get();
          }
        } finally {
          executor.shutdown();
        }
      } else {
        for (int i = 0; i < FILES_PER_NODE; i++) {
          exec(depth - 1, concurrencyDepth, path.join(Integer.toString(i)));
        }
      }
    }
  }
}
