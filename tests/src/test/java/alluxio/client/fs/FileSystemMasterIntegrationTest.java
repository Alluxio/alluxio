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
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.PropertyKey.Name;
import alluxio.client.WriteType;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.exception.AccessControlException;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyCompletedException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.status.FailedPreconditionException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.MasterClientConfig;
import alluxio.master.MasterRegistry;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.meta.TtlIntervalRule;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.DeleteOptions;
import alluxio.master.file.options.FreeOptions;
import alluxio.master.file.options.GetStatusOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.master.file.options.MountOptions;
import alluxio.master.file.options.RenameOptions;
import alluxio.master.file.options.SetAttributeOptions;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.security.authorization.Mode;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.testutils.master.MasterTestUtils;
import alluxio.underfs.UfsDirectoryStatus;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemFactory;
import alluxio.underfs.UnderFileSystemFactoryRegistry;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.util.ShellUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.file.options.CommonOptions;
import alluxio.wire.FileInfo;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.TtlAction;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

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
  private static final long TTL_CHECKER_INTERVAL_MS = 1000;
  private static final String TEST_USER = "test";
  // Time to wait for shutting down thread pool.
  private static final long SHUTDOWN_TIME_MS = 15 * Constants.SECOND_MS;

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
          .setProperty(PropertyKey.WORKER_MEMORY_SIZE, 1000)
          .setProperty(PropertyKey.SECURITY_LOGIN_USERNAME, TEST_USER).build();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Rule
  public AuthenticatedUserRule mAuthenticatedUser = new AuthenticatedUserRule(TEST_USER);

  private FileSystemMaster mFsMaster;

  @Before
  public final void before() throws Exception {
    mFsMaster = mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
        .getMaster(FileSystemMaster.class);
  }

  /**
   * Tests the {@link FileInfo} of a directory.
   */
  @Test
  public void clientFileInfoDirectory() throws Exception {
    AlluxioURI path = new AlluxioURI("/testFolder");
    mFsMaster.createDirectory(path, CreateDirectoryOptions.defaults());
    long fileId = mFsMaster.getFileId(path);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("testFolder", fileInfo.getName());
    Assert.assertEquals(1, fileInfo.getFileId());
    Assert.assertEquals(0, fileInfo.getLength());
    assertFalse(fileInfo.isCacheable());
    Assert.assertTrue(fileInfo.isCompleted());
    Assert.assertTrue(fileInfo.isFolder());
    assertFalse(fileInfo.isPersisted());
    assertFalse(fileInfo.isPinned());
    Assert.assertEquals("", fileInfo.getOwner());
    Assert.assertEquals(0755, (short) fileInfo.getMode());
  }

  /**
   * Tests the {@link FileInfo} of an empty file.
   */
  @Test
  public void clientFileInfoEmptyFile() throws Exception {
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileOptions.defaults());
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("testFile", fileInfo.getName());
    Assert.assertEquals(fileId, fileInfo.getFileId());
    Assert.assertEquals(0, fileInfo.getLength());
    Assert.assertTrue(fileInfo.isCacheable());
    assertFalse(fileInfo.isCompleted());
    assertFalse(fileInfo.isFolder());
    assertFalse(fileInfo.isPersisted());
    assertFalse(fileInfo.isPinned());
    Assert.assertEquals(Constants.NO_TTL, fileInfo.getTtl());
    Assert.assertEquals(TtlAction.DELETE, fileInfo.getTtlAction());
    Assert.assertEquals("", fileInfo.getOwner());
    Assert.assertEquals(0644, (short) fileInfo.getMode());
  }

  private MasterRegistry createFileSystemMasterFromJournal() throws Exception {
    return MasterTestUtils.createLeaderFileSystemMasterFromJournal();
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

      MasterRegistry registry = createFileSystemMasterFromJournal();
      FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);
      for (FileInfo info : mFsMaster.listStatus(new AlluxioURI("/"),
          ListStatusOptions.defaults())) {
        AlluxioURI path = new AlluxioURI(info.getPath());
        Assert.assertEquals(mFsMaster.getFileId(path), fsMaster.getFileId(path));
      }
      registry.stop();
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

    Assert.assertEquals(0,
        mFsMaster.listStatus(new AlluxioURI("/"), ListStatusOptions.defaults()).size());
  }

  /**
   * Tests concurrent free of files.
   */
  @Test
  public void concurrentFree() throws Exception {
    ConcurrentCreator concurrentCreator =
        new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH,
            CreateFileOptions.defaults().setPersisted(true));
    concurrentCreator.call();

    ConcurrentFreer concurrentFreer = new ConcurrentFreer(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentFreer.call();
  }

  /**
   * Tests concurrent rename of files.
   */
  @Ignore("https://alluxio.atlassian.net/browse/ALLUXIO-2908")
  @Test
  public void concurrentRename() throws Exception {
    ConcurrentCreator concurrentCreator =
        new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentCreator.call();

    int numFiles = mFsMaster.listStatus(ROOT_PATH, ListStatusOptions.defaults()).size();

    ConcurrentRenamer concurrentRenamer = new ConcurrentRenamer(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH,
        ROOT_PATH2, AlluxioURI.EMPTY_URI);
    concurrentRenamer.call();

    Assert.assertEquals(numFiles,
        mFsMaster.listStatus(ROOT_PATH2, ListStatusOptions.defaults()).size());
  }

  @Test
  public void createAlreadyExistFile() throws Exception {
    mThrown.expect(FileAlreadyExistsException.class);
    mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileOptions.defaults());
    mFsMaster.createDirectory(new AlluxioURI("/testFile"), CreateDirectoryOptions.defaults());
  }

  @Test
  public void createDirectory() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    FileInfo fileInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertTrue(fileInfo.isFolder());
    Assert.assertEquals("", fileInfo.getOwner());
    Assert.assertEquals(0755, (short) fileInfo.getMode());
  }

  @Test
  public void createFileInvalidPath() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mFsMaster.createFile(new AlluxioURI("testFile"), CreateFileOptions.defaults());
  }

  @Test
  public void createFileInvalidPathTest2() throws Exception {
    mThrown.expect(FileAlreadyExistsException.class);
    mFsMaster.createFile(new AlluxioURI("/"), CreateFileOptions.defaults());
  }

  @Test
  public void createFileInvalidPathTest3() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mFsMaster.createFile(new AlluxioURI("/testFile1"), CreateFileOptions.defaults());
    mFsMaster.createFile(new AlluxioURI("/testFile1/testFile2"), CreateFileOptions.defaults());
  }

  @Test
  public void createFilePerf() throws Exception {
    for (int k = 0; k < 200; k++) {
      CreateDirectoryOptions options = CreateDirectoryOptions.defaults().setRecursive(true);
      mFsMaster.createDirectory(
          new AlluxioURI("/testFile").join(Constants.MASTER_COLUMN_FILE_PREFIX + k).join("0"),
          options);
    }
    for (int k = 0; k < 200; k++) {
      mFsMaster.getFileInfo(mFsMaster.getFileId(
          new AlluxioURI("/testFile").join(Constants.MASTER_COLUMN_FILE_PREFIX + k).join("0")));
    }
  }

  @Test
  public void createFile() throws Exception {
    mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileOptions.defaults());
    FileInfo fileInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFile")));
    assertFalse(fileInfo.isFolder());
    Assert.assertEquals("", fileInfo.getOwner());
    Assert.assertEquals(0644, (short) fileInfo.getMode());
  }

  @Test
  public void deleteUnsyncedDirectory() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"),
        CreateDirectoryOptions.defaults().setPersisted(true));
    mFsMaster.createDirectory(new AlluxioURI("/testFolder/child"),
        CreateDirectoryOptions.defaults().setPersisted(true));
    String ufs = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Files.createDirectory(Paths.get(ufs, "testFolder", "ufsOnlyDir"));
    try {
      mFsMaster.delete(new AlluxioURI("/testFolder"),
          DeleteOptions.defaults().setUnchecked(false).setRecursive(true));
      Assert.fail("Expected deleting an out of sync directory to fail");
    } catch (IOException e) {
      // Expected
    }
    // Make sure the root folder still exists in Alluxio space.
    mFsMaster.listStatus(new AlluxioURI("/testFolder"),
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never));
    // The child was in sync, so it should be deleted both from Alluxio and the UFS.
    mThrown.expect(FileDoesNotExistException.class);
    mFsMaster.listStatus(new AlluxioURI("/testFolder/child"),
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Always));
  }

  @Test
  public void deleteDirectoryWithDirectories() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    mFsMaster.createDirectory(new AlluxioURI("/testFolder/testFolder2"),
        CreateDirectoryOptions.defaults());
    long fileId =
        mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"), CreateFileOptions.defaults());
    long fileId2 = mFsMaster.createFile(new AlluxioURI("/testFolder/testFolder2/testFile2"),
        CreateFileOptions.defaults());
    Assert.assertEquals(1, mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertEquals(2, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFolder2")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    Assert.assertEquals(fileId2,
        mFsMaster.getFileId(new AlluxioURI("/testFolder/testFolder2/testFile2")));
    mFsMaster.delete(new AlluxioURI("/testFolder"), DeleteOptions.defaults()
        .setRecursive(true));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID,
        mFsMaster.getFileId(new AlluxioURI("/testFolder/testFolder2/testFile2")));
  }

  @Test
  public void deleteDirectoryWithDirectoriesTest2() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    mFsMaster.createDirectory(new AlluxioURI("/testFolder/testFolder2"),
        CreateDirectoryOptions.defaults());
    long fileId =
        mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"), CreateFileOptions.defaults());
    long fileId2 = mFsMaster.createFile(new AlluxioURI("/testFolder/testFolder2/testFile2"),
        CreateFileOptions.defaults());
    Assert.assertEquals(1, mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertEquals(2, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFolder2")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    Assert.assertEquals(fileId2,
        mFsMaster.getFileId(new AlluxioURI("/testFolder/testFolder2/testFile2")));
    try {
      mFsMaster.delete(new AlluxioURI("/testFolder/testFolder2"), DeleteOptions.defaults()
          .setRecursive(false));
      Assert.fail("Deleting a nonempty directory nonrecursively should fail");
    } catch (DirectoryNotEmptyException e) {
      Assert.assertEquals(
          ExceptionMessage.DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE.getMessage("testFolder2"),
          e.getMessage());
    }
    Assert.assertEquals(1, mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertEquals(2, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFolder2")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    Assert.assertEquals(fileId2,
        mFsMaster.getFileId(new AlluxioURI("/testFolder/testFolder2/testFile2")));
  }

  @Test
  public void deleteDirectoryWithFiles() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long fileId =
        mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"), CreateFileOptions.defaults());
    Assert.assertEquals(1, mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    mFsMaster.delete(new AlluxioURI("/testFolder"), DeleteOptions.defaults()
        .setRecursive(true));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID,
        mFsMaster.getFileId(new AlluxioURI("/testFolder")));
  }

  @Test
  public void deleteDirectoryWithFilesTest2() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long fileId =
        mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"), CreateFileOptions.defaults());
    Assert.assertEquals(1, mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    try {
      mFsMaster.delete(new AlluxioURI("/testFolder"), DeleteOptions.defaults()
          .setRecursive(false));
      Assert.fail("Deleting a nonempty directory nonrecursively should fail");
    } catch (DirectoryNotEmptyException e) {
      Assert.assertEquals(
          ExceptionMessage.DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE.getMessage("testFolder"),
          e.getMessage());
    }
    Assert.assertEquals(1, mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
  }

  @Test
  public void deleteEmptyDirectory() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    Assert.assertEquals(1, mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    mFsMaster.delete(new AlluxioURI("/testFolder"), DeleteOptions.defaults()
        .setRecursive(true));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID,
        mFsMaster.getFileId(new AlluxioURI("/testFolder")));
  }

  @Test
  public void deleteDirectoryWithPersistedAndNotPersistedSubfolders() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"),
        CreateDirectoryOptions.defaults().setPersisted(true));
    mFsMaster.createDirectory(new AlluxioURI("/testFolder/persisted"),
        CreateDirectoryOptions.defaults().setPersisted(true));
    mFsMaster.createDirectory(new AlluxioURI("/testFolder/inAlluxio1"),
        CreateDirectoryOptions.defaults().setPersisted(false));
    mFsMaster.createDirectory(new AlluxioURI("/testFolder/inAlluxio1/inAlluxio2"),
        CreateDirectoryOptions.defaults().setPersisted(false));
    mFsMaster.delete(new AlluxioURI("/testFolder"), DeleteOptions.defaults()
        .setRecursive(true));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID,
        mFsMaster.getFileId(new AlluxioURI("/testFolder")));
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {Name.WORKER_MEMORY_SIZE, "10mb", Name.USER_BLOCK_SIZE_BYTES_DEFAULT, "1k"})
  public void deleteDirectoryRecursive() throws Exception {
    AlluxioURI dir = new AlluxioURI("/testFolder");
    mFsMaster.createDirectory(dir, CreateDirectoryOptions.defaults());
    FileSystem fs = mLocalAlluxioClusterResource.get().getClient();
    for (int i = 0; i < 3; i++) {
      FileSystemTestUtils.createByteFile(fs, PathUtils.concatPath(dir, "file" + i), 100,
          alluxio.client.file.options.CreateFileOptions.defaults()
              .setWriteType(WriteType.MUST_CACHE));
    }
    fs.delete(dir, alluxio.client.file.options.DeleteOptions.defaults().setRecursive(true));
    assertFalse(fs.exists(dir));
    // Make sure that the blocks are cleaned up
    BlockMasterClient blockClient = BlockMasterClient.Factory.create(MasterClientConfig.defaults());
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
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileOptions.defaults());
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFile")));
    mFsMaster.delete(new AlluxioURI("/testFile"), DeleteOptions.defaults().setRecursive(true));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFsMaster.getFileId(new AlluxioURI("/testFile")));
  }

  @Test
  public void deleteRoot() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage(ExceptionMessage.DELETE_ROOT_DIRECTORY.getMessage());
    mFsMaster.delete(new AlluxioURI("/"), DeleteOptions.defaults().setRecursive(true));
  }

  @Test
  public void getCapacityBytes() {
    BlockMaster blockMaster =
        mLocalAlluxioClusterResource.get().getLocalAlluxioMaster().getMasterProcess()
            .getMaster(BlockMaster.class);
    Assert.assertEquals(1000, blockMaster.getCapacityBytes());
  }

  @Test
  public void lastModificationTimeCompleteFile() throws Exception {
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileOptions.defaults());
    long opTimeMs = TEST_TIME_MS;
    mFsMaster.completeFile(new AlluxioURI("/testFile"),
        CompleteFileOptions.defaults().setOperationTimeMs(opTimeMs).setUfsLength(0));
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals(opTimeMs, fileInfo.getLastModificationTimeMs());
  }

  @Test
  public void lastModificationTimeCreateFile() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long opTimeMs = TEST_TIME_MS;
    CreateFileOptions options = CreateFileOptions.defaults().setOperationTimeMs(opTimeMs);
    mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"), options);
    FileInfo folderInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertEquals(opTimeMs, folderInfo.getLastModificationTimeMs());
  }

  /**
   * Tests that deleting a file from a folder updates the folder's last modification time.
   */
  @Test
  public void lastModificationTimeDelete() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"), CreateFileOptions.defaults());
    long folderId = mFsMaster.getFileId(new AlluxioURI("/testFolder"));
    long modificationTimeBeforeDelete = mFsMaster.getFileInfo(folderId).getLastModificationTimeMs();
    CommonUtils.sleepMs(2);
    mFsMaster.delete(new AlluxioURI("/testFolder/testFile"), DeleteOptions.defaults()
        .setRecursive(true));
    long modificationTimeAfterDelete = mFsMaster.getFileInfo(folderId).getLastModificationTimeMs();
    Assert.assertTrue(modificationTimeBeforeDelete < modificationTimeAfterDelete);
  }

  @Test
  public void lastModificationTimeRename() throws Exception {
    AlluxioURI srcPath = new AlluxioURI("/testFolder/testFile1");
    AlluxioURI dstPath = new AlluxioURI("/testFolder/testFile2");
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    mFsMaster.createFile(srcPath, CreateFileOptions.defaults());
    RenameOptions options = RenameOptions.defaults().setOperationTimeMs(TEST_TIME_MS);
    mFsMaster.rename(srcPath, dstPath, options);
    FileInfo folderInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertEquals(TEST_TIME_MS, folderInfo.getLastModificationTimeMs());
  }

  @Test
  public void listFiles() throws Exception {
    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(64);

    HashSet<Long> ids = new HashSet<>();
    HashSet<Long> dirIds = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      AlluxioURI dir = new AlluxioURI("/i" + i);
      mFsMaster.createDirectory(dir, CreateDirectoryOptions.defaults());
      dirIds.add(mFsMaster.getFileId(dir));
      for (int j = 0; j < 10; j++) {
        ids.add(mFsMaster.createFile(dir.join("j" + j), options));
      }
    }
    HashSet<Long> listedIds = new HashSet<>();
    HashSet<Long> listedDirIds = new HashSet<>();
    List<FileInfo> infoList =
        mFsMaster.listStatus(new AlluxioURI("/"), ListStatusOptions.defaults());
    for (FileInfo info : infoList) {
      long id = info.getFileId();
      listedDirIds.add(id);
      for (FileInfo fileInfo : mFsMaster.listStatus(new AlluxioURI(info.getPath()),
          ListStatusOptions.defaults())) {
        listedIds.add(fileInfo.getFileId());
      }
    }
    Assert.assertEquals(ids, listedIds);
    Assert.assertEquals(dirIds, listedDirIds);
  }

  @Test
  public void listStatus() throws Exception {
    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(64);

    for (int i = 0; i < 10; i++) {
      mFsMaster.createDirectory(new AlluxioURI("/i" + i), CreateDirectoryOptions.defaults());
      for (int j = 0; j < 10; j++) {
        mFsMaster.createFile(new AlluxioURI("/i" + i + "/j" + j), options);
      }
    }

    Assert.assertEquals(1,
        mFsMaster.listStatus(new AlluxioURI("/i0/j0"), ListStatusOptions.defaults()).size());
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(10,
          mFsMaster.listStatus(new AlluxioURI("/i" + i), ListStatusOptions.defaults()).size());
    }
    Assert.assertEquals(10,
        mFsMaster.listStatus(new AlluxioURI("/"), ListStatusOptions.defaults()).size());
  }

  @Test
  public void notFileCompletion() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mFsMaster.createDirectory(new AlluxioURI("/testFile"), CreateDirectoryOptions.defaults());
    CompleteFileOptions options = CompleteFileOptions.defaults();
    mFsMaster.completeFile(new AlluxioURI("/testFile"), options);
  }

  @Test
  public void renameExistingDst() throws Exception {
    mFsMaster.createFile(new AlluxioURI("/testFile1"), CreateFileOptions.defaults());
    mFsMaster.createFile(new AlluxioURI("/testFile2"), CreateFileOptions.defaults());
    try {
      mFsMaster.rename(new AlluxioURI("/testFile1"), new AlluxioURI("/testFile2"),
          RenameOptions.defaults());
      Assert.fail("Should not be able to rename to an existing file");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void renameNonexistent() throws Exception {
    mFsMaster.createFile(new AlluxioURI("/testFile1"), CreateFileOptions.defaults());
    Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFsMaster.getFileId(new AlluxioURI("/testFile2")));
  }

  @Test
  public void renameToDeeper() throws Exception {
    CreateFileOptions createFileOptions = CreateFileOptions.defaults().setRecursive(true);
    CreateDirectoryOptions createDirectoryOptions =
        CreateDirectoryOptions.defaults().setRecursive(true);
    mThrown.expect(InvalidPathException.class);
    mFsMaster.createDirectory(new AlluxioURI("/testDir1/testDir2"), createDirectoryOptions);
    mFsMaster.createFile(new AlluxioURI("/testDir1/testDir2/testDir3/testFile3"),
        createFileOptions);
    mFsMaster.rename(new AlluxioURI("/testDir1/testDir2"),
        new AlluxioURI("/testDir1/testDir2/testDir3/testDir4"), RenameOptions.defaults());
  }

  @Test
  public void ttlCreateFile() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long ttl = 100;
    CreateFileOptions options = CreateFileOptions.defaults().setTtl(ttl);
    options.setTtlAction(TtlAction.FREE);
    mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"), options);
    FileInfo folderInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    Assert.assertEquals(ttl, folderInfo.getTtl());
    Assert.assertEquals(TtlAction.FREE, folderInfo.getTtlAction());
  }

  @Test
  public void ttlExpiredCreateFile() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long ttl = 1;
    CreateFileOptions options = CreateFileOptions.defaults().setTtl(ttl);
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFolder/testFile1"), options);
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
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long ttl = 1;
    CreateFileOptions options =
        CreateFileOptions.defaults().setPersisted(true).setTtl(ttl).setTtlAction(TtlAction.FREE);
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFolder/testFile1"), options);
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
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long ttl = 1;
    CreateFileOptions createOptions = CreateFileOptions.defaults().setTtl(ttl);
    mFsMaster.createFile(srcPath, createOptions);
    RenameOptions renameOptions = RenameOptions.defaults().setOperationTimeMs(TEST_TIME_MS);
    mFsMaster.rename(srcPath, dstPath, renameOptions);
    FileInfo folderInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile2")));
    Assert.assertEquals(ttl, folderInfo.getTtl());
  }

  @Test
  public void concurrentCreateDelete() throws Exception {
    List<Future<?>> futures = new ArrayList<>();
    AlluxioURI directory = new AlluxioURI("/dir");
    AlluxioURI[] files = new AlluxioURI[10];
    final int numThreads = 8;
    final int testDurationMs = 3000;

    for (int i = 0; i < 10; i++) {
      files[i] = directory.join("file_" + i);
    }

    mFsMaster.createDirectory(directory, CreateDirectoryOptions.defaults());
    AtomicBoolean stopThreads = new AtomicBoolean(false);
    CyclicBarrier barrier = new CyclicBarrier(numThreads);
    ExecutorService threadPool = Executors.newCachedThreadPool();
    try {
      for (int i = 0; i < numThreads; i++) {
        futures.add(threadPool.submit(new ConcurrentCreateDelete(barrier, stopThreads, files)));
      }

      CommonUtils.sleepMs(testDurationMs);
      stopThreads.set(true);
      for (Future<?> future : futures) {
        future.get();
      }

      // Stop Alluxio.
      mLocalAlluxioClusterResource.get().stopFS();
      // Create the master using the existing journal.
      createFileSystemMasterFromJournal();
    } finally {
      threadPool.shutdownNow();
      threadPool.awaitTermination(SHUTDOWN_TIME_MS, TimeUnit.MILLISECONDS);
    }
  }

  @Test
  public void syncReplay() throws Exception {
    AlluxioURI root = new AlluxioURI("/");
    AlluxioURI alluxioFile = new AlluxioURI("/in_alluxio");

    // Create a persisted Alluxio file (but no ufs file).
    mFsMaster.createFile(alluxioFile, CreateFileOptions.defaults().setPersisted(true));
    mFsMaster.completeFile(alluxioFile,
        CompleteFileOptions.defaults().setOperationTimeMs(TEST_TIME_MS).setUfsLength(0));

    // List what is in Alluxio, without syncing.
    List<FileInfo> files = mFsMaster.listStatus(root,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(CommonOptions.defaults().setSyncIntervalMs(-1)));
    Assert.assertEquals(1, files.size());
    Assert.assertEquals(alluxioFile.getName(), files.get(0).getName());

    // Add ufs only paths
    String ufs = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Files.createDirectory(Paths.get(ufs, "ufs_dir"));
    Files.createFile(Paths.get(ufs, "ufs_file"));

    // List with syncing, which will remove alluxio only path, and add ufs only paths.
    files = mFsMaster.listStatus(root,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(CommonOptions.defaults().setSyncIntervalMs(0)));
    Assert.assertEquals(2, files.size());
    Set<String> filenames = files.stream().map(FileInfo::getName).collect(Collectors.toSet());
    Assert.assertTrue(filenames.contains("ufs_dir"));
    Assert.assertTrue(filenames.contains("ufs_file"));

    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    MasterRegistry registry = createFileSystemMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);

    // List what is in Alluxio, without syncing. Should match the last state.
    files = fsMaster.listStatus(root,
        ListStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(CommonOptions.defaults().setSyncIntervalMs(-1)));
    Assert.assertEquals(2, files.size());
    filenames = files.stream().map(FileInfo::getName).collect(Collectors.toSet());
    Assert.assertTrue(filenames.contains("ufs_dir"));
    Assert.assertTrue(filenames.contains("ufs_file"));
  }

  @Test
  public void syncDirReplay() throws Exception {
    AlluxioURI dir = new AlluxioURI("/dir/");

    // Add ufs nested file.
    String ufs = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    Files.createDirectory(Paths.get(ufs, "dir"));
    Files.createFile(Paths.get(ufs, "dir", "file"));

    File ufsDir = new File(Paths.get(ufs, "dir").toString());
    Assert.assertTrue(ufsDir.setReadable(true, false));
    Assert.assertTrue(ufsDir.setWritable(true, false));
    Assert.assertTrue(ufsDir.setExecutable(true, false));

    // List dir with syncing
    FileInfo info = mFsMaster.getFileInfo(dir,
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(CommonOptions.defaults().setSyncIntervalMs(0)));
    Assert.assertNotNull(info);
    Assert.assertEquals("dir", info.getName());
    // Retrieve the mode
    int mode = info.getMode();

    // Update mode of the ufs dir
    Assert.assertTrue(ufsDir.setExecutable(false, false));

    // List dir with syncing, should update the mode
    info = mFsMaster.getFileInfo(dir,
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(CommonOptions.defaults().setSyncIntervalMs(0)));
    Assert.assertNotNull(info);
    Assert.assertEquals("dir", info.getName());
    Assert.assertNotEquals(mode, info.getMode());
    // update the expected mode
    mode = info.getMode();

    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    MasterRegistry registry = createFileSystemMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);

    // List what is in Alluxio, without syncing.
    info = fsMaster.getFileInfo(dir,
        GetStatusOptions.defaults().setLoadMetadataType(LoadMetadataType.Never)
            .setCommonOptions(CommonOptions.defaults().setSyncIntervalMs(-1)));
    Assert.assertNotNull(info);
    Assert.assertEquals("dir", info.getName());
    Assert.assertEquals(mode, info.getMode());
  }

  @Test
  public void unavailableUfsRecursiveCreate() throws Exception {
    String ufsBase = "test://test";

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
    Mockito.when(mockUfs.getDirectoryStatus(ufsBase))
        .thenReturn(ufsStatus);
    Mockito.when(mockUfs.mkdirs(Matchers.eq(ufsBase + "/dir1"), Matchers.any()))
        .thenThrow(new IOException("ufs unavailable"));
    Mockito.when(mockUfs.getStatus(ufsBase))
        .thenReturn(ufsStatus);

    UnderFileSystemFactoryRegistry.register(mockUfsFactory);

    mFsMaster.mount(new AlluxioURI("/mnt"), new AlluxioURI(ufsBase), MountOptions.defaults());

    AlluxioURI root = new AlluxioURI("/mnt/");
    AlluxioURI alluxioFile = new AlluxioURI("/mnt/dir1/dir2/file");

    // Create a persisted Alluxio file (but no ufs file).
    try {
      mFsMaster.createFile(alluxioFile,
          CreateFileOptions.defaults().setPersisted(true).setRecursive(true));
      Assert.fail("persisted create should fail, when UFS is unavailable");
    } catch (Exception e) {
      // expected, ignore
    }

    List<FileInfo> files = mFsMaster.listStatus(root, ListStatusOptions.defaults());
    Assert.assertTrue(files.isEmpty());

    try {
      // should not exist
      files = mFsMaster.listStatus(new AlluxioURI("/mnt/dir1/"), ListStatusOptions.defaults());
      Assert.fail("dir should not exist, when UFS is unavailable");
    } catch (Exception e) {
      // expected, ignore
    }

    try {
      // should not exist
      mFsMaster.delete(new AlluxioURI("/mnt/dir1/"), DeleteOptions.defaults().setRecursive(true));
      Assert.fail("cannot delete non-existing directory, when UFS is unavailable");
    } catch (Exception e) {
      // expected, ignore
      files = null;
    }

    files = mFsMaster.listStatus(new AlluxioURI("/mnt/"), ListStatusOptions.defaults());
    Assert.assertTrue(files.isEmpty());

    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    MasterRegistry registry = MasterTestUtils.createLeaderFileSystemMasterFromJournal();
    FileSystemMaster newFsMaster = registry.get(FileSystemMaster.class);

    files = newFsMaster.listStatus(new AlluxioURI("/mnt/"), ListStatusOptions.defaults());
    Assert.assertTrue(files.isEmpty());
  }

  // TODO(gene): Journal format has changed, maybe add Version to the format and add this test back
  // or remove this test when we have better tests against journal checkpoint.
  // @Test
  // public void writeImage() throws IOException {
  // // initialize the MasterInfo
  // Journal journal =
  // new Journal(mLocalAlluxioCluster.getAlluxioHome() + "journal/", "image.data", "log.data",
  // mMasterAlluxioConf);
  // Journal
  // MasterInfo info =
  // new MasterInfo(new InetSocketAddress(9999), journal, mExecutorService, mMasterAlluxioConf);

  // // create the output streams
  // ByteArrayOutputStream os = new ByteArrayOutputStream();
  // DataOutputStream dos = new DataOutputStream(os);
  // ObjectMapper mapper = JsonObject.createObjectMapper();
  // ObjectWriter writer = mapper.writer();
  // ImageElement version = null;
  // ImageElement checkpoint = null;

  // // write the image
  // info.writeImage(writer, dos);

  // // parse the written bytes and look for the Checkpoint and Version ImageElements
  // String[] splits = new String(os.toByteArray()).split("\n");
  // for (String split : splits) {
  // byte[] bytes = split.getBytes();
  // JsonParser parser = mapper.getFactory().createParser(bytes);
  // ImageElement ele = parser.readValueAs(ImageElement.class);

  // if (ele.mType.equals(ImageElementType.Checkpoint)) {
  // checkpoint = ele;
  // }

  // if (ele.mType.equals(ImageElementType.Version)) {
  // version = ele;
  // }
  // }

  // // test the elements
  // Assert.assertNotNull(checkpoint);
  // Assert.assertEquals(checkpoint.mType, ImageElementType.Checkpoint);
  // Assert.assertEquals(Constants.JOURNAL_VERSION, version.getInt("version").intValue());
  // Assert.assertEquals(1, checkpoint.getInt("inodeCounter").intValue());
  // Assert.assertEquals(0, checkpoint.getInt("editTransactionCounter").intValue());
  // Assert.assertEquals(0, checkpoint.getInt("dependencyCounter").intValue());
  // }

  @Test
  public void ufsModeCreateFile() throws Exception {
    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.READ_ONLY);

    // Alluxio only should not be affected
    mFsMaster.createFile(new AlluxioURI("/in_alluxio"),
        CreateFileOptions.defaults().setPersisted(false));
    // Ufs file creation should throw an exception
    mThrown.expect(AccessControlException.class);
    mFsMaster.createFile(new AlluxioURI("/in_ufs"),
        CreateFileOptions.defaults().setPersisted(true));
  }

  @Test
  public void ufsModeCreateDirectory() throws Exception {
    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.READ_ONLY);

    // Alluxio only should not be affected
    mFsMaster.createDirectory(new AlluxioURI("/in_alluxio"),
        CreateDirectoryOptions.defaults().setPersisted(false));
    // Ufs file creation should throw an exception
    mThrown.expect(AccessControlException.class);
    mFsMaster.createDirectory(new AlluxioURI("/in_ufs"),
        CreateDirectoryOptions.defaults().setPersisted(true));
  }

  @Test
  public void ufsModePersist() throws Exception {
    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.READ_ONLY);

    AlluxioURI alluxioFile = new AlluxioURI("/in_alluxio");
    mFsMaster.createFile(alluxioFile, CreateFileOptions.defaults().setPersisted(false));

    mThrown.expect(AccessControlException.class);
    mFsMaster.scheduleAsyncPersistence(alluxioFile);
  }

  @Test
  public void ufsModeDeleteFile() throws Exception {
    AlluxioURI alluxioFile = new AlluxioURI("/in_alluxio");
    mFsMaster.createFile(alluxioFile, CreateFileOptions.defaults().setPersisted(true));

    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.READ_ONLY);

    mThrown.expect(FailedPreconditionException.class);
    mFsMaster.delete(alluxioFile, DeleteOptions.defaults().setAlluxioOnly(false));
  }

  @Test
  public void ufsModeDeleteDirectory() throws Exception {
    AlluxioURI alluxioDirectory = new AlluxioURI("/in_ufs_dir");
    mFsMaster.createDirectory(alluxioDirectory,
        CreateDirectoryOptions.defaults().setPersisted(true));
    AlluxioURI alluxioFile = new AlluxioURI("/in_ufs_dir/in_ufs_file");
    mFsMaster.createFile(alluxioFile, CreateFileOptions.defaults().setPersisted(true));

    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.READ_ONLY);

    mThrown.expect(FailedPreconditionException.class);
    mFsMaster.delete(alluxioDirectory,
        DeleteOptions.defaults().setAlluxioOnly(false).setRecursive(true));

    // Check Alluxio entries exist after failed delete
    long dirId = mFsMaster.getFileId(alluxioDirectory);
    Assert.assertNotEquals(-1, dirId);
    long fileId = mFsMaster.getFileId(alluxioFile);
    Assert.assertNotEquals(-1, fileId);
  }

  @Test
  public void ufsModeRenameFile() throws Exception {
    mFsMaster.createFile(new AlluxioURI("/in_ufs_src"),
        CreateFileOptions.defaults().setPersisted(true));

    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.READ_ONLY);

    mThrown.expect(AccessControlException.class);
    mFsMaster.rename(new AlluxioURI("/in_ufs_src"), new AlluxioURI("/in_ufs_dst"),
        RenameOptions.defaults());
  }

  @Test
  public void ufsModeRenameDirectory() throws Exception {
    AlluxioURI alluxioDirectory = new AlluxioURI("/in_ufs_dir");
    mFsMaster.createDirectory(alluxioDirectory,
        CreateDirectoryOptions.defaults().setPersisted(true));
    AlluxioURI alluxioFile = new AlluxioURI("/in_ufs_dir/in_ufs_file");
    mFsMaster.createFile(alluxioFile, CreateFileOptions.defaults().setPersisted(true));

    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.READ_ONLY);

    mThrown.expect(AccessControlException.class);
    mFsMaster.rename(alluxioDirectory, new AlluxioURI("/in_ufs_dst"), RenameOptions.defaults());

    // Check Alluxio entries exist after failed rename
    long dirId = mFsMaster.getFileId(alluxioDirectory);
    Assert.assertNotEquals(-1, dirId);
    long fileId = mFsMaster.getFileId(alluxioFile);
    Assert.assertNotEquals(-1, fileId);
  }

  @Test
  public void ufsModeSetAttribute() throws Exception {
    AlluxioURI alluxioFile = new AlluxioURI("/in_alluxio");
    mFsMaster.createFile(alluxioFile, CreateFileOptions.defaults().setPersisted(true));

    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.READ_ONLY);
    long opTimeMs = TEST_TIME_MS;
    mFsMaster.completeFile(alluxioFile,
        CompleteFileOptions.defaults().setOperationTimeMs(opTimeMs).setUfsLength(0));

    mThrown.expect(AccessControlException.class);
    mFsMaster.setAttribute(alluxioFile, SetAttributeOptions.defaults().setMode((short) 0777));
  }

  @Test
  public void ufsModeReplay() throws Exception {
    mFsMaster.updateUfsMode(new AlluxioURI(mFsMaster.getUfsAddress()),
        UfsMode.NO_ACCESS);

    // Stop Alluxio.
    mLocalAlluxioClusterResource.get().stopFS();
    // Create the master using the existing journal.
    MasterRegistry registry = createFileSystemMasterFromJournal();
    FileSystemMaster fsMaster = registry.get(FileSystemMaster.class);

    AlluxioURI alluxioFile = new AlluxioURI("/in_alluxio");
    // Create file should throw an Exception even after restart
    mThrown.expect(AccessControlException.class);
    fsMaster.createFile(alluxioFile, CreateFileOptions.defaults().setPersisted(true));
  }

  /**
   * Tests creating a directory in a nested directory load the UFS status of Inodes on the path.
   */
  @Test
  public void createDirectoryInNestedDirectories() throws Exception {
    String ufs = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    String targetPath = Paths.get(ufs, "d1", "d2", "d3").toString();
    FileUtils.createDir(targetPath);
    FileUtils.changeLocalFilePermission(targetPath, new Mode((short) 0755).toString());
    String parentPath = Paths.get(ufs, "d1").toString();
    FileUtils.changeLocalFilePermission(parentPath, new Mode((short) 0700).toString());
    AlluxioURI path = new AlluxioURI(Paths.get("/d1", "d2", "d3", "d4").toString());

    mFsMaster.createDirectory(path, CreateDirectoryOptions.defaults()
        .setPersisted(true).setRecursive(true).setMode(new Mode((short) 0755)));

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
    String ufs = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    String targetPath = Paths.get(ufs, "d1", "d2", "d3").toString();
    FileUtils.createDir(targetPath);
    FileUtils.changeLocalFilePermission(targetPath, new Mode((short) 0755).toString());
    String parentPath = Paths.get(ufs, "d1").toString();
    FileUtils.changeLocalFilePermission(parentPath, new Mode((short) 0700).toString());
    AlluxioURI path = new AlluxioURI(Paths.get("/d1", "d2", "d3").toString());

    mFsMaster.listStatus(path, ListStatusOptions.defaults()
        .setLoadMetadataType(LoadMetadataType.Once));

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
    String ufs = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    String parentPath = Paths.get(ufs, "d1").toString();
    FileUtils.createDir(parentPath);
    FileUtils.changeLocalFilePermission(parentPath, new Mode((short) 0755).toString());
    AlluxioURI path = new AlluxioURI(Paths.get("/d1", "d2", "d3", "d4").toString());
    String ufsPath = Paths.get(ufs, "d1", "d2", "d3", "d4").toString();
    mFsMaster.createDirectory(path, CreateDirectoryOptions.defaults()
        .setPersisted(true).setRecursive(true).setMode(new Mode((short) 0700)));
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
    String ufs = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    String parentPath = Paths.get(ufs, "d1").toString();
    FileUtils.createDir(parentPath);
    FileUtils.changeLocalFilePermission(parentPath, new Mode((short) 0600).toString());
    AlluxioURI path = new AlluxioURI(Paths.get("/d1", "d2", "d3", "d4").toString());

    // this should fail
    mFsMaster.createDirectory(path, CreateDirectoryOptions.defaults()
        .setPersisted(true).setRecursive(true).setMode(new Mode((short) 0755)));
  }

  /**
   * Tests loading ufs last modified time for directories.
   */
  @Test
  public void loadDirectoryTimestamps() throws Exception {
    String name = "d1";
    String ufs = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
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
    String ufs = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
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
    String ufs = Configuration.get(PropertyKey.MASTER_MOUNT_TABLE_ROOT_UFS);
    String parentUfsPath = Paths.get(ufs, parentName).toString();
    FileUtils.createDir(parentUfsPath);
    File file = new File(parentUfsPath);
    long actualTime = file.lastModified();
    Thread.sleep(100);
    AlluxioURI parentPath = new AlluxioURI("/" + parentName);
    AlluxioURI path = new AlluxioURI("/" + Paths.get(parentName, childName));
    mFsMaster.createDirectory(path, CreateDirectoryOptions.defaults()
        .setPersisted(true).setRecursive(true).setMode(new Mode((short) 0700)));
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
    private CreateFileOptions mCreateFileOptions;

    /**
     * Constructs the concurrent creator.
     *
     * @param depth the depth of files to be created in one directory
     * @param concurrencyDepth the concurrency depth of files to be created in one directory
     * @param initPath the directory of files to be created in
     */
    ConcurrentCreator(int depth, int concurrencyDepth, AlluxioURI initPath) {
      this(depth, concurrencyDepth, initPath, CreateFileOptions.defaults());
    }

    /**
     * Constructs the concurrent creator.
     *
     * @param depth the depth of files to be created in one directory
     * @param concurrencyDepth the concurrency depth of files to be created in one directory
     * @param initPath the directory of files to be created in
     * @param options method options
     */
    ConcurrentCreator(int depth, int concurrencyDepth, AlluxioURI initPath,
        CreateFileOptions options) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mInitPath = initPath;
      mCreateFileOptions = options;
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
        long fileId = mFsMaster.createFile(path, mCreateFileOptions);
        Assert.assertEquals(fileId, mFsMaster.getFileId(path));
        // verify the user permission for file
        FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
        Assert.assertEquals("", fileInfo.getOwner());
        Assert.assertEquals(0644, (short) fileInfo.getMode());
      } else {
        mFsMaster.createDirectory(path, CreateDirectoryOptions.defaults());
        Assert.assertNotNull(mFsMaster.getFileId(path));
        long dirId = mFsMaster.getFileId(path);
        Assert.assertNotEquals(-1, dirId);
        FileInfo dirInfo = mFsMaster.getFileInfo(dirId);
        Assert.assertEquals("", dirInfo.getOwner());
        Assert.assertEquals(0755, (short) dirInfo.getMode());
      }

      if (concurrencyDepth > 0) {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
          ArrayList<Future<Void>> futures = new ArrayList<>(FILES_PER_NODE);
          for (int i = 0; i < FILES_PER_NODE; i++) {
            Callable<Void> call = (new ConcurrentCreator(depth - 1, concurrencyDepth - 1,
                path.join(Integer.toString(i)), mCreateFileOptions));
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
      mFsMaster.free(path, FreeOptions.defaults().setForced(true).setRecursive(true));
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
      mFsMaster.delete(path, DeleteOptions.defaults().setRecursive(true));
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
          CreateDirectoryOptions options = CreateDirectoryOptions.defaults().setRecursive(true);
          mFsMaster.createDirectory(dstPath.getParent(), options);
        } catch (FileAlreadyExistsException | InvalidPathException e) {
          // FileAlreadyExistsException: This is an acceptable exception to get, since we don't know
          // if the parent has been created yet by another thread.
          // InvalidPathException: This could happen if we are renaming something that's a child of
          // the root.
        }
        mFsMaster.rename(srcPath, dstPath, RenameOptions.defaults());
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

  /**
   * A class to start a thread that creates a file, completes the file and then deletes the file.
   */
  private class ConcurrentCreateDelete implements Callable<Void> {
    private final CyclicBarrier mStartBarrier;
    private final AtomicBoolean mStopThread;
    private final AlluxioURI[] mFiles;

    /**
     * Concurrent create and delete of file.
     * @param barrier cyclic barrier
     * @param stopThread stop Thread
     * @param files files to create or delete
     */
    public ConcurrentCreateDelete(CyclicBarrier barrier, AtomicBoolean stopThread,
        AlluxioURI[] files) {
      mStartBarrier = barrier;
      mStopThread = stopThread;
      mFiles = files;
    }

    @Override
    @Nullable
    public Void call() throws Exception {
      AuthenticatedClientUser.set(TEST_USER);
      mStartBarrier.await();
      Random random = new Random();
      while (!mStopThread.get()) {
        int id = random.nextInt(mFiles.length);
        try {
          // Create and complete a random file.
          mFsMaster.createFile(mFiles[id], CreateFileOptions.defaults());
          mFsMaster.completeFile(mFiles[id], CompleteFileOptions.defaults());
        } catch (FileAlreadyExistsException | FileDoesNotExistException
            | FileAlreadyCompletedException | InvalidPathException e) {
          // Ignore
        } catch (Exception e) {
          throw e;
        }
        id = random.nextInt(mFiles.length);
        try {
          // Delete a random file.
          mFsMaster.delete(mFiles[id], DeleteOptions.defaults().setRecursive(false));
        } catch (FileDoesNotExistException | InvalidPathException e) {
          // Ignore
        } catch (Exception e) {
          throw e;
        }
      }
      return null;
    }
  }
}
