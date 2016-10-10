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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
import alluxio.PropertyKey;
import alluxio.exception.DirectoryNotEmptyException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatScheduler;
import alluxio.heartbeat.ManuallyScheduleHeartbeat;
import alluxio.master.MasterTestUtils;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.meta.InodePathPair;
import alluxio.master.file.meta.InodeTree;
import alluxio.master.file.meta.LockedInodePath;
import alluxio.master.file.meta.TtlIntervalRule;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.master.file.options.ListStatusOptions;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.wire.FileInfo;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.mockito.internal.util.reflection.Whitebox;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Test behavior of {@link FileSystemMaster}.
 *
 * For example, (concurrently) creating/deleting/renaming files.
 */
public class FileSystemMasterIntegrationTest {
  private static final int DEPTH = 6;
  private static final int FILES_PER_NODE = 4;
  private static final int CONCURRENCY_DEPTH = 3;
  private static final AlluxioURI ROOT_PATH = new AlluxioURI("/root");
  private static final AlluxioURI ROOT_PATH2 = new AlluxioURI("/root2");
  // Modify current time so that implementations can't accidentally pass unit tests by ignoring
  // this specified time and always using System.currentTimeMillis()
  private static final long TEST_CURRENT_TIME = 300;
  private static final long TTL_CHECKER_INTERVAL_MS = 1000;
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
          .setProperty(PropertyKey.MASTER_TTL_CHECKER_INTERVAL_MS,
              String.valueOf(TTL_CHECKER_INTERVAL_MS))
          .setProperty(PropertyKey.WORKER_MEMORY_SIZE, 1000)
          .setProperty(PropertyKey.SECURITY_LOGIN_USERNAME, TEST_USER)
          .build();

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private FileSystemMaster mFsMaster;
  private InodeTree mInodeTree;

  @Before
  public final void before() throws Exception {
    mFsMaster =
        mLocalAlluxioClusterResource.get().getMaster().getInternalMaster().getFileSystemMaster();
    AuthenticatedClientUser.set(TEST_USER);
    mInodeTree = (InodeTree) Whitebox.getInternalState(mFsMaster, "mInodeTree");
  }

  @After
  public final void after() throws Exception {
    AuthenticatedClientUser.remove();
  }

  @Test
  public void clientFileInfoDirectory() throws Exception {
    AlluxioURI path = new AlluxioURI("/testFolder");
    mFsMaster.createDirectory(path, CreateDirectoryOptions.defaults());
    long fileId = mFsMaster.getFileId(path);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("testFolder", fileInfo.getName());
    Assert.assertEquals(1, fileInfo.getFileId());
    Assert.assertEquals(0, fileInfo.getLength());
    Assert.assertFalse(fileInfo.isCacheable());
    Assert.assertTrue(fileInfo.isCompleted());
    Assert.assertTrue(fileInfo.isFolder());
    Assert.assertFalse(fileInfo.isPersisted());
    Assert.assertFalse(fileInfo.isPinned());
    Assert.assertEquals("", fileInfo.getOwner());
    Assert.assertEquals(0755, (short) fileInfo.getMode());
  }

  @Test
  public void clientFileInfoEmptyFile() throws Exception {
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileOptions.defaults());
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("testFile", fileInfo.getName());
    Assert.assertEquals(fileId, fileInfo.getFileId());
    Assert.assertEquals(0, fileInfo.getLength());
    Assert.assertTrue(fileInfo.isCacheable());
    Assert.assertFalse(fileInfo.isCompleted());
    Assert.assertFalse(fileInfo.isFolder());
    Assert.assertFalse(fileInfo.isPersisted());
    Assert.assertFalse(fileInfo.isPinned());
    Assert.assertEquals(Constants.NO_TTL, fileInfo.getTtl());
    Assert.assertEquals("", fileInfo.getOwner());
    Assert.assertEquals(0644, (short) fileInfo.getMode());
  }

  private FileSystemMaster createFileSystemMasterFromJournal() throws IOException {
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

      FileSystemMaster fsMaster = createFileSystemMasterFromJournal();
      for (FileInfo info : mFsMaster
          .listStatus(new AlluxioURI("/"), ListStatusOptions.defaults())) {
        AlluxioURI path = new AlluxioURI(info.getPath());
        Assert.assertEquals(mFsMaster.getFileId(path), fsMaster.getFileId(path));
      }
      before();
    }
  }

  @Test
  public void concurrentCreate() throws Exception {
    ConcurrentCreator concurrentCreator =
        new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentCreator.call();
  }

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

  @Test
  public void concurrentFree() throws Exception {
    ConcurrentCreator concurrentCreator =
        new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentCreator.call();

    ConcurrentFreer concurrentFreer =
        new ConcurrentFreer(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentFreer.call();
  }

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
    Assert.assertFalse(fileInfo.isFolder());
    Assert.assertEquals("", fileInfo.getOwner());
    Assert.assertEquals(0644, (short) fileInfo.getMode());
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
    mFsMaster.delete(new AlluxioURI("/testFolder"), true);
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
      mFsMaster.delete(new AlluxioURI("/testFolder/testFolder2"), false);
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
    mFsMaster.delete(new AlluxioURI("/testFolder"), true);
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
      mFsMaster.delete(new AlluxioURI("/testFolder"), false);
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
    mFsMaster.delete(new AlluxioURI("/testFolder"), true);
    Assert.assertEquals(IdUtils.INVALID_FILE_ID,
        mFsMaster.getFileId(new AlluxioURI("/testFolder")));
  }

  @Test
  public void deleteFile() throws Exception {
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileOptions.defaults());
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFile")));
    mFsMaster.delete(new AlluxioURI("/testFile"), true);
    Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFsMaster.getFileId(new AlluxioURI("/testFile")));
  }

  @Test
  public void deleteRoot() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage(ExceptionMessage.DELETE_ROOT_DIRECTORY.getMessage());
    mFsMaster.delete(new AlluxioURI("/"), true);
  }

  @Test
  public void getCapacityBytes() {
    BlockMaster blockMaster =
        mLocalAlluxioClusterResource.get().getMaster().getInternalMaster().getBlockMaster();
    Assert.assertEquals(1000, blockMaster.getCapacityBytes());
  }

  @Test
  public void lastModificationTimeCompleteFile() throws Exception {
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileOptions.defaults());
    long opTimeMs = TEST_CURRENT_TIME;
    try (LockedInodePath inodePath = mInodeTree
        .lockFullInodePath(new AlluxioURI("/testFile"), InodeTree.LockMode.WRITE)) {
      mFsMaster.completeFileInternal(new ArrayList<Long>(), inodePath, 0, opTimeMs);
    }
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals(opTimeMs, fileInfo.getLastModificationTimeMs());
  }

  @Test
  public void lastModificationTimeCreateFile() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long opTimeMs = TEST_CURRENT_TIME;
    CreateFileOptions options = CreateFileOptions.defaults().setOperationTimeMs(opTimeMs);
    try (LockedInodePath inodePath = mInodeTree
        .lockInodePath(new AlluxioURI("/testFolder/testFile"), InodeTree.LockMode.WRITE)) {
      mFsMaster.createFileInternal(inodePath, options);
    }
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
    mFsMaster.delete(new AlluxioURI("/testFolder/testFile"), true);
    long modificationTimeAfterDelete = mFsMaster.getFileInfo(folderId).getLastModificationTimeMs();
    Assert.assertTrue(modificationTimeBeforeDelete < modificationTimeAfterDelete);
  }

  @Test
  public void lastModificationTimeRename() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long fileId =
        mFsMaster.createFile(new AlluxioURI("/testFolder/testFile1"), CreateFileOptions.defaults());
    long opTimeMs = TEST_CURRENT_TIME;

    try (InodePathPair inodePathPair = mInodeTree
        .lockInodePathPair(new AlluxioURI("/testFolder/testFile1"), InodeTree.LockMode.WRITE_PARENT,
            new AlluxioURI("/testFolder/testFile2"), InodeTree.LockMode.WRITE)) {
      LockedInodePath srcPath = inodePathPair.getFirst();
      LockedInodePath dstPath = inodePathPair.getSecond();
      mFsMaster.renameInternal(srcPath, dstPath, true, opTimeMs);
    }
    FileInfo folderInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertEquals(opTimeMs, folderInfo.getLastModificationTimeMs());
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
      for (FileInfo fileInfo : mFsMaster
          .listStatus(new AlluxioURI(info.getPath()), ListStatusOptions.defaults())) {
        listedIds.add(fileInfo.getFileId());
      }
    }
    Assert.assertEquals(ids, listedIds);
    Assert.assertEquals(dirIds, listedDirIds);
  }

  @Test
  public void ls() throws Exception {
    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(64);

    for (int i = 0; i < 10; i++) {
      mFsMaster.createDirectory(new AlluxioURI("/i" + i), CreateDirectoryOptions.defaults());
      for (int j = 0; j < 10; j++) {
        mFsMaster.createFile(new AlluxioURI("/i" + i + "/j" + j), options);
      }
    }

    Assert.assertEquals(1,
        mFsMaster.listStatus(new AlluxioURI("/i0/j0"), ListStatusOptions.defaults())
            .size());
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(10,
          mFsMaster.listStatus(new AlluxioURI("/i" + i), ListStatusOptions.defaults())
              .size());
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
      mFsMaster.rename(new AlluxioURI("/testFile1"), new AlluxioURI("/testFile2"));
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
        new AlluxioURI("/testDir1/testDir2/testDir3/testDir4"));
  }

  @Test
  public void ttlCreateFile() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long ttl = 100;
    CreateFileOptions options = CreateFileOptions.defaults().setTtl(ttl);
    try (LockedInodePath inodePath = mInodeTree
        .lockInodePath(new AlluxioURI("/testFolder/testFile"), InodeTree.LockMode.WRITE)) {
      mFsMaster.createFileInternal(inodePath, options);
    }
    FileInfo folderInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    Assert.assertEquals(ttl, folderInfo.getTtl());
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
  public void ttlRename() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long ttl = 1;
    CreateFileOptions options = CreateFileOptions.defaults().setTtl(ttl);
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFolder/testFile1"), options);

    try (InodePathPair inodePathPair = mInodeTree
        .lockInodePathPair(new AlluxioURI("/testFolder/testFile1"), InodeTree.LockMode.WRITE_PARENT,
            new AlluxioURI("/testFolder/testFile2"), InodeTree.LockMode.WRITE)) {
      LockedInodePath srcPath = inodePathPair.getFirst();
      LockedInodePath dstPath = inodePathPair.getSecond();
      mFsMaster.renameInternal(srcPath, dstPath, true, TEST_CURRENT_TIME);
    }
    FileInfo folderInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile2")));
    Assert.assertEquals(ttl, folderInfo.getTtl());
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

  class ConcurrentCreator implements Callable<Void> {
    private int mDepth;
    private int mConcurrencyDepth;
    private AlluxioURI mInitPath;

    ConcurrentCreator(int depth, int concurrencyDepth, AlluxioURI initPath) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mInitPath = initPath;
    }

    @Override
    public Void call() throws Exception {
      AuthenticatedClientUser.set(TEST_USER);
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

    public void exec(int depth, int concurrencyDepth, AlluxioURI path) throws Exception {
      if (depth < 1) {
        return;
      } else if (depth == 1) {
        long fileId = mFsMaster.createFile(path, CreateFileOptions.defaults());
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
    }
  }

  /*
   * This class provides multiple concurrent threads to free all files in one directory.
   */
  class ConcurrentFreer implements Callable<Void> {
    private int mDepth;
    private int mConcurrencyDepth;
    private AlluxioURI mInitPath;

    ConcurrentFreer(int depth, int concurrencyDepth, AlluxioURI initPath) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mInitPath = initPath;
    }

    @Override
    public Void call() throws Exception {
      AuthenticatedClientUser.set(TEST_USER);
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

    private void doFree(AlluxioURI path) throws Exception {
      mFsMaster.free(path, true);
      Assert.assertNotEquals(IdUtils.INVALID_FILE_ID, mFsMaster.getFileId(path));
    }

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
  /*
   * This class provides multiple concurrent threads to delete all files in one directory.
   */
  class ConcurrentDeleter implements Callable<Void> {
    private int mDepth;
    private int mConcurrencyDepth;
    private AlluxioURI mInitPath;

    ConcurrentDeleter(int depth, int concurrencyDepth, AlluxioURI initPath) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mInitPath = initPath;
    }

    @Override
    public Void call() throws Exception {
      AuthenticatedClientUser.set(TEST_USER);
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

    private void doDelete(AlluxioURI path) throws Exception {
      mFsMaster.delete(path, true);
      Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFsMaster.getFileId(path));
    }

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

  class ConcurrentRenamer implements Callable<Void> {
    private int mDepth;
    private int mConcurrencyDepth;
    private AlluxioURI mRootPath;
    private AlluxioURI mRootPath2;
    private AlluxioURI mInitPath;

    ConcurrentRenamer(int depth, int concurrencyDepth, AlluxioURI rootPath, AlluxioURI rootPath2,
                      AlluxioURI initPath) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mRootPath = rootPath;
      mRootPath2 = rootPath2;
      mInitPath = initPath;
    }

    @Override
    public Void call() throws Exception {
      AuthenticatedClientUser.set(TEST_USER);
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

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
        mFsMaster.rename(srcPath, dstPath);
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
