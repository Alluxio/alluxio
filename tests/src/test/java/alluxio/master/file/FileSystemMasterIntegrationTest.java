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

package alluxio.master.file;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.LocalAlluxioClusterResource;
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
import alluxio.master.file.meta.TtlBucketPrivateAccess;
import alluxio.master.file.options.CompleteFileOptions;
import alluxio.master.file.options.CreateDirectoryOptions;
import alluxio.master.file.options.CreateFileOptions;
import alluxio.security.authentication.AuthType;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.CommonUtils;
import alluxio.util.IdUtils;
import alluxio.wire.FileInfo;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Test behavior of {@link FileSystemMaster}.
 *
 * For example, (concurrently) creating/deleting/renaming files.
 */
public class FileSystemMasterIntegrationTest {
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
      AuthenticatedClientUser.set(TEST_AUTHENTICATE_USER);
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
        Assert.assertEquals(TEST_AUTHENTICATE_USER, fileInfo.getUserName());
        Assert.assertEquals(0644, (short) fileInfo.getPermission());
      } else {
        mFsMaster.createDirectory(path, CreateDirectoryOptions.defaults());
        Assert.assertNotNull(mFsMaster.getFileId(path));
        long dirId = mFsMaster.getFileId(path);
        Assert.assertNotEquals(-1, dirId);
        FileInfo dirInfo = mFsMaster.getFileInfo(dirId);
        Assert.assertEquals(TEST_AUTHENTICATE_USER, dirInfo.getUserName());
        Assert.assertEquals(0755, (short) dirInfo.getPermission());
      }

      if (concurrencyDepth > 0) {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
          ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>(FILES_PER_NODE);
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
            ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>(FILES_PER_NODE);
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
            ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>(FILES_PER_NODE);
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
      AuthenticatedClientUser.set(TEST_AUTHENTICATE_USER);
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
        } catch (FileAlreadyExistsException e) {
          // This is an acceptable exception to get, since we don't know if the parent has been
          // created yet by another thread.
        } catch (InvalidPathException e) {
          // This could happen if we are renaming something that's a child of the root.
        }
        mFsMaster.rename(srcPath, dstPath);
        Assert.assertEquals(fileId, mFsMaster.getFileId(dstPath));
      } else if (concurrencyDepth > 0) {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
          ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>(FILES_PER_NODE);
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

  private static final int DEPTH = 6;
  private static final int FILES_PER_NODE = 4;
  private static final int CONCURRENCY_DEPTH = 3;
  private static final AlluxioURI ROOT_PATH = new AlluxioURI("/root");
  private static final AlluxioURI ROOT_PATH2 = new AlluxioURI("/root2");
  // Modify current time so that implementations can't accidentally pass unit tests by ignoring
  // this specified time and always using System.currentTimeMillis()
  private static final long TEST_CURRENT_TIME = 300;
  private static final long TTL_CHECKER_INTERVAL_MS = 1000;

  /**
   * The authenticate user is gotten from current thread local. If MasterInfo starts a concurrent
   * thread to do operations, {@link AuthenticatedClientUser} will be null. So
   * {@link AuthenticatedClientUser#set(String)} should be called in the {@link Callable#call()} to
   * set this user for testing.
   */
  private static final String TEST_AUTHENTICATE_USER = "test-user";

  @ClassRule
  public static ManuallyScheduleHeartbeat sManuallySchedule =
      new ManuallyScheduleHeartbeat(HeartbeatContext.MASTER_TTL_CHECK);

  @Rule
  public Timeout mGlobalTimeout = Timeout.seconds(60);

  @Rule
  public LocalAlluxioClusterResource mLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource(1000, Constants.GB,
          Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName(),
          Constants.MASTER_TTL_CHECKER_INTERVAL_MS, String.valueOf(TTL_CHECKER_INTERVAL_MS));

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  private Configuration mMasterConfiguration;
  private FileSystemMaster mFsMaster;

  @Before
  public final void before() throws Exception {
    // mock the authentication user
    AuthenticatedClientUser.set(TEST_AUTHENTICATE_USER);

    mFsMaster =
        mLocalAlluxioClusterResource.get().getMaster().getInternalMaster().getFileSystemMaster();
    mMasterConfiguration = mLocalAlluxioClusterResource.get().getMasterConf();

    TtlBucketPrivateAccess
        .setTtlIntervalMs(mMasterConfiguration.getLong(Constants.MASTER_TTL_CHECKER_INTERVAL_MS));
  }

  @Test
  public void clientFileInfoDirectoryTest() throws Exception {
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
    Assert.assertEquals(TEST_AUTHENTICATE_USER, fileInfo.getUserName());
    Assert.assertEquals(0755, (short) fileInfo.getPermission());
  }

  @Test
  public void clientFileInfoEmptyFileTest() throws Exception {
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
    Assert.assertEquals(TEST_AUTHENTICATE_USER, fileInfo.getUserName());
    Assert.assertEquals(0644, (short) fileInfo.getPermission());
  }

  private FileSystemMaster createFileSystemMasterFromJournal() throws IOException {
    return MasterTestUtils.createFileSystemMasterFromJournal(mMasterConfiguration);
  }

  // TODO(calvin): This test currently relies on the fact the HDFS client is a cached instance to
  // avoid invalid lease exception. This should be fixed.
  @Ignore
  @Test
  public void concurrentCreateJournalTest() throws Exception {
    // Makes sure the file id's are the same between a master info and the journal it creates
    for (int i = 0; i < 5; i++) {
      ConcurrentCreator concurrentCreator =
          new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
      concurrentCreator.call();

      FileSystemMaster fsMaster = createFileSystemMasterFromJournal();
      for (FileInfo info : mFsMaster.getFileInfoList(new AlluxioURI("/"))) {
        AlluxioURI path = new AlluxioURI(info.getPath());
        Assert.assertEquals(mFsMaster.getFileId(path), fsMaster.getFileId(path));
      }
      before();
    }
  }

  @Test
  public void concurrentCreateTest() throws Exception {
    ConcurrentCreator concurrentCreator =
        new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentCreator.call();
  }

  @Test
  public void concurrentDeleteTest() throws Exception {
    ConcurrentCreator concurrentCreator =
        new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentCreator.call();

    ConcurrentDeleter concurrentDeleter =
        new ConcurrentDeleter(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentDeleter.call();

    Assert.assertEquals(0,
        mFsMaster.getFileInfoList(new AlluxioURI("/")).size());
  }

  @Test
  public void concurrentFreeTest() throws Exception {
    ConcurrentCreator concurrentCreator =
        new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentCreator.call();

    ConcurrentFreer concurrentFreer =
        new ConcurrentFreer(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentFreer.call();
  }

  @Test
  public void concurrentRenameTest() throws Exception {
    ConcurrentCreator concurrentCreator =
        new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentCreator.call();

    int numFiles = mFsMaster.getFileInfoList(ROOT_PATH).size();

    ConcurrentRenamer concurrentRenamer = new ConcurrentRenamer(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH,
        ROOT_PATH2, AlluxioURI.EMPTY_URI);
    concurrentRenamer.call();

    Assert.assertEquals(numFiles,
        mFsMaster.getFileInfoList(ROOT_PATH2).size());
  }

  @Test
  public void createAlreadyExistFileTest() throws Exception {
    mThrown.expect(FileAlreadyExistsException.class);
    mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileOptions.defaults());
    mFsMaster.createDirectory(new AlluxioURI("/testFile"), CreateDirectoryOptions.defaults());
  }

  @Test
  public void createDirectoryTest() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    FileInfo fileInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertTrue(fileInfo.isFolder());
    Assert.assertEquals(TEST_AUTHENTICATE_USER, fileInfo.getUserName());
    Assert.assertEquals(0755, (short) fileInfo.getPermission());
  }

  @Test
  public void createFileInvalidPathTest() throws Exception {
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
  public void createFilePerfTest() throws Exception {
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
  public void createFileTest() throws Exception {
    mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileOptions.defaults());
    FileInfo fileInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFile")));
    Assert.assertFalse(fileInfo.isFolder());
    Assert.assertEquals(TEST_AUTHENTICATE_USER, fileInfo.getUserName());
    Assert.assertEquals(0644, (short) fileInfo.getPermission());
  }

  @Test
  public void deleteDirectoryWithDirectoriesTest() throws Exception {
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
    Assert.assertTrue(mFsMaster.delete(new AlluxioURI("/testFolder"), true));
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
  public void deleteDirectoryWithFilesTest() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long fileId =
        mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"), CreateFileOptions.defaults());
    Assert.assertEquals(1, mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    Assert.assertTrue(mFsMaster.delete(new AlluxioURI("/testFolder"), true));
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
  public void deleteEmptyDirectoryTest() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    Assert.assertEquals(1, mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertTrue(mFsMaster.delete(new AlluxioURI("/testFolder"), true));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID,
        mFsMaster.getFileId(new AlluxioURI("/testFolder")));
  }

  @Test
  public void deleteFileTest() throws Exception {
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileOptions.defaults());
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFile")));
    Assert.assertTrue(mFsMaster.delete(new AlluxioURI("/testFile"), true));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFsMaster.getFileId(new AlluxioURI("/testFile")));
  }

  @Test
  public void deleteRootTest() throws Exception {
    Assert.assertFalse(mFsMaster.delete(new AlluxioURI("/"), true));
    Assert.assertFalse(mFsMaster.delete(new AlluxioURI("/"), false));
  }

  @Test
  public void getCapacityBytesTest() {
    BlockMaster blockMaster =
        mLocalAlluxioClusterResource.get().getMaster().getInternalMaster().getBlockMaster();
    Assert.assertEquals(1000, blockMaster.getCapacityBytes());
  }

  @Test
  public void lastModificationTimeCompleteFileTest() throws Exception {
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFile"), CreateFileOptions.defaults());
    long opTimeMs = TEST_CURRENT_TIME;
    mFsMaster.completeFileInternal(Lists.<Long>newArrayList(), fileId, 0, opTimeMs);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals(opTimeMs, fileInfo.getLastModificationTimeMs());
  }

  @Test
  public void lastModificationTimeCreateFileTest() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long opTimeMs = TEST_CURRENT_TIME;
    CreateFileOptions options = CreateFileOptions.defaults().setOperationTimeMs(opTimeMs);
    mFsMaster.createFileInternal(new AlluxioURI("/testFolder/testFile"), options);
    FileInfo folderInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertEquals(opTimeMs, folderInfo.getLastModificationTimeMs());
  }

  @Test
  public void lastModificationTimeDeleteTest() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long fileId =
        mFsMaster.createFile(new AlluxioURI("/testFolder/testFile"), CreateFileOptions.defaults());
    long folderId = mFsMaster.getFileId(new AlluxioURI("/testFolder"));
    Assert.assertEquals(1, folderId);
    Assert.assertEquals(fileId, mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    long opTimeMs = TEST_CURRENT_TIME;
    Assert.assertTrue(mFsMaster.deleteInternal(fileId, true, true, opTimeMs));
    FileInfo folderInfo = mFsMaster.getFileInfo(folderId);
    Assert.assertEquals(opTimeMs, folderInfo.getLastModificationTimeMs());
  }

  @Test
  public void lastModificationTimeRenameTest() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long fileId =
        mFsMaster.createFile(new AlluxioURI("/testFolder/testFile1"), CreateFileOptions.defaults());
    long opTimeMs = TEST_CURRENT_TIME;
    mFsMaster.renameInternal(fileId, new AlluxioURI("/testFolder/testFile2"), true, opTimeMs);
    FileInfo folderInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder")));
    Assert.assertEquals(opTimeMs, folderInfo.getLastModificationTimeMs());
  }

  @Test
  public void listFilesTest() throws Exception {
    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(64);

    HashSet<Long> ids = new HashSet<Long>();
    HashSet<Long> dirIds = new HashSet<Long>();
    for (int i = 0; i < 10; i++) {
      AlluxioURI dir = new AlluxioURI("/i" + i);
      mFsMaster.createDirectory(dir, CreateDirectoryOptions.defaults());
      dirIds.add(mFsMaster.getFileId(dir));
      for (int j = 0; j < 10; j++) {
        ids.add(mFsMaster.createFile(dir.join("j" + j), options));
      }
    }
    HashSet<Long> listedIds = Sets.newHashSet();
    HashSet<Long> listedDirIds = Sets.newHashSet();
    List<FileInfo> infoList = mFsMaster.getFileInfoList(new AlluxioURI("/"));
    for (FileInfo info : infoList) {
      long id = info.getFileId();
      listedDirIds.add(id);
      for (FileInfo fileInfo : mFsMaster.getFileInfoList(new AlluxioURI(info.getPath()))) {
        listedIds.add(fileInfo.getFileId());
      }
    }
    Assert.assertEquals(ids, listedIds);
    Assert.assertEquals(dirIds, listedDirIds);
  }

  @Test
  public void lsTest() throws Exception {
    CreateFileOptions options = CreateFileOptions.defaults().setBlockSizeBytes(64);

    for (int i = 0; i < 10; i++) {
      mFsMaster.createDirectory(new AlluxioURI("/i" + i), CreateDirectoryOptions.defaults());
      for (int j = 0; j < 10; j++) {
        mFsMaster.createFile(new AlluxioURI("/i" + i + "/j" + j), options);
      }
    }

    Assert.assertEquals(1,
        mFsMaster.getFileInfoList(new AlluxioURI("/i0/j0")).size());
    for (int i = 0; i < 10; i++) {
      Assert.assertEquals(10,
          mFsMaster.getFileInfoList(new AlluxioURI("/i" + i)).size());
    }
    Assert.assertEquals(10,
        mFsMaster.getFileInfoList(new AlluxioURI("/")).size());
  }

  @Test
  public void notFileCompletionTest() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mFsMaster.createDirectory(new AlluxioURI("/testFile"), CreateDirectoryOptions.defaults());
    CompleteFileOptions options = CompleteFileOptions.defaults();
    mFsMaster.completeFile(new AlluxioURI("/testFile"), options);
  }

  @Test
  public void renameExistingDstTest() throws Exception {
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
  public void renameNonexistentTest() throws Exception {
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
  public void ttlCreateFileTest() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long ttl = 100;
    CreateFileOptions options = CreateFileOptions.defaults().setTtl(ttl);
    mFsMaster.createFileInternal(new AlluxioURI("/testFolder/testFile"), options);
    FileInfo folderInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile")));
    Assert.assertEquals(ttl, folderInfo.getTtl());
  }

  @Test
  public void ttlExpiredCreateFileTest() throws Exception {
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
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_TTL_CHECK, 10,
        TimeUnit.SECONDS));
    HeartbeatScheduler.schedule(HeartbeatContext.MASTER_TTL_CHECK);
    Assert.assertTrue(HeartbeatScheduler.await(HeartbeatContext.MASTER_TTL_CHECK, 10,
        TimeUnit.SECONDS));
    mThrown.expect(FileDoesNotExistException.class);
    mFsMaster.getFileInfo(fileId);
  }

  @Test
  public void ttlRenameTest() throws Exception {
    mFsMaster.createDirectory(new AlluxioURI("/testFolder"), CreateDirectoryOptions.defaults());
    long ttl = 1;
    CreateFileOptions options = CreateFileOptions.defaults().setTtl(ttl);
    long fileId = mFsMaster.createFile(new AlluxioURI("/testFolder/testFile1"), options);
    mFsMaster.renameInternal(fileId, new AlluxioURI("/testFolder/testFile2"), true,
        TEST_CURRENT_TIME);
    FileInfo folderInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new AlluxioURI("/testFolder/testFile2")));
    Assert.assertEquals(ttl, folderInfo.getTtl());
  }

  // TODO(gene): Journal format has changed, maybe add Version to the format and add this test back
  // or remove this test when we have better tests against journal checkpoint.
  // @Test
  // public void writeImageTest() throws IOException {
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
}
