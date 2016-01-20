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

package tachyon.master.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import tachyon.Constants;
import tachyon.LocalTachyonClusterResource;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.exception.DirectoryNotEmptyException;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.master.MasterContext;
import tachyon.master.MasterTestUtils;
import tachyon.master.block.BlockMaster;
import tachyon.master.file.meta.TtlBucketPrivateAccess;
import tachyon.master.file.options.CompleteFileOptions;
import tachyon.master.file.options.CreateOptions;
import tachyon.master.file.options.MkdirOptions;
import tachyon.security.User;
import tachyon.security.authentication.AuthType;
import tachyon.security.authentication.PlainSaslServer.AuthorizedClientUser;
import tachyon.thrift.FileInfo;
import tachyon.util.CommonUtils;
import tachyon.util.IdUtils;

/**
 * Test behavior of {@link FileSystemMaster}.
 *
 * For example, (concurrently) creating/deleting/renaming files.
 */
public class FileSystemMasterIntegrationTest {
  class ConcurrentCreator implements Callable<Void> {
    private int mDepth;
    private int mConcurrencyDepth;
    private TachyonURI mInitPath;

    ConcurrentCreator(int depth, int concurrencyDepth, TachyonURI initPath) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mInitPath = initPath;
    }

    @Override
    public Void call() throws Exception {
      AuthorizedClientUser.set(TEST_AUTHENTICATE_USER);
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

    public void exec(int depth, int concurrencyDepth, TachyonURI path) throws Exception {
      if (depth < 1) {
        return;
      } else if (depth == 1) {
        long fileId = mFsMaster.create(path, CreateOptions.defaults());
        Assert.assertEquals(fileId, mFsMaster.getFileId(path));
        // verify the user permission for file
        FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
        Assert.assertEquals(TEST_AUTHENTICATE_USER, fileInfo.getUserName());
        Assert.assertEquals(0644, (short)fileInfo.getPermission());
      } else {
        mFsMaster.mkdir(path, MkdirOptions.defaults());
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
          for (int i = 0; i < FILES_PER_NODE; i ++) {
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
        for (int i = 0; i < FILES_PER_NODE; i ++) {
          exec(depth - 1, concurrencyDepth, path.join(Integer.toString(i)));
        }
      }
    }
  }

  class ConcurrentDeleter implements Callable<Void> {
    private int mDepth;
    private int mConcurrencyDepth;
    private TachyonURI mInitPath;

    ConcurrentDeleter(int depth, int concurrencyDepth, TachyonURI initPath) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mInitPath = initPath;
    }

    @Override
    public Void call() throws Exception {
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

    private void doDelete(TachyonURI path) throws Exception {
      mFsMaster.deleteFile(mFsMaster.getFileId(path), true);
      Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFsMaster.getFileId(path));
    }

    public void exec(int depth, int concurrencyDepth, TachyonURI path) throws Exception {
      if (depth < 1) {
        return;
      } else if (depth == 1 || (path.hashCode() % 10 == 0)) {
        // Sometimes we want to try deleting a path when we're not all the way down, which is what
        // the second condition is for
        doDelete(path);
      } else {
        if (concurrencyDepth > 0) {
          ExecutorService executor = Executors.newCachedThreadPool();
          try {
            ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>(FILES_PER_NODE);
            for (int i = 0; i < FILES_PER_NODE; i ++) {
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
          for (int i = 0; i < FILES_PER_NODE; i ++) {
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
    private TachyonURI mRootPath;
    private TachyonURI mRootPath2;
    private TachyonURI mInitPath;

    ConcurrentRenamer(int depth, int concurrencyDepth, TachyonURI rootPath, TachyonURI rootPath2,
        TachyonURI initPath) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mRootPath = rootPath;
      mRootPath2 = rootPath2;
      mInitPath = initPath;
    }

    @Override
    public Void call() throws Exception {
      AuthorizedClientUser.set(TEST_AUTHENTICATE_USER);
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

    public void exec(int depth, int concurrencyDepth, TachyonURI path) throws Exception {
      if (depth < 1) {
        return;
      } else if (depth == 1 || (depth < mDepth && path.hashCode() % 10 < 3)) {
        // Sometimes we want to try renaming a path when we're not all the way down, which is what
        // the second condition is for. We have to create the path in the destination up till what
        // we're renaming. This might already exist, so createFile could throw a
        // FileAlreadyExistsException, which we silently handle.
        TachyonURI srcPath = mRootPath.join(path);
        TachyonURI dstPath = mRootPath2.join(path);
        long fileId = mFsMaster.getFileId(srcPath);
        try {
          MkdirOptions options =
              new MkdirOptions.Builder(MasterContext.getConf()).setRecursive(true).build();
          mFsMaster.mkdir(dstPath.getParent(), options);
        } catch (FileAlreadyExistsException e) {
          // This is an acceptable exception to get, since we don't know if the parent has been
          // created yet by another thread.
        } catch (InvalidPathException e) {
          // This could happen if we are renaming something that's a child of the root.
        }
        mFsMaster.rename(fileId, dstPath);
        Assert.assertEquals(fileId, mFsMaster.getFileId(dstPath));
      } else if (concurrencyDepth > 0) {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
          ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>(FILES_PER_NODE);
          for (int i = 0; i < FILES_PER_NODE; i ++) {
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
        for (int i = 0; i < FILES_PER_NODE; i ++) {
          exec(depth - 1, concurrencyDepth, path.join(Integer.toString(i)));
        }
      }
    }
  }

  private static final int DEPTH = 6;
  private static final int FILES_PER_NODE = 4;
  private static final int CONCURRENCY_DEPTH = 3;
  private static final TachyonURI ROOT_PATH = new TachyonURI("/root");
  private static final TachyonURI ROOT_PATH2 = new TachyonURI("/root2");
  // Modify current time so that implementations can't accidentally pass unit tests by ignoring
  // this specified time and always using System.currentTimeMillis()
  private static final long TEST_CURRENT_TIME = 300;

  /**
   * The authenticate user is gotten from current thread local. If MasterInfo starts a concurrent
   * thread to do operations, {@link AuthorizedClientUser} will be null. So
   * {@link AuthorizedClientUser#set(String)} should be called in the {@link Callable#call()} to
   * set this user for testing.
   */
  private static final String TEST_AUTHENTICATE_USER = "test-user";

  @Rule
  public LocalTachyonClusterResource mLocalTachyonClusterResource =
      new LocalTachyonClusterResource(1000, 1000, Constants.GB,
          Constants.SECURITY_AUTHENTICATION_TYPE, AuthType.SIMPLE.getAuthName());
  private TachyonConf mMasterTachyonConf;
  private FileSystemMaster mFsMaster;
  private User mOldUser;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @After
  public final void after() throws Exception {
    if (mOldUser == null) {
      AuthorizedClientUser.remove();
    } else {
      AuthorizedClientUser.set(mOldUser.getName());
    }
  }

  @Before
  public final void before() throws Exception {
    // mock the authentication user
    mOldUser = AuthorizedClientUser.get();
    AuthorizedClientUser.set(TEST_AUTHENTICATE_USER);

    mFsMaster =
        mLocalTachyonClusterResource.get().getMaster().getInternalMaster().getFileSystemMaster();
    mMasterTachyonConf = mLocalTachyonClusterResource.get().getMasterTachyonConf();

    // restore the ttl interval to the correct value in case it was set to a different value when
    // the TtlBucket class was statically initialized
    // TODO(andrew): prevent tests from interfering with each other through this value
    TtlBucketPrivateAccess
        .setTtlIntervalMs(mMasterTachyonConf.getLong(Constants.MASTER_TTLCHECKER_INTERVAL_MS));
  }

  @Test
  public void clientFileInfoDirectoryTest() throws Exception {
    TachyonURI path = new TachyonURI("/testFolder");
    mFsMaster.mkdir(path, MkdirOptions.defaults());
    long fileId = mFsMaster.getFileId(path);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("testFolder", fileInfo.getName());
    Assert.assertEquals(1, fileInfo.getFileId());
    Assert.assertEquals(0, fileInfo.getLength());
    Assert.assertFalse(fileInfo.isCacheable);
    Assert.assertTrue(fileInfo.isCompleted);
    Assert.assertTrue(fileInfo.isFolder);
    Assert.assertFalse(fileInfo.isPersisted);
    Assert.assertFalse(fileInfo.isPinned);
    Assert.assertEquals(TEST_AUTHENTICATE_USER, fileInfo.getUserName());
    Assert.assertEquals(0755, (short) fileInfo.getPermission());
  }

  @Test
  public void clientFileInfoEmptyFileTest() throws Exception {
    long fileId = mFsMaster.create(new TachyonURI("/testFile"), CreateOptions.defaults());
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("testFile", fileInfo.getName());
    Assert.assertEquals(fileId, fileInfo.getFileId());
    Assert.assertEquals(0, fileInfo.getLength());
    Assert.assertTrue(fileInfo.isCacheable);
    Assert.assertFalse(fileInfo.isCompleted);
    Assert.assertFalse(fileInfo.isFolder);
    Assert.assertFalse(fileInfo.isPersisted);
    Assert.assertFalse(fileInfo.isPinned);
    Assert.assertEquals(Constants.NO_TTL, fileInfo.ttl);
    Assert.assertEquals(TEST_AUTHENTICATE_USER, fileInfo.getUserName());
    Assert.assertEquals(0644, (short)fileInfo.getPermission());
  }

  private FileSystemMaster createFileSystemMasterFromJournal() throws IOException {
    return MasterTestUtils.createFileSystemMasterFromJournal(mMasterTachyonConf);
  }

  // TODO(calvin): This test currently relies on the fact the HDFS client is a cached instance to
  // avoid invalid lease exception. This should be fixed.
  @Ignore
  @Test
  public void concurrentCreateJournalTest() throws Exception {
    // Makes sure the file id's are the same between a master info and the journal it creates
    for (int i = 0; i < 5; i ++) {
      ConcurrentCreator concurrentCreator =
          new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
      concurrentCreator.call();

      FileSystemMaster fsMaster = createFileSystemMasterFromJournal();
      for (FileInfo info : mFsMaster.getFileInfoList(mFsMaster.getFileId(new TachyonURI("/")))) {
        TachyonURI path = new TachyonURI(info.getPath());
        Assert.assertEquals(mFsMaster.getFileId(path), fsMaster.getFileId(path));
      }
      after();
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
        mFsMaster.getFileInfoList(mFsMaster.getFileId(new TachyonURI("/"))).size());
  }

  @Test
  public void concurrentRenameTest() throws Exception {
    ConcurrentCreator concurrentCreator =
        new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH);
    concurrentCreator.call();

    int numFiles = mFsMaster.getFileInfoList(mFsMaster.getFileId(ROOT_PATH)).size();

    ConcurrentRenamer concurrentRenamer = new ConcurrentRenamer(DEPTH, CONCURRENCY_DEPTH, ROOT_PATH,
        ROOT_PATH2, TachyonURI.EMPTY_URI);
    concurrentRenamer.call();

    Assert.assertEquals(numFiles,
        mFsMaster.getFileInfoList(mFsMaster.getFileId(ROOT_PATH2)).size());
  }

  @Test
  public void createAlreadyExistFileTest() throws Exception {
    mThrown.expect(FileAlreadyExistsException.class);
    mFsMaster.create(new TachyonURI("/testFile"), CreateOptions.defaults());
    mFsMaster.mkdir(new TachyonURI("/testFile"), MkdirOptions.defaults());
  }

  @Test
  public void createDirectoryTest() throws Exception {
    mFsMaster.mkdir(new TachyonURI("/testFolder"), MkdirOptions.defaults());
    FileInfo fileInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertTrue(fileInfo.isFolder);
    Assert.assertEquals(TEST_AUTHENTICATE_USER, fileInfo.getUserName());
    Assert.assertEquals(0755, (short) fileInfo.getPermission());
  }

  @Test
  public void createFileInvalidPathTest() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mFsMaster.create(new TachyonURI("testFile"), CreateOptions.defaults());
  }

  @Test
  public void createFileInvalidPathTest2() throws Exception {
    mThrown.expect(FileAlreadyExistsException.class);
    mFsMaster.create(new TachyonURI("/"), CreateOptions.defaults());
  }

  @Test
  public void createFileInvalidPathTest3() throws Exception {
    mThrown.expect(InvalidPathException.class);
    mFsMaster.create(new TachyonURI("/testFile1"), CreateOptions.defaults());
    mFsMaster.create(new TachyonURI("/testFile1/testFile2"), CreateOptions.defaults());
  }

  @Test
  public void createFilePerfTest() throws Exception {
    for (int k = 0; k < 200; k ++) {
      MkdirOptions options =
          new MkdirOptions.Builder(MasterContext.getConf()).setRecursive(true).build();
      mFsMaster.mkdir(
          new TachyonURI("/testFile").join(Constants.MASTER_COLUMN_FILE_PREFIX + k).join("0"),
          options);
    }
    for (int k = 0; k < 200; k ++) {
      mFsMaster.getFileInfo(mFsMaster.getFileId(
          new TachyonURI("/testFile").join(Constants.MASTER_COLUMN_FILE_PREFIX + k).join("0")));
    }
  }

  @Test
  public void createFileTest() throws Exception {
    mFsMaster.create(new TachyonURI("/testFile"), CreateOptions.defaults());
    FileInfo fileInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI("/testFile")));
    Assert.assertFalse(fileInfo.isFolder);
    Assert.assertEquals(TEST_AUTHENTICATE_USER, fileInfo.getUserName());
    Assert.assertEquals(0644, (short) fileInfo.getPermission());
  }

  @Test
  public void deleteDirectoryWithDirectoriesTest() throws Exception {
    mFsMaster.mkdir(new TachyonURI("/testFolder"), MkdirOptions.defaults());
    mFsMaster.mkdir(new TachyonURI("/testFolder/testFolder2"), MkdirOptions.defaults());
    long fileId =
        mFsMaster.create(new TachyonURI("/testFolder/testFile"), CreateOptions.defaults());
    long fileId2 = mFsMaster.create(new TachyonURI("/testFolder/testFolder2/testFile2"),
        CreateOptions.defaults());
    Assert.assertEquals(1, mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(2, mFsMaster.getFileId(new TachyonURI("/testFolder/testFolder2")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFolder/testFile")));
    Assert.assertEquals(fileId2,
        mFsMaster.getFileId(new TachyonURI("/testFolder/testFolder2/testFile2")));
    Assert.assertTrue(mFsMaster.deleteFile(1, true));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID,
        mFsMaster.getFileId(new TachyonURI("/testFolder/testFolder2/testFile2")));
  }

  @Test
  public void deleteDirectoryWithDirectoriesTest2() throws Exception {
    mFsMaster.mkdir(new TachyonURI("/testFolder"), MkdirOptions.defaults());
    mFsMaster.mkdir(new TachyonURI("/testFolder/testFolder2"), MkdirOptions.defaults());
    long fileId =
        mFsMaster.create(new TachyonURI("/testFolder/testFile"), CreateOptions.defaults());
    long fileId2 = mFsMaster.create(new TachyonURI("/testFolder/testFolder2/testFile2"),
        CreateOptions.defaults());
    Assert.assertEquals(1, mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(2, mFsMaster.getFileId(new TachyonURI("/testFolder/testFolder2")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFolder/testFile")));
    Assert.assertEquals(fileId2,
        mFsMaster.getFileId(new TachyonURI("/testFolder/testFolder2/testFile2")));
    try {
      mFsMaster.deleteFile(2, false);
      Assert.fail("Deleting a nonempty directory nonrecursively should fail");
    } catch (DirectoryNotEmptyException e) {
      Assert.assertEquals(
          ExceptionMessage.DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE.getMessage("testFolder2"),
          e.getMessage());
    }
    Assert.assertEquals(1, mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(2, mFsMaster.getFileId(new TachyonURI("/testFolder/testFolder2")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFolder/testFile")));
    Assert.assertEquals(fileId2,
        mFsMaster.getFileId(new TachyonURI("/testFolder/testFolder2/testFile2")));
  }

  @Test
  public void deleteDirectoryWithFilesTest() throws Exception {
    mFsMaster.mkdir(new TachyonURI("/testFolder"), MkdirOptions.defaults());
    long fileId =
        mFsMaster.create(new TachyonURI("/testFolder/testFile"), CreateOptions.defaults());
    Assert.assertEquals(1, mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFolder/testFile")));
    Assert.assertTrue(mFsMaster.deleteFile(1, true));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID,
        mFsMaster.getFileId(new TachyonURI("/testFolder")));
  }

  @Test
  public void deleteDirectoryWithFilesTest2() throws Exception {
    mFsMaster.mkdir(new TachyonURI("/testFolder"), MkdirOptions.defaults());
    long fileId =
        mFsMaster.create(new TachyonURI("/testFolder/testFile"), CreateOptions.defaults());
    Assert.assertEquals(1, mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFolder/testFile")));
    try {
      mFsMaster.deleteFile(1, false);
      Assert.fail("Deleting a nonempty directory nonrecursively should fail");
    } catch (DirectoryNotEmptyException e) {
      Assert.assertEquals(
          ExceptionMessage.DELETE_NONEMPTY_DIRECTORY_NONRECURSIVE.getMessage("testFolder"),
          e.getMessage());
    }
    Assert.assertEquals(1, mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFolder/testFile")));
  }

  @Test
  public void deleteEmptyDirectoryTest() throws Exception {
    mFsMaster.mkdir(new TachyonURI("/testFolder"), MkdirOptions.defaults());
    Assert.assertEquals(1, mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertTrue(mFsMaster.deleteFile(1, true));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID,
        mFsMaster.getFileId(new TachyonURI("/testFolder")));
  }

  @Test
  public void deleteFileTest() throws Exception {
    long fileId = mFsMaster.create(new TachyonURI("/testFile"), CreateOptions.defaults());
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFile")));
    Assert.assertTrue(mFsMaster.deleteFile(fileId, true));
    Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFsMaster.getFileId(new TachyonURI("/testFile")));
  }

  @Test
  public void deleteRootTest() throws Exception {
    long rootId = mFsMaster.getFileId(new TachyonURI("/"));
    Assert.assertFalse(mFsMaster.deleteFile(rootId, true));
    Assert.assertFalse(mFsMaster.deleteFile(rootId, false));
  }

  @Test
  public void getCapacityBytesTest() {
    BlockMaster blockMaster =
        mLocalTachyonClusterResource.get().getMaster().getInternalMaster().getBlockMaster();
    Assert.assertEquals(1000, blockMaster.getCapacityBytes());
  }

  @Test
  public void lastModificationTimeCompleteFileTest() throws Exception {
    long fileId = mFsMaster.create(new TachyonURI("/testFile"), CreateOptions.defaults());
    long opTimeMs = TEST_CURRENT_TIME;
    mFsMaster.completeFileInternal(Lists.<Long>newArrayList(), fileId, 0, opTimeMs);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals(opTimeMs, fileInfo.lastModificationTimeMs);
  }

  @Test
  public void lastModificationTimeCreateFileTest() throws Exception {
    mFsMaster.mkdir(new TachyonURI("/testFolder"), MkdirOptions.defaults());
    long opTimeMs = TEST_CURRENT_TIME;
    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setOperationTimeMs(opTimeMs).build();
    mFsMaster.createInternal(new TachyonURI("/testFolder/testFile"), options);
    FileInfo folderInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(opTimeMs, folderInfo.lastModificationTimeMs);
  }

  @Test
  public void lastModificationTimeDeleteTest() throws Exception {
    mFsMaster.mkdir(new TachyonURI("/testFolder"), MkdirOptions.defaults());
    long fileId =
        mFsMaster.create(new TachyonURI("/testFolder/testFile"), CreateOptions.defaults());
    long folderId = mFsMaster.getFileId(new TachyonURI("/testFolder"));
    Assert.assertEquals(1, folderId);
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFolder/testFile")));
    long opTimeMs = TEST_CURRENT_TIME;
    Assert.assertTrue(mFsMaster.deleteFileInternal(fileId, true, true, opTimeMs));
    FileInfo folderInfo = mFsMaster.getFileInfo(folderId);
    Assert.assertEquals(opTimeMs, folderInfo.lastModificationTimeMs);
  }

  @Test
  public void lastModificationTimeRenameTest() throws Exception {
    mFsMaster.mkdir(new TachyonURI("/testFolder"), MkdirOptions.defaults());
    long fileId =
        mFsMaster.create(new TachyonURI("/testFolder/testFile1"), CreateOptions.defaults());
    long opTimeMs = TEST_CURRENT_TIME;
    mFsMaster.renameInternal(fileId, new TachyonURI("/testFolder/testFile2"), true, opTimeMs);
    FileInfo folderInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(opTimeMs, folderInfo.lastModificationTimeMs);
  }

  @Test
  public void listFilesTest() throws Exception {
    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(64).build();

    HashSet<Long> ids = new HashSet<Long>();
    HashSet<Long> dirIds = new HashSet<Long>();
    for (int i = 0; i < 10; i ++) {
      TachyonURI dir = new TachyonURI("/i" + i);
      mFsMaster.mkdir(dir, MkdirOptions.defaults());
      dirIds.add(mFsMaster.getFileId(dir));
      for (int j = 0; j < 10; j ++) {
        ids.add(mFsMaster.create(dir.join("j" + j), options));
      }
    }
    HashSet<Long> listedIds = Sets.newHashSet();
    HashSet<Long> listedDirIds = Sets.newHashSet();
    List<FileInfo> infoList = mFsMaster.getFileInfoList(mFsMaster.getFileId(new TachyonURI("/")));
    for (FileInfo info : infoList) {
      long id = info.getFileId();
      listedDirIds.add(id);
      for (FileInfo fileInfo : mFsMaster.getFileInfoList(id)) {
        listedIds.add(fileInfo.getFileId());
      }
    }
    Assert.assertEquals(ids, listedIds);
    Assert.assertEquals(dirIds, listedDirIds);
  }

  @Test
  public void lsTest() throws Exception {
    CreateOptions options =
        new CreateOptions.Builder(MasterContext.getConf()).setBlockSizeBytes(64).build();

    for (int i = 0; i < 10; i ++) {
      mFsMaster.mkdir(new TachyonURI("/i" + i), MkdirOptions.defaults());
      for (int j = 0; j < 10; j ++) {
        mFsMaster.create(new TachyonURI("/i" + i + "/j" + j), options);
      }
    }

    Assert.assertEquals(1,
        mFsMaster.getFileInfoList(mFsMaster.getFileId(new TachyonURI("/i0/j0"))).size());
    for (int i = 0; i < 10; i ++) {
      Assert.assertEquals(10,
          mFsMaster.getFileInfoList(mFsMaster.getFileId(new TachyonURI("/i" + i))).size());
    }
    Assert.assertEquals(10,
        mFsMaster.getFileInfoList(mFsMaster.getFileId(new TachyonURI("/"))).size());
  }

  @Test
  public void notFileCompletionTest() throws Exception {
    mThrown.expect(FileDoesNotExistException.class);
    mFsMaster.mkdir(new TachyonURI("/testFile"), MkdirOptions.defaults());
    CompleteFileOptions options = CompleteFileOptions.defaults();
    mFsMaster.completeFile(mFsMaster.getFileId(new TachyonURI("/testFile")), options);
  }

  @Test
  public void renameExistingDstTest() throws Exception {
    mFsMaster.create(new TachyonURI("/testFile1"), CreateOptions.defaults());
    mFsMaster.create(new TachyonURI("/testFile2"), CreateOptions.defaults());
    Assert.assertFalse(mFsMaster.rename(mFsMaster.getFileId(new TachyonURI("/testFile1")),
        new TachyonURI("/testFile2")));
  }

  @Test
  public void renameNonexistentTest() throws Exception {
    mFsMaster.create(new TachyonURI("/testFile1"), CreateOptions.defaults());
    Assert.assertEquals(IdUtils.INVALID_FILE_ID, mFsMaster.getFileId(new TachyonURI("/testFile2")));
  }

  @Test
  public void renameToDeeper() throws Exception {
    CreateOptions createOptions =
        new CreateOptions.Builder(MasterContext.getConf()).setRecursive(true).build();
    MkdirOptions mkdirOptions =
        new MkdirOptions.Builder(MasterContext.getConf()).setRecursive(true).build();
    mThrown.expect(InvalidPathException.class);
    mFsMaster.mkdir(new TachyonURI("/testDir1/testDir2"), mkdirOptions);
    mFsMaster.create(new TachyonURI("/testDir1/testDir2/testDir3/testFile3"), createOptions);
    mFsMaster.rename(mFsMaster.getFileId(new TachyonURI("/testDir1/testDir2")),
        new TachyonURI("/testDir1/testDir2/testDir3/testDir4"));
  }

  @Test
  public void ttlCreateFileTest() throws Exception {
    mFsMaster.mkdir(new TachyonURI("/testFolder"), MkdirOptions.defaults());
    long ttl = 100;
    CreateOptions options = new CreateOptions.Builder(MasterContext.getConf()).setTtl(ttl).build();
    mFsMaster.createInternal(new TachyonURI("/testFolder/testFile"), options);
    FileInfo folderInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI("/testFolder/testFile")));
    Assert.assertEquals(ttl, folderInfo.ttl);
  }

  @Test
  public void ttlExpiredCreateFileTest() throws Exception {
    mFsMaster.mkdir(new TachyonURI("/testFolder"), MkdirOptions.defaults());
    long ttl = 1;
    CreateOptions options = new CreateOptions.Builder(MasterContext.getConf()).setTtl(ttl).build();
    long fileId = mFsMaster.create(new TachyonURI("/testFolder/testFile1"), options);
    FileInfo folderInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI("/testFolder/testFile1")));
    Assert.assertEquals(fileId, folderInfo.fileId);
    Assert.assertEquals(ttl, folderInfo.ttl);
    CommonUtils.sleepMs(5000);
    mThrown.expect(FileDoesNotExistException.class);
    mFsMaster.getFileInfo(fileId);
  }

  @Test
  public void ttlRenameTest() throws Exception {
    mFsMaster.mkdir(new TachyonURI("/testFolder"), MkdirOptions.defaults());
    long ttl = 1;
    CreateOptions options = new CreateOptions.Builder(MasterContext.getConf()).setTtl(ttl).build();
    long fileId = mFsMaster.create(new TachyonURI("/testFolder/testFile1"), options);
    mFsMaster.renameInternal(fileId, new TachyonURI("/testFolder/testFile2"), true,
        TEST_CURRENT_TIME);
    FileInfo folderInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI("/testFolder/testFile2")));
    Assert.assertEquals(ttl, folderInfo.ttl);
  }

  // TODO(gene): Journal format has changed, maybe add Version to the format and add this test back
  // or remove this test when we have better tests against journal checkpoint.
  // @Test
  // public void writeImageTest() throws IOException {
  // // initialize the MasterInfo
  // Journal journal =
  // new Journal(mLocalTachyonCluster.getTachyonHome() + "journal/", "image.data", "log.data",
  // mMasterTachyonConf);
  // Journal
  // MasterInfo info =
  // new MasterInfo(new InetSocketAddress(9999), journal, mExecutorService, mMasterTachyonConf);

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
