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

import java.io.FileNotFoundException;
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
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;
import tachyon.master.LocalTachyonCluster;
import tachyon.master.MasterTestUtils;
import tachyon.master.block.BlockMaster;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.SuspectedFileSizeException;
import tachyon.thrift.TachyonException;
import tachyon.util.io.PathUtils;

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
      exec(mDepth, mConcurrencyDepth, mInitPath);
      return null;
    }

    public void exec(int depth, int concurrencyDepth, TachyonURI path) throws Exception {
      if (depth < 1) {
        return;
      } else if (depth == 1) {
        long fileId = mFsMaster.createFile(path, Constants.DEFAULT_BLOCK_SIZE_BYTE, false);
        Assert.assertEquals(fileId, mFsMaster.getFileId(path));
      } else {
        mFsMaster.mkdirs(path, false);
        Assert.assertNotNull(mFsMaster.getFileId(path));
      }

      if (concurrencyDepth > 0) {
        ExecutorService executor = Executors.newCachedThreadPool();
        ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>(FILES_PER_NODE);
        for (int i = 0; i < FILES_PER_NODE; i ++) {
          Callable<Void> call =
              (new ConcurrentCreator(depth - 1, concurrencyDepth - 1,
                  path.join(Integer.toString(i))));
          futures.add(executor.submit(call));
        }
        for (Future<Void> f : futures) {
          f.get();
        }
        executor.shutdown();
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
      boolean exception = false;
      try {
        mFsMaster.getFileId(path);
      } catch (InvalidPathException ipe) {
        exception = true;
      }
      Assert.assertTrue(exception);
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
          ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>(FILES_PER_NODE);
          for (int i = 0; i < FILES_PER_NODE; i ++) {
            Callable<Void> call =
                (new ConcurrentDeleter(depth - 1, concurrencyDepth - 1, path.join(Integer
                    .toString(i))));
            futures.add(executor.submit(call));
          }
          for (Future<Void> f : futures) {
            f.get();
          }
          executor.shutdown();
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
    private TachyonURI mRoot;
    private TachyonURI mRoot2;
    private TachyonURI mInitPath;

    ConcurrentRenamer(int depth, int concurrencyDepth, TachyonURI rootPath, TachyonURI rootPath2,
        TachyonURI initPath) {
      mDepth = depth;
      mConcurrencyDepth = concurrencyDepth;
      mRoot = rootPath;
      mRoot2 = rootPath2;
      mInitPath = initPath;
    }

    @Override
    public Void call() throws Exception {
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
        // FileAlreadyExistException, which we silently handle.
        TachyonURI srcPath = mRoot.join(path);
        TachyonURI dstPath = mRoot2.join(path);
        long fileId = mFsMaster.getFileId(srcPath);
        try {
          mFsMaster.mkdirs(dstPath.getParent(), true);
        } catch (FileAlreadyExistException e) {
          // This is an acceptable exception to get, since we don't know if the parent has been
          // created yet by another thread.
        } catch (InvalidPathException e) {
          // This could happen if we are renaming something that's a child of the root.
        }
        mFsMaster.rename(fileId, dstPath);
        Assert.assertEquals(fileId, mFsMaster.getFileId(dstPath));
      } else if (concurrencyDepth > 0) {
        ExecutorService executor = Executors.newCachedThreadPool();
        ArrayList<Future<Void>> futures = new ArrayList<Future<Void>>(FILES_PER_NODE);
        for (int i = 0; i < FILES_PER_NODE; i ++) {
          Callable<Void> call =
              (new ConcurrentRenamer(depth - 1, concurrencyDepth - 1, mRoot, mRoot2,
                  path.join(Integer.toString(i))));
          futures.add(executor.submit(call));
        }
        for (Future<Void> f : futures) {
          f.get();
        }
        executor.shutdown();
      } else {
        for (int i = 0; i < FILES_PER_NODE; i ++) {
          exec(depth - 1, concurrencyDepth, path.join(Integer.toString(i)));
        }
      }
    }
  }

  private LocalTachyonCluster mLocalTachyonCluster = null;
  private String mMountPoint;
  private TachyonURI mRoot;
  private TachyonURI mRoot2;
  private FileSystemMaster mFsMaster;

  private static final int DEPTH = 6;

  private static final int FILES_PER_NODE = 4;

  private static final int CONCURRENCY_DEPTH = 3;

  private ExecutorService mExecutorService = null;

  private TachyonConf mMasterTachyonConf;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void addCheckpointTest() throws FileDoesNotExistException, SuspectedFileSizeException,
      FileAlreadyExistException, InvalidPathException, BlockInfoException, TachyonException,
      IOException {
    TachyonURI uri = new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile"));
    long fileId = mFsMaster.createFile(uri, Constants.DEFAULT_BLOCK_SIZE_BYTE, false);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertFalse(fileInfo.isIsPersisted());
    mFsMaster.addCheckpoint(-1, fileId, 1,
        new TachyonURI(PathUtils.concatPath(mMountPoint, "testPath")));
    fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertTrue(fileInfo.isIsPersisted());
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    mExecutorService.shutdown();
  }

  @Before
  public final void before() throws Exception {
    mLocalTachyonCluster = new LocalTachyonCluster(1000, 1000, Constants.GB);
    mLocalTachyonCluster.start();
    mMountPoint = mLocalTachyonCluster.getMountPoint();
    mRoot = new TachyonURI(PathUtils.concatPath(mMountPoint, "root"));
    mRoot2 = new TachyonURI(PathUtils.concatPath(mMountPoint, "root2"));
    mExecutorService = Executors.newFixedThreadPool(2);
    mFsMaster = mLocalTachyonCluster.getMaster().getInternalMaster().getFileSystemMaster();
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
  }

  @Test
  public void clientFileInfoDirectoryTest() throws InvalidPathException, FileDoesNotExistException,
      FileAlreadyExistException, TachyonException {
    TachyonURI uri = new TachyonURI(PathUtils.concatPath(mMountPoint, "testDir"));
    mFsMaster.mkdirs(uri, true);
    long fileId = mFsMaster.getFileId(uri);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("testDir", fileInfo.getName());
    Assert.assertEquals(0, fileInfo.getLength());
    Assert.assertEquals(PathUtils.concatPath(mLocalTachyonCluster.getTachyonHome(), "testDir"),
        fileInfo.getUfsPath());
    Assert.assertTrue(fileInfo.isFolder);
    Assert.assertFalse(fileInfo.isPinned);
    Assert.assertFalse(fileInfo.isCacheable);
    Assert.assertTrue(fileInfo.isCompleted);
  }

  @Test
  public void clientFileInfoEmptyFileTest() throws InvalidPathException, FileDoesNotExistException,
      FileAlreadyExistException, BlockInfoException, TachyonException {
    TachyonURI uri = new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile"));
    long fileId = mFsMaster.createFile(uri, Constants.DEFAULT_BLOCK_SIZE_BYTE, false);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("testFile", fileInfo.getName());
    Assert.assertEquals(fileId, fileInfo.getFileId());
    Assert.assertEquals(0, fileInfo.getLength());
    Assert.assertEquals(PathUtils.concatPath(mLocalTachyonCluster.getTachyonHome(), "testFile"),
        fileInfo.getUfsPath());
    Assert.assertFalse(fileInfo.isFolder);
    Assert.assertFalse(fileInfo.isPinned);
    Assert.assertTrue(fileInfo.isCacheable);
    Assert.assertFalse(fileInfo.isCompleted);
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
      ConcurrentCreator concurrentCreator = new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, mRoot);
      concurrentCreator.call();

      FileSystemMaster fsMaster = createFileSystemMasterFromJournal();
      TachyonURI mntPath = new TachyonURI(mMountPoint);
      for (FileInfo info : mFsMaster.getFileInfoList(mFsMaster.getFileId(mntPath))) {
        TachyonURI path = new TachyonURI(info.getPath());
        Assert.assertEquals(mFsMaster.getFileId(path), fsMaster.getFileId(path));
      }
      after();
      before();
    }
  }

  @Test
  public void concurrentCreateTest() throws Exception {
    ConcurrentCreator concurrentCreator = new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, mRoot);
    concurrentCreator.call();
  }

  @Test
  public void concurrentDeleteTest() throws Exception {
    ConcurrentCreator concurrentCreator = new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, mRoot);
    concurrentCreator.call();

    ConcurrentDeleter concurrentDeleter = new ConcurrentDeleter(DEPTH, CONCURRENCY_DEPTH, mRoot);
    concurrentDeleter.call();

    TachyonURI mntPath = new TachyonURI(mMountPoint);
    Assert.assertEquals(0, mFsMaster.getFileInfoList(mFsMaster.getFileId(mntPath)).size());
  }

  @Test
  public void concurrentRenameTest() throws Exception {
    ConcurrentCreator concurrentCreator = new ConcurrentCreator(DEPTH, CONCURRENCY_DEPTH, mRoot);
    concurrentCreator.call();

    int numFiles = mFsMaster.getFileInfoList(mFsMaster.getFileId(mRoot)).size();

    ConcurrentRenamer concurrentRenamer =
        new ConcurrentRenamer(DEPTH, CONCURRENCY_DEPTH, mRoot, mRoot2, TachyonURI.EMPTY_URI);
    concurrentRenamer.call();

    Assert.assertEquals(numFiles, mFsMaster.getFileInfoList(mFsMaster.getFileId(mRoot2)).size());
  }

  @Test
  public void createAlreadyExistFileTest() throws InvalidPathException, FileAlreadyExistException,
      BlockInfoException, TachyonException {
    mThrown.expect(FileAlreadyExistException.class);
    mFsMaster.createFile(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile")),
        Constants.DEFAULT_BLOCK_SIZE_BYTE, false);
    mFsMaster.mkdirs(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile")), true);
  }

  @Test
  public void createDirectoryTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException {
    mFsMaster.mkdirs(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder")), true);
    FileInfo fileInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint,
            "testFolder"))));
    Assert.assertTrue(fileInfo.isFolder);
  }

  @Test
  public void createFileInvalidPathTest() throws InvalidPathException, FileAlreadyExistException,
      BlockInfoException, TachyonException {
    mThrown.expect(InvalidPathException.class);
    mFsMaster.createFile(new TachyonURI("testFile"), Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
  }

  @Test
  public void createFileInvalidPathTest2() throws InvalidPathException, FileAlreadyExistException,
      BlockInfoException, TachyonException {
    mThrown.expect(FileAlreadyExistException.class);
    mFsMaster.createFile(new TachyonURI(mMountPoint), Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
  }

  @Test
  public void createFileInvalidPathTest3() throws InvalidPathException, FileAlreadyExistException,
      BlockInfoException, TachyonException {
    mThrown.expect(InvalidPathException.class);
    mFsMaster.createFile(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile1")),
        Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    mFsMaster.createFile(
        new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile1", "testFile2")),
        Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
  }

  @Test
  public void createFilePerfTest() throws FileAlreadyExistException, InvalidPathException,
      FileDoesNotExistException, TachyonException {
    // long sMs = System.currentTimeMillis();
    for (int k = 0; k < 200; k ++) {
      mFsMaster.mkdirs(
          new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile")).join(
              Constants.MASTER_COLUMN_FILE_PREFIX + k).join("0"), true);
    }
    // System.out.println(System.currentTimeMillis() - sMs);
    // sMs = System.currentTimeMillis();
    for (int k = 0; k < 200; k ++) {
      mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint,
          "testFile")).join(Constants.MASTER_COLUMN_FILE_PREFIX + k).join("0")));
    }
    // System.out.println(System.currentTimeMillis() - sMs);
  }

  @Test
  public void createFileTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, BlockInfoException, TachyonException {
    mFsMaster.createFile(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile")),
        Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertFalse(mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI(PathUtils
        .concatPath(mMountPoint, "testFile")))).isFolder);
  }

  @Test
  public void deleteDirectoryWithDirectoriesTest() throws InvalidPathException,
      FileAlreadyExistException, FileDoesNotExistException, TachyonException, BlockInfoException {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Could not find path: "
        + PathUtils.concatPath(mMountPoint, "testFolder", "testFolder2", "testFile2"));
    mFsMaster.mkdirs(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder")), true);
    mFsMaster.mkdirs(
        new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder", "testFolder2")), true);
    long fileId =
        mFsMaster.createFile(
            new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder", "testFile")),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    long fileId2 =
        mFsMaster.createFile(
            new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder", "testFolder2",
                "testFile2")), Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(
        mMountPoint, "testFolder", "testFile"))));
    Assert.assertEquals(fileId2, mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(
        mMountPoint, "testFolder", "testFolder2", "testFile2"))));
    Assert.assertTrue(mFsMaster.deleteFile(1, true));
    mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder",
        "testFolder2", "testFile2")));
  }

  @Test
  public void deleteDirectoryWithDirectoriesTest2() throws InvalidPathException,
      FileAlreadyExistException, FileDoesNotExistException, TachyonException, BlockInfoException {
    mFsMaster.mkdirs(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder")), true);
    mFsMaster.mkdirs(
        new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder", "testFolder2")), true);
    long fileId =
        mFsMaster.createFile(
            new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder", "testFile")),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    long fileId2 =
        mFsMaster.createFile(
            new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder", "testFolder2",
                "testFile2")), Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertEquals(3,
        mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder"))));
    Assert.assertEquals(4, mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint,
        "testFolder", "testFolder2"))));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(
        mMountPoint, "testFolder", "testFile"))));
    Assert.assertEquals(fileId2, mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(
        mMountPoint, "testFolder", "testFolder2", "testFile2"))));
    Assert.assertFalse(mFsMaster.deleteFile(2, false));
    Assert.assertEquals(3,
        mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder"))));
    Assert.assertEquals(4, mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint,
        "testFolder", "testFolder2"))));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(
        mMountPoint, "testFolder", "testFile"))));
    Assert.assertEquals(fileId2, mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(
        mMountPoint, "testFolder", "testFolder2", "testFile2"))));
  }

  @Test
  public void deleteDirectoryWithFilesTest() throws InvalidPathException,
      FileAlreadyExistException, FileDoesNotExistException, TachyonException, BlockInfoException {
    mThrown.expect(InvalidPathException.class);
    mThrown
        .expectMessage("Could not find path: " + PathUtils.concatPath(mMountPoint, "testFolder"));
    mFsMaster.mkdirs(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder")), true);
    long fileId =
        mFsMaster.createFile(
            new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder", "testFile")),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertEquals(3,
        mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder"))));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(
        mMountPoint, "testFolder", "testFile"))));
    Assert.assertTrue(mFsMaster.deleteFile(1, true));
    mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder")));
  }

  @Test
  public void deleteDirectoryWithFilesTest2() throws InvalidPathException,
      FileAlreadyExistException, FileDoesNotExistException, TachyonException, BlockInfoException {
    mFsMaster.mkdirs(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder")), true);
    long fileId =
        mFsMaster.createFile(
            new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder", "testFile")),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertEquals(3,
        mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder"))));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(
        mMountPoint, "testFolder", "testFile"))));
    Assert.assertFalse(mFsMaster.deleteFile(1, false));
    Assert.assertEquals(3,
        mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder"))));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(
        mMountPoint, "testFolder", "testFile"))));
  }

  @Test
  public void deleteEmptyDirectoryTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException {
    mThrown.expect(InvalidPathException.class);
    mThrown
        .expectMessage("Could not find path: " + PathUtils.concatPath(mMountPoint, "testFolder"));
    mFsMaster.mkdirs(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder")), true);
    Assert.assertEquals(3,
        mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder"))));
    Assert.assertTrue(mFsMaster.deleteFile(1, true));
    mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder")));
  }

  @Test
  public void deleteFileTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException, BlockInfoException {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Could not find path: " + PathUtils.concatPath(mMountPoint, "testFile"));
    long fileId =
        mFsMaster.createFile(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile")),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertEquals(fileId,
        mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile"))));
    Assert.assertTrue(mFsMaster.deleteFile(fileId, true));
    mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile")));
  }

  @Test
  public void deleteRootTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException, BlockInfoException {
    long rootId = mFsMaster.getFileId(new TachyonURI("/"));
    Assert.assertFalse(mFsMaster.deleteFile(rootId, true));
    Assert.assertFalse(mFsMaster.deleteFile(rootId, false));
  }

  @Test
  public void getCapacityBytesTest() {
    BlockMaster blockMaster = mLocalTachyonCluster.getMaster().getInternalMaster().getBlockMaster();
    Assert.assertEquals(1000, blockMaster.getCapacityBytes());
  }

  @Test
  public void lastModificationTimeAddCheckpointTest() throws FileDoesNotExistException,
      SuspectedFileSizeException, FileAlreadyExistException, InvalidPathException,
      BlockInfoException, FileNotFoundException, TachyonException {
    long fileId =
        mFsMaster.createFile(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile")),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    long opTimeMs = System.currentTimeMillis();
    mFsMaster.addCheckpointInternal(-1, fileId, 1,
        new TachyonURI(PathUtils.concatPath(mMountPoint, "testPath")), opTimeMs);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals(opTimeMs, fileInfo.lastModificationTimeMs);
  }

  @Test
  public void lastModificationTimeCompleteFileTest() throws FileDoesNotExistException,
      SuspectedFileSizeException, FileAlreadyExistException, InvalidPathException,
      BlockInfoException, FileNotFoundException, TachyonException {
    long fileId =
        mFsMaster.createFile(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile")),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    long opTimeMs = System.currentTimeMillis();
    mFsMaster.completeFileInternal(Lists.<Long>newArrayList(), fileId, 0, opTimeMs);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals(opTimeMs, fileInfo.lastModificationTimeMs);
  }

  @Test
  public void lastModificationTimeCreateFileTest() throws InvalidPathException,
      FileAlreadyExistException, FileDoesNotExistException, TachyonException, BlockInfoException {
    mFsMaster.mkdirs(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder")), true);
    long opTimeMs = System.currentTimeMillis();
    mFsMaster.createFileInternal(
        new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder", "testFile")),
        Constants.DEFAULT_BLOCK_SIZE_BYTE, true, opTimeMs);
    FileInfo folderInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint,
            "testFolder"))));
    Assert.assertEquals(opTimeMs, folderInfo.lastModificationTimeMs);
  }

  @Test
  public void lastModificationTimeDeleteTest() throws InvalidPathException,
      FileAlreadyExistException, FileDoesNotExistException, TachyonException, BlockInfoException {
    mFsMaster.mkdirs(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder")), true);
    long fileId =
        mFsMaster.createFile(
            new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder", "testFile")),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    long folderId =
        mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(
        mMountPoint, "testFolder", "testFile"))));
    long opTimeMs = System.currentTimeMillis();
    Assert.assertTrue(mFsMaster.deleteFileInternal(fileId, true, true, opTimeMs));
    FileInfo folderInfo = mFsMaster.getFileInfo(folderId);
    Assert.assertEquals(opTimeMs, folderInfo.lastModificationTimeMs);
  }

  @Test
  public void lastModificationTimeRenameTest() throws InvalidPathException,
      FileAlreadyExistException, FileDoesNotExistException, TachyonException, BlockInfoException,
      IOException {
    mFsMaster.mkdirs(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder")), true);
    long fileId =
        mFsMaster.createFile(
            new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder", "testFile1")),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    long opTimeMs = System.currentTimeMillis();
    mFsMaster.renameInternal(fileId,
        new TachyonURI(PathUtils.concatPath(mMountPoint, "testFolder", "testFile2")), true,
        opTimeMs);
    FileInfo folderInfo =
        mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint,
            "testFolder"))));
    Assert.assertEquals(opTimeMs, folderInfo.lastModificationTimeMs);
  }

  @Test
  public void listFilesTest() throws InvalidPathException, FileDoesNotExistException,
      FileAlreadyExistException, BlockInfoException, TachyonException {
    HashSet<Long> ids = new HashSet<Long>();
    HashSet<Long> dirIds = new HashSet<Long>();
    for (int i = 0; i < 10; i ++) {
      TachyonURI dir = new TachyonURI(PathUtils.concatPath(mMountPoint, "i" + i));
      mFsMaster.mkdirs(dir, true);
      dirIds.add(mFsMaster.getFileId(dir));
      for (int j = 0; j < 10; j ++) {
        ids.add(mFsMaster.createFile(dir.join("j" + j), 64, true));
      }
    }
    HashSet<Long> listedIds = Sets.newHashSet();
    HashSet<Long> listedDirIds = Sets.newHashSet();
    List<FileInfo> infoList =
        mFsMaster.getFileInfoList(mFsMaster.getFileId(new TachyonURI(mMountPoint)));
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
  public void lsTest() throws FileAlreadyExistException, InvalidPathException, TachyonException,
      BlockInfoException, FileDoesNotExistException {
    for (int i = 0; i < 10; i ++) {
      mFsMaster.mkdirs(new TachyonURI(PathUtils.concatPath(mMountPoint, "i" + i)), true);
      for (int j = 0; j < 10; j ++) {
        mFsMaster.createFile(
            new TachyonURI(PathUtils.concatPath(mMountPoint, "i" + i, "j" + j)), 64, true);
      }
    }

    Assert.assertEquals(1,
        mFsMaster.getFileInfoList(mFsMaster.getFileId(
            new TachyonURI(PathUtils.concatPath(mMountPoint, "i0", "j0")))).size());
    for (int i = 0; i < 10; i ++) {
      Assert.assertEquals(10,
          mFsMaster.getFileInfoList(mFsMaster.getFileId(
              new TachyonURI(PathUtils.concatPath(mMountPoint, "i" + i)))).size());
    }
    Assert.assertEquals(10,
        mFsMaster.getFileInfoList(mFsMaster.getFileId(new TachyonURI(mMountPoint))).size());
  }

  @Test
  public void notFileCheckpointTest() throws FileDoesNotExistException, SuspectedFileSizeException,
      FileAlreadyExistException, InvalidPathException, BlockInfoException, TachyonException {
    mThrown.expect(FileDoesNotExistException.class);
    mFsMaster.mkdirs(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile")), true);
    mFsMaster.addCheckpoint(-1,
        mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile"))), 0,
        new TachyonURI(PathUtils.concatPath(mMountPoint, "testPath")));
  }

  @Test
  public void renameExistingDstTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException, BlockInfoException, IOException {
    mFsMaster.createFile(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile1")),
        Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    mFsMaster.createFile(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile2")),
        Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertFalse(mFsMaster.rename(
        mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile1"))),
        new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile2"))));
  }

  @Test
  public void renameNonexistentTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException, BlockInfoException, IOException {
    mThrown.expect(InvalidPathException.class);
    mFsMaster.createFile(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile1")),
        Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    mFsMaster.rename(
        mFsMaster.getFileId(new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile2"))),
        new TachyonURI(PathUtils.concatPath(mMountPoint, "testFile3")));
  }

  @Test
  public void renameToDeeper() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException, BlockInfoException, IOException {
    mThrown.expect(InvalidPathException.class);
    mFsMaster.mkdirs(new TachyonURI(PathUtils.concatPath(mMountPoint, "testDir1", "testDir2")),
        true);
    mFsMaster.createFile(
        new TachyonURI(PathUtils.concatPath(mMountPoint, "testDir1", "testDir2", "testDir3",
            "testFile3")), Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    mFsMaster.rename(
        mFsMaster.getFileId(new TachyonURI(PathUtils
            .concatPath(mMountPoint, "testDir1", "testDir2"))),
        new TachyonURI(PathUtils.concatPath(mMountPoint, "testDir1", "testDir2", "testDir3",
            "testDir4")));
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
