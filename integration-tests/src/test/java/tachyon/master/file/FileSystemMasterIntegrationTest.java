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
        TachyonURI srcPath = mRootPath.join(path);
        TachyonURI dstPath = mRootPath2.join(path);
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
              (new ConcurrentRenamer(depth - 1, concurrencyDepth - 1, mRootPath, mRootPath2,
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
  private FileSystemMaster mFsMaster;

  private static final int DEPTH = 6;

  private static final int FILES_PER_NODE = 4;

  private static final int CONCURRENCY_DEPTH = 3;

  private static final TachyonURI ROOT_PATH = new TachyonURI("/root");

  private static final TachyonURI ROOT_PATH2 = new TachyonURI("/root2");

  private ExecutorService mExecutorService = null;

  private TachyonConf mMasterTachyonConf;

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void addCheckpointTest() throws FileDoesNotExistException, SuspectedFileSizeException,
      FileAlreadyExistException, InvalidPathException, BlockInfoException, FileNotFoundException,
      TachyonException {
    long fileId =
        mFsMaster.createFile(new TachyonURI("/testFile"), Constants.DEFAULT_BLOCK_SIZE_BYTE, false);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("", fileInfo.getUfsPath());
    mFsMaster.completeFileCheckpoint(-1, fileId, 1, new TachyonURI("/testPath"));
    fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("/testPath", fileInfo.getUfsPath());
    mFsMaster.completeFileCheckpoint(-1, fileId, 1, new TachyonURI("/testPath"));
    fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("/testPath", fileInfo.getUfsPath());
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
    mExecutorService = Executors.newFixedThreadPool(2);
    mFsMaster = mLocalTachyonCluster.getMaster().getInternalMaster().getFileSystemMaster();
    mMasterTachyonConf = mLocalTachyonCluster.getMasterTachyonConf();
  }

  @Test
  public void clientFileInfoDirectoryTest() throws InvalidPathException, FileDoesNotExistException,
      FileAlreadyExistException, TachyonException {
    TachyonURI path = new TachyonURI("/testFolder");
    mFsMaster.mkdirs(path, true);
    long fileId = mFsMaster.getFileId(path);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("testFolder", fileInfo.getName());
    Assert.assertEquals(1, fileInfo.getFileId());
    Assert.assertEquals(0, fileInfo.getLength());
    Assert.assertEquals("", fileInfo.getUfsPath());
    Assert.assertTrue(fileInfo.isFolder);
    Assert.assertFalse(fileInfo.isPinned);
    Assert.assertFalse(fileInfo.isCacheable);
    Assert.assertTrue(fileInfo.isComplete);
  }

  @Test
  public void clientFileInfoEmptyFileTest() throws InvalidPathException, FileDoesNotExistException,
      FileAlreadyExistException, BlockInfoException, TachyonException {
    long fileId =
        mFsMaster.createFile(new TachyonURI("/testFile"), Constants.DEFAULT_BLOCK_SIZE_BYTE, false);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals("testFile", fileInfo.getName());
    Assert.assertEquals(fileId, fileInfo.getFileId());
    Assert.assertEquals(0, fileInfo.getLength());
    Assert.assertEquals("", fileInfo.getUfsPath());
    Assert.assertFalse(fileInfo.isFolder);
    Assert.assertFalse(fileInfo.isPinned);
    Assert.assertTrue(fileInfo.isCacheable);
    Assert.assertFalse(fileInfo.isComplete);
  }

  private FileSystemMaster createFileSystemMasterFromJournal() throws IOException {
    return MasterTestUtils.createFileSystemMasterFromJournal(mMasterTachyonConf);
  }

  // TODO: This test currently relies on the fact the HDFS client is a cached instance to avoid
  // TODO: invalid lease exception. This should be fixed.
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

    ConcurrentRenamer concurrentRenamer =
        new ConcurrentRenamer(
            DEPTH, CONCURRENCY_DEPTH, ROOT_PATH, ROOT_PATH2, TachyonURI.EMPTY_URI);
    concurrentRenamer.call();

    Assert.assertEquals(numFiles,
        mFsMaster.getFileInfoList(mFsMaster.getFileId(ROOT_PATH2)).size());
  }

  @Test
  public void createAlreadyExistFileTest() throws InvalidPathException, FileAlreadyExistException,
      BlockInfoException, TachyonException {
    mThrown.expect(FileAlreadyExistException.class);
    mFsMaster.createFile(new TachyonURI("/testFile"), Constants.DEFAULT_BLOCK_SIZE_BYTE, false);
    mFsMaster.mkdirs(new TachyonURI("/testFile"), true);
  }

  @Test
  public void createDirectoryTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException {
    mFsMaster.mkdirs(new TachyonURI("/testFolder"), true);
    FileInfo fileInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI("/testFolder")));
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
    mFsMaster.createFile(new TachyonURI("/"), Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
  }

  @Test
  public void createFileInvalidPathTest3() throws InvalidPathException, FileAlreadyExistException,
      BlockInfoException, TachyonException {
    mThrown.expect(InvalidPathException.class);
    mFsMaster.createFile(new TachyonURI("/testFile1"), Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    mFsMaster.createFile(new TachyonURI("/testFile1/testFile2"), Constants.DEFAULT_BLOCK_SIZE_BYTE,
        true);
  }

  @Test
  public void createFilePerfTest() throws FileAlreadyExistException, InvalidPathException,
      FileDoesNotExistException, TachyonException {
    // long sMs = System.currentTimeMillis();
    for (int k = 0; k < 200; k ++) {
      mFsMaster.mkdirs(new TachyonURI("/testFile").join(Constants.MASTER_COLUMN_FILE_PREFIX + k)
          .join("0"), true);
    }
    // System.out.println(System.currentTimeMillis() - sMs);
    // sMs = System.currentTimeMillis();
    for (int k = 0; k < 200; k ++) {
      mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI("/testFile").join(
          Constants.MASTER_COLUMN_FILE_PREFIX + k).join("0")));
    }
    // System.out.println(System.currentTimeMillis() - sMs);
  }

  @Test
  public void createFileTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, BlockInfoException, TachyonException {
    mFsMaster.createFile(new TachyonURI("/testFile"), Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertFalse(
        mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI("/testFile"))).isFolder);
  }

  @Test
  public void deleteDirectoryWithDirectoriesTest() throws InvalidPathException,
      FileAlreadyExistException, FileDoesNotExistException, TachyonException, BlockInfoException {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Could not find path: /testFolder/testFolder2/testFile2");
    mFsMaster.mkdirs(new TachyonURI("/testFolder"), true);
    mFsMaster.mkdirs(new TachyonURI("/testFolder/testFolder2"), true);
    long fileId =
        mFsMaster.createFile(new TachyonURI("/testFolder/testFile"),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    long fileId2 =
        mFsMaster.createFile(new TachyonURI("/testFolder/testFolder2/testFile2"),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertEquals(1, mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(2, mFsMaster.getFileId(new TachyonURI("/testFolder/testFolder2")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFolder/testFile")));
    Assert.assertEquals(fileId2,
        mFsMaster.getFileId(new TachyonURI("/testFolder/testFolder2/testFile2")));
    Assert.assertTrue(mFsMaster.deleteFile(1, true));
    mFsMaster.getFileId(new TachyonURI("/testFolder/testFolder2/testFile2"));
  }

  @Test
  public void deleteDirectoryWithDirectoriesTest2() throws InvalidPathException,
      FileAlreadyExistException, FileDoesNotExistException, TachyonException, BlockInfoException {
    mFsMaster.mkdirs(new TachyonURI("/testFolder"), true);
    mFsMaster.mkdirs(new TachyonURI("/testFolder/testFolder2"), true);
    long fileId =
        mFsMaster.createFile(new TachyonURI("/testFolder/testFile"),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    long fileId2 =
        mFsMaster.createFile(new TachyonURI("/testFolder/testFolder2/testFile2"),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertEquals(1, mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(2, mFsMaster.getFileId(new TachyonURI("/testFolder/testFolder2")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFolder/testFile")));
    Assert.assertEquals(fileId2,
        mFsMaster.getFileId(new TachyonURI("/testFolder/testFolder2/testFile2")));
    Assert.assertFalse(mFsMaster.deleteFile(2, false));
    Assert.assertEquals(1, mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(2, mFsMaster.getFileId(new TachyonURI("/testFolder/testFolder2")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFolder/testFile")));
    Assert.assertEquals(fileId2,
        mFsMaster.getFileId(new TachyonURI("/testFolder/testFolder2/testFile2")));
  }

  @Test
  public void deleteDirectoryWithFilesTest() throws InvalidPathException,
      FileAlreadyExistException, FileDoesNotExistException, TachyonException, BlockInfoException {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Could not find path: /testFolder");
    mFsMaster.mkdirs(new TachyonURI("/testFolder"), true);
    long fileId =
        mFsMaster.createFile(new TachyonURI("/testFolder/testFile"),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertEquals(1, mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFolder/testFile")));
    Assert.assertTrue(mFsMaster.deleteFile(1, true));
    mFsMaster.getFileId(new TachyonURI("/testFolder"));
  }

  @Test
  public void deleteDirectoryWithFilesTest2() throws InvalidPathException,
      FileAlreadyExistException, FileDoesNotExistException, TachyonException, BlockInfoException {
    mFsMaster.mkdirs(new TachyonURI("/testFolder"), true);
    long fileId =
        mFsMaster.createFile(new TachyonURI("/testFolder/testFile"),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertEquals(1, mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFolder/testFile")));
    Assert.assertFalse(mFsMaster.deleteFile(1, false));
    Assert.assertEquals(1, mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFolder/testFile")));
  }

  @Test
  public void deleteEmptyDirectoryTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Could not find path: /testFolder");
    mFsMaster.mkdirs(new TachyonURI("/testFolder"), true);
    Assert.assertEquals(1, mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertTrue(mFsMaster.deleteFile(1, true));
    mFsMaster.getFileId(new TachyonURI("/testFolder"));
  }

  @Test
  public void deleteFileTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException, BlockInfoException {
    mThrown.expect(InvalidPathException.class);
    mThrown.expectMessage("Could not find path: /testFile");
    long fileId =
        mFsMaster.createFile(new TachyonURI("/testFile"), Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFile")));
    Assert.assertTrue(mFsMaster.deleteFile(fileId, true));
    mFsMaster.getFileId(new TachyonURI("/testFile"));
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
        mFsMaster.createFile(new TachyonURI("/testFile"), Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    long opTimeMs = System.currentTimeMillis();
    mFsMaster.completeFileCheckpointInternal(-1, fileId, 1, new TachyonURI("/testPath"), opTimeMs);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals(opTimeMs, fileInfo.lastModificationTimeMs);
  }

  @Test
  public void lastModificationTimeCompleteFileTest() throws FileDoesNotExistException,
      SuspectedFileSizeException, FileAlreadyExistException, InvalidPathException,
      BlockInfoException, FileNotFoundException, TachyonException {
    long fileId =
        mFsMaster.createFile(new TachyonURI("/testFile"), Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    long opTimeMs = System.currentTimeMillis();
    mFsMaster.completeFileInternal(Lists.<Long>newArrayList(), fileId, 0, opTimeMs);
    FileInfo fileInfo = mFsMaster.getFileInfo(fileId);
    Assert.assertEquals(opTimeMs, fileInfo.lastModificationTimeMs);
  }

  // TODO: Should writing in a file in a directory update its last mod time? If so we need to log
  // it in the journal
  @Ignore
  @Test
  public void lastModificationTimeCreateFileTest() throws InvalidPathException,
      FileAlreadyExistException, FileDoesNotExistException, TachyonException, BlockInfoException {
    mFsMaster.mkdirs(new TachyonURI("/testFolder"), true);
    long opTimeMs = System.currentTimeMillis();
    mFsMaster.createFileInternal(new TachyonURI("/testFolder/testFile"),
        Constants.DEFAULT_BLOCK_SIZE_BYTE, true, opTimeMs);
    FileInfo folderInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(opTimeMs, folderInfo.lastModificationTimeMs);
  }

  @Test
  public void lastModificationTimeDeleteTest() throws InvalidPathException,
      FileAlreadyExistException, FileDoesNotExistException, TachyonException, BlockInfoException {
    mFsMaster.mkdirs(new TachyonURI("/testFolder"), true);
    long fileId =
        mFsMaster.createFile(new TachyonURI("/testFolder/testFile"),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    long folderId = mFsMaster.getFileId(new TachyonURI("/testFolder"));
    Assert.assertEquals(1, folderId);
    Assert.assertEquals(fileId, mFsMaster.getFileId(new TachyonURI("/testFolder/testFile")));
    long opTimeMs = System.currentTimeMillis();
    Assert.assertTrue(mFsMaster.deleteFileInternal(fileId, true, opTimeMs));
    FileInfo folderInfo = mFsMaster.getFileInfo(folderId);
    Assert.assertEquals(opTimeMs, folderInfo.lastModificationTimeMs);
  }

  @Test
  public void lastModificationTimeRenameTest() throws InvalidPathException,
      FileAlreadyExistException, FileDoesNotExistException, TachyonException, BlockInfoException {
    mFsMaster.mkdirs(new TachyonURI("/testFolder"), true);
    long fileId =
        mFsMaster.createFile(new TachyonURI("/testFolder/testFile1"),
            Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    long opTimeMs = System.currentTimeMillis();
    mFsMaster.renameInternal(fileId, new TachyonURI("/testFolder/testFile2"), opTimeMs);
    FileInfo folderInfo = mFsMaster.getFileInfo(mFsMaster.getFileId(new TachyonURI("/testFolder")));
    Assert.assertEquals(opTimeMs, folderInfo.lastModificationTimeMs);
  }

  @Test
  public void listFilesTest() throws InvalidPathException, FileDoesNotExistException,
      FileAlreadyExistException, BlockInfoException, TachyonException {
    HashSet<Long> ids = new HashSet<Long>();
    HashSet<Long> dirIds = new HashSet<Long>();
    for (int i = 0; i < 10; i ++) {
      TachyonURI dir = new TachyonURI("/i" + i);
      mFsMaster.mkdirs(dir, true);
      dirIds.add(mFsMaster.getFileId(dir));
      for (int j = 0; j < 10; j ++) {
        ids.add(mFsMaster.createFile(dir.join("j" + j), 64, true));
      }
    }
    HashSet<Long> listedIds = Sets.newHashSet();
    HashSet<Long> listedDirIds = Sets.newHashSet();
    List<FileInfo> infoList = mFsMaster.getFileInfoList(mFsMaster.getFileId(new TachyonURI("/")));
    for (FileInfo info : infoList) {
      // TODO: After info.getFileId return long, remove this type cast
      long id = new Long(info.getFileId());
      listedDirIds.add(id);
      for (FileInfo fileInfo : mFsMaster.getFileInfoList(id)) {
        listedIds.add((long) fileInfo.getFileId());
      }
    }
    Assert.assertEquals(ids, listedIds);
    Assert.assertEquals(dirIds, listedDirIds);
  }

  // TODO: There is no longer `ls` method in FileSystemMaster, should this test be removed or should
  // `ls` be added back?
  //@Test
  //public void lsTest() throws FileAlreadyExistException, InvalidPathException, TachyonException,
  //    BlockInfoException, FileDoesNotExistException {
  //  for (int i = 0; i < 10; i ++) {
  //    mMasterInfo.mkdirs(new TachyonURI("/i" + i), true);
  //    for (int j = 0; j < 10; j ++) {
  //      mMasterInfo.createFile(new TachyonURI("/i" + i + "/j" + j), 64);
  //    }
  //  }

  //  Assert.assertEquals(1, mMasterInfo.ls(new TachyonURI("/i0/j0"), false).size());
  //  Assert.assertEquals(1, mMasterInfo.ls(new TachyonURI("/i0/j0"), true).size());
  //  for (int i = 0; i < 10; i ++) {
  //    Assert.assertEquals(11, mMasterInfo.ls(new TachyonURI("/i" + i), false).size());
  //    Assert.assertEquals(11, mMasterInfo.ls(new TachyonURI("/i" + i), true).size());
  //  }
  //  Assert.assertEquals(11, mMasterInfo.ls(new TachyonURI(TachyonURI.SEPARATOR), false).size());
  //  Assert.assertEquals(111, mMasterInfo.ls(new TachyonURI(TachyonURI.SEPARATOR), true).size());
  //}

  @Test
  public void notFileCheckpointTest() throws FileDoesNotExistException, SuspectedFileSizeException,
      FileAlreadyExistException, InvalidPathException, BlockInfoException, TachyonException {
    mThrown.expect(FileDoesNotExistException.class);
    mFsMaster.mkdirs(new TachyonURI("/testFile"), true);
    mFsMaster.completeFileCheckpoint(-1, mFsMaster.getFileId(new TachyonURI("/testFile")), 0,
        new TachyonURI("/testPath"));
  }

  @Test
  public void renameExistingDstTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException, BlockInfoException {
    mFsMaster.createFile(new TachyonURI("/testFile1"), Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    mFsMaster.createFile(new TachyonURI("/testFile2"), Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    Assert.assertFalse(mFsMaster.rename(mFsMaster.getFileId(new TachyonURI("/testFile1")),
        new TachyonURI("/testFile2")));
  }

  @Test
  public void renameNonexistentTest() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException, BlockInfoException {
    mThrown.expect(InvalidPathException.class);
    mFsMaster.createFile(new TachyonURI("/testFile1"), Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    mFsMaster.rename(mFsMaster.getFileId(new TachyonURI("/testFile2")),
        new TachyonURI("/testFile3"));
  }

  @Test
  public void renameToDeeper() throws InvalidPathException, FileAlreadyExistException,
      FileDoesNotExistException, TachyonException, BlockInfoException {
    mThrown.expect(InvalidPathException.class);
    mFsMaster.mkdirs(new TachyonURI("/testDir1/testDir2"), true);
    mFsMaster.createFile(new TachyonURI("/testDir1/testDir2/testDir3/testFile3"),
        Constants.DEFAULT_BLOCK_SIZE_BYTE, true);
    mFsMaster.rename(mFsMaster.getFileId(new TachyonURI("/testDir1/testDir2")),
        new TachyonURI("/testDir1/testDir2/testDir3/testDir4"));
  }

  // TODO: Journal format has changed, maybe add Version to the format and add this test back
  //       or remove this test when we have better tests against journal checkpoint
  //@Test
  //public void writeImageTest() throws IOException {
  //  // initialize the MasterInfo
  //  Journal journal =
  //      new Journal(mLocalTachyonCluster.getTachyonHome() + "journal/", "image.data", "log.data",
  //          mMasterTachyonConf);
  //  Journal
  //  MasterInfo info =
  //     new MasterInfo(new InetSocketAddress(9999), journal, mExecutorService, mMasterTachyonConf);

  //  // create the output streams
  //  ByteArrayOutputStream os = new ByteArrayOutputStream();
  //  DataOutputStream dos = new DataOutputStream(os);
  //  ObjectMapper mapper = JsonObject.createObjectMapper();
  //  ObjectWriter writer = mapper.writer();
  //  ImageElement version = null;
  //  ImageElement checkpoint = null;

  //  // write the image
  //  info.writeImage(writer, dos);

  //  // parse the written bytes and look for the Checkpoint and Version ImageElements
  //  String[] splits = new String(os.toByteArray()).split("\n");
  //  for (String split : splits) {
  //    byte[] bytes = split.getBytes();
  //    JsonParser parser = mapper.getFactory().createParser(bytes);
  //    ImageElement ele = parser.readValueAs(ImageElement.class);

  //    if (ele.mType.equals(ImageElementType.Checkpoint)) {
  //      checkpoint = ele;
  //    }

  //    if (ele.mType.equals(ImageElementType.Version)) {
  //      version = ele;
  //    }
  //  }

  //  // test the elements
  //  Assert.assertNotNull(checkpoint);
  //  Assert.assertEquals(checkpoint.mType, ImageElementType.Checkpoint);
  //  Assert.assertEquals(Constants.JOURNAL_VERSION, version.getInt("version").intValue());
  //  Assert.assertEquals(1, checkpoint.getInt("inodeCounter").intValue());
  //  Assert.assertEquals(0, checkpoint.getInt("editTransactionCounter").intValue());
  //  Assert.assertEquals(0, checkpoint.getInt("dependencyCounter").intValue());
  //}
}
