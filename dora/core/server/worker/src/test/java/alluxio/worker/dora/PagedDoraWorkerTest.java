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

package alluxio.worker.dora;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.FileInfo;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadFileFailure;
import alluxio.grpc.Route;
import alluxio.grpc.RouteFailure;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UfsReadOptions;
import alluxio.grpc.WriteOptions;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsStatus;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class PagedDoraWorkerTest {
  private PagedDoraWorker mWorker;
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();
  private CacheManager mCacheManager;
  private final long mPageSize =
      Configuration.global().getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);
  private static final GetStatusPOptions GET_STATUS_OPTIONS_MUST_SYNC =
      GetStatusPOptions.newBuilder().setCommonOptions(
          FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0)).build();

  @Before
  public void before() throws Exception {
    Configuration.set(PropertyKey.DORA_WORKER_METASTORE_ROCKSDB_DIR,
        mTestFolder.newFolder("rocks"));
    CacheManagerOptions cacheManagerOptions =
        CacheManagerOptions.createForWorker(Configuration.global());

    PageMetaStore pageMetaStore =
        PageMetaStore.create(CacheManagerOptions.createForWorker(Configuration.global()));
    mCacheManager =
        CacheManager.Factory.create(Configuration.global(), cacheManagerOptions, pageMetaStore);
    mWorker = new PagedDoraWorker(new AtomicReference<>(1L), Configuration.global(), mCacheManager);
  }

  @After
  public void after() throws Exception {
    mWorker.close();
  }

  @Test
  @Ignore
  // TODO(elega) fix this broken test
  public void testLoad()
      throws AccessControlException, ExecutionException, InterruptedException, TimeoutException,
      IOException {
    int numPages = 10;
    long length = mPageSize * numPages;
    String ufsPath = mTestFolder.newFile("test").getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) length);
    BufferUtils.writeBufferToFile(ufsPath, buffer);
    alluxio.grpc.File file =
        alluxio.grpc.File.newBuilder().setUfsPath(ufsPath).setLength(length).setMountId(1).build();
    ListenableFuture<List<LoadFileFailure>> load = mWorker.load(true, Collections.emptyList(),
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build());
    List<LoadFileFailure> fileFailures = load.get(30, TimeUnit.SECONDS);
    Assert.assertEquals(0, fileFailures.size());
    List<PageId> cachedPages =
        mCacheManager.getCachedPageIdsByFileId(new AlluxioURI(ufsPath).hash(), length);
    assertEquals(numPages, cachedPages.size());
    int start = 0;
    for (PageId pageId : cachedPages) {
      byte[] buff = new byte[(int) mPageSize];
      mCacheManager.get(pageId, (int) mPageSize, buff, 0);
      assertTrue(BufferUtils.equalIncreasingByteArray(start, (int) mPageSize, buff));
      start += mPageSize;
    }
  }

  @Test
  public void testSingleFileCopy() throws IOException, ExecutionException, InterruptedException {
    File srcRoot = mTestFolder.newFolder("src");
    File dstRoot = mTestFolder.newFolder("dst");
    // create test file under mSrcFolder
    File a = new File(srcRoot, "a");
    a.createNewFile();
    File b = new File(dstRoot, "b");
    int length = 10;
    byte[] buffer = BufferUtils.getIncreasingByteArray(length);
    BufferUtils.writeBufferToFile(a.getAbsolutePath(), buffer);
    Route route =
        Route.newBuilder().setDst(b.getAbsolutePath()).setSrc(a.getAbsolutePath()).setLength(length)
            .build();
    WriteOptions writeOptions =
        WriteOptions.newBuilder().setOverwrite(false).setCheckContent(true).build();
    UfsReadOptions read =
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build();
    ListenableFuture<List<RouteFailure>> copy =
        mWorker.copy(Collections.singletonList(route), read, writeOptions);
    List<RouteFailure> failures = copy.get();
    assertEquals(0, failures.size());
    Assert.assertTrue(b.exists());
    try (InputStream in = Files.newInputStream(b.toPath())) {
      byte[] readBuffer = new byte[length];
      while (in.read(readBuffer) != -1) {
      }
      Assert.assertArrayEquals(buffer, readBuffer);
    }
  }

  @Test
  public void testCopyException() throws IOException, ExecutionException, InterruptedException {
    File srcRoot = mTestFolder.newFolder("srcException");
    File dstRoot = mTestFolder.newFolder("dstException");
    // create test file under mSrcFolder
    File a = new File(srcRoot, "a");
    a.createNewFile();
    File b = new File(dstRoot, "b");
    int length = 10;
    byte[] buffer = BufferUtils.getIncreasingByteArray(length);
    BufferUtils.writeBufferToFile(a.getAbsolutePath(), buffer);
    Route route =
        Route.newBuilder().setDst(b.getAbsolutePath()).setSrc(a.getAbsolutePath()).setLength(length)
            .build();

    WriteOptions writeOptions = WriteOptions.newBuilder().setOverwrite(false).build();
    ListenableFuture<List<RouteFailure>> copy =
        mWorker.copy(Collections.singletonList(route), null, writeOptions);
    List<RouteFailure> failures = copy.get();
    assertEquals(1, failures.size());
    assertFalse(b.exists());
  }

  @Test
  public void testSingleFolderCopy() throws IOException, ExecutionException, InterruptedException {
    File srcRoot = mTestFolder.newFolder("src");
    File dstRoot = mTestFolder.newFolder("dst");
    // create test file under mSrcFolder
    File a = new File(srcRoot, "a");
    a.mkdirs();
    File b = new File(dstRoot, "b");
    Route route =
        Route.newBuilder().setDst(b.getAbsolutePath()).setSrc(a.getAbsolutePath()).build();
    WriteOptions writeOptions =
        WriteOptions.newBuilder().setOverwrite(false).setCheckContent(true).build();
    UfsReadOptions read =
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build();
    ListenableFuture<List<RouteFailure>> copy =
        mWorker.copy(Collections.singletonList(route), read, writeOptions);
    List<RouteFailure> failures = copy.get();
    assertEquals(0, failures.size());
    Assert.assertTrue(b.exists());
    Assert.assertTrue(b.isDirectory());
  }

  @Test
  @Ignore
  public void testFolderWithFileCopy()
      throws IOException, ExecutionException, InterruptedException {
    File srcRoot = mTestFolder.newFolder("src");
    File dstRoot = mTestFolder.newFolder("dst");
    // create test file under mSrcFolder
    File a = new File(srcRoot, "a");
    a.mkdirs();
    File c = new File(a, "c");
    c.createNewFile();
    File d = new File(a, "d");
    d.mkdirs();
    File b = new File(dstRoot, "b");
    File dstC = new File(b, "c");
    File dstD = new File(b, "d");
    int length = 10;
    byte[] buffer = BufferUtils.getIncreasingByteArray(length);
    BufferUtils.writeBufferToFile(c.getAbsolutePath(), buffer);
    List<Route> routes = new ArrayList<>();
    Route route = Route.newBuilder().setDst(dstC.getAbsolutePath()).setSrc(c.getAbsolutePath())
        .setLength(length).build();
    Route route2 =
        Route.newBuilder().setDst(b.getAbsolutePath()).setSrc(a.getAbsolutePath()).build();
    Route route3 =
        Route.newBuilder().setDst(dstD.getAbsolutePath()).setSrc(d.getAbsolutePath()).build();
    routes.add(route);
    routes.add(route2);
    routes.add(route3);
    WriteOptions writeOptions =
        WriteOptions.newBuilder().setOverwrite(false).setCheckContent(true).build();
    UfsReadOptions read =
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build();
    ListenableFuture<List<RouteFailure>> copy = mWorker.copy(routes, read, writeOptions);
    List<RouteFailure> failures = copy.get();

    assertEquals(0, failures.size());
    Assert.assertTrue(dstC.exists());
    Assert.assertTrue(b.exists());
    Assert.assertTrue(b.isDirectory());
    Assert.assertTrue(dstD.exists());
    Assert.assertTrue(dstD.isDirectory());
    try (InputStream in = Files.newInputStream(dstC.toPath())) {
      byte[] readBuffer = new byte[length];
      while (in.read(readBuffer) != -1) {
      }
      Assert.assertArrayEquals(buffer, readBuffer);
    }
  }

  @Test
  public void testSingleFileMove() throws IOException, ExecutionException, InterruptedException {
    File srcRoot = mTestFolder.newFolder("src");
    File dstRoot = mTestFolder.newFolder("dst");
    // create test file under mSrcFolder
    File a = new File(srcRoot, "a");
    a.createNewFile();
    File b = new File(dstRoot, "b");
    int length = 10;
    byte[] buffer = BufferUtils.getIncreasingByteArray(length);
    BufferUtils.writeBufferToFile(a.getAbsolutePath(), buffer);
    Route route =
        Route.newBuilder().setDst(b.getAbsolutePath()).setSrc(a.getAbsolutePath())
            .setLength(length).build();
    WriteOptions writeOptions =
        WriteOptions.newBuilder().setOverwrite(false).setCheckContent(true).build();
    UfsReadOptions read =
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build();
    ListenableFuture<List<RouteFailure>> move =
        mWorker.move(Collections.singletonList(route), read, writeOptions);
    List<RouteFailure> failures = move.get();
    assertEquals(0, failures.size());
    Assert.assertTrue(b.exists());
    assertFalse(a.exists());
    try (InputStream in = Files.newInputStream(b.toPath())) {
      byte[] readBuffer = new byte[length];
      while (in.read(readBuffer) != -1) {
      }
      Assert.assertArrayEquals(buffer, readBuffer);
    }
  }

  @Test
  public void testMoveException() throws IOException, ExecutionException, InterruptedException {
    File srcRoot = mTestFolder.newFolder("srcException");
    File dstRoot = mTestFolder.newFolder("dstException");
    // create test file under mSrcFolder
    File a = new File(srcRoot, "a");
    a.createNewFile();
    File b = new File(dstRoot, "b");
    int length = 10;
    byte[] buffer = BufferUtils.getIncreasingByteArray(length);
    BufferUtils.writeBufferToFile(a.getAbsolutePath(), buffer);
    Route route =
        Route.newBuilder().setDst(b.getAbsolutePath()).setSrc(a.getAbsolutePath())
            .setLength(length).build();
    WriteOptions writeOptions = WriteOptions.newBuilder().setOverwrite(false).build();
    ListenableFuture<List<RouteFailure>> move =
        mWorker.move(Collections.singletonList(route), null, writeOptions);
    List<RouteFailure> failures = move.get();
    assertEquals(1, failures.size());
    assertFalse(b.exists());
  }

  @Test
  public void testSingleFolderMove() throws IOException, ExecutionException, InterruptedException {
    File srcRoot = mTestFolder.newFolder("src");
    File dstRoot = mTestFolder.newFolder("dst");
    // create test file under mSrcFolder
    File a = new File(srcRoot, "a");
    a.mkdirs();
    File b = new File(dstRoot, "b");
    Route route =
        Route.newBuilder().setDst(b.getAbsolutePath()).setSrc(a.getAbsolutePath()).build();
    WriteOptions writeOptions =
        WriteOptions.newBuilder().setOverwrite(false).setCheckContent(true).build();
    UfsReadOptions read =
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build();
    ListenableFuture<List<RouteFailure>> move =
        mWorker.move(Collections.singletonList(route), read, writeOptions);
    List<RouteFailure> failures = move.get();
    assertEquals(0, failures.size());
    Assert.assertTrue(b.exists());
    Assert.assertTrue(b.isDirectory());
    assertFalse(a.exists());
  }

  @Test
  @Ignore
  public void testFolderWithFileMove()
      throws IOException, ExecutionException, InterruptedException {
    File srcRoot = mTestFolder.newFolder("src");
    File dstRoot = mTestFolder.newFolder("dst");
    // create test file under mSrcFolder
    File a = new File(srcRoot, "a");
    a.mkdirs();
    File c = new File(a, "c");
    c.createNewFile();
    File d = new File(a, "d");
    d.mkdirs();
    File b = new File(dstRoot, "b");
    File dstC = new File(b, "c");
    File dstD = new File(b, "d");
    int length = 10;
    byte[] buffer = BufferUtils.getIncreasingByteArray(length);
    BufferUtils.writeBufferToFile(c.getAbsolutePath(), buffer);
    List<Route> routes = new ArrayList<>();
    Route route = Route.newBuilder().setDst(dstC.getAbsolutePath()).setSrc(c.getAbsolutePath())
        .setLength(length).build();
    Route route2 =
        Route.newBuilder().setDst(b.getAbsolutePath()).setSrc(a.getAbsolutePath()).build();
    Route route3 =
        Route.newBuilder().setDst(dstD.getAbsolutePath()).setSrc(d.getAbsolutePath()).build();
    routes.add(route);
    routes.add(route2);
    routes.add(route3);
    WriteOptions writeOptions =
        WriteOptions.newBuilder().setOverwrite(false).setCheckContent(true).build();
    UfsReadOptions read =
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build();
    ListenableFuture<List<RouteFailure>> move = mWorker.move(routes, read, writeOptions);
    List<RouteFailure> failures = move.get();

    assertEquals(0, failures.size());
    Assert.assertTrue(dstC.exists());
    Assert.assertTrue(b.exists());
    Assert.assertTrue(b.isDirectory());
    Assert.assertTrue(dstD.exists());
    Assert.assertTrue(dstD.isDirectory());
    assertFalse(a.exists());
    assertFalse(c.exists());
    assertFalse(d.exists());
    try (InputStream in = Files.newInputStream(dstC.toPath())) {
      byte[] readBuffer = new byte[length];
      while (in.read(readBuffer) != -1) {
      }
      Assert.assertArrayEquals(buffer, readBuffer);
    }
  }

  @Test
  public void testSetAttribute() throws Exception {
    String fileContent = "foobar";
    File f = mTestFolder.newFile();
    Files.write(f.toPath(), fileContent.getBytes());

    mWorker.setAttribute(f.getPath(), SetAttributePOptions.newBuilder()
        .setMode(new Mode((short) 0444).toProto()).build());

    loadFileData(f.getPath());

    alluxio.wire.FileInfo result =
        mWorker.getFileInfo(f.getPath(), GetStatusPOptions.getDefaultInstance());
    List<PageId> cachedPages =
        mCacheManager.getCachedPageIdsByFileId(
            new AlluxioURI(f.getPath()).hash(), fileContent.length());
    assertEquals(1, cachedPages.size());
    assertEquals(0444, result.getMode());

    mWorker.setAttribute(f.getPath(), SetAttributePOptions.newBuilder()
        .setMode(new Mode((short) 0777).toProto()).build());
    result = mWorker.getFileInfo(f.getPath(), GetStatusPOptions.getDefaultInstance());
    cachedPages =
        mCacheManager.getCachedPageIdsByFileId(
            new AlluxioURI(f.getPath()).hash(), fileContent.length());
    // The data cache won't be invalidated after the MODE update,
    // as we know the content part does not change, by comparing the content hash.
    // Otherwise, the data cache will be invalidated, and we will get 0 page.
    assertEquals(1, cachedPages.size());
    assertEquals(0777, result.getMode());

    assertTrue(f.delete());
    // The target file is already deleted. setAttribute() should throw an exception.
    assertThrows(Exception.class, () ->
        mWorker.setAttribute(f.getPath(), SetAttributePOptions.newBuilder()
        .setMode(new Mode((short) 777).toProto()).build()));
  }

  @Test
  public void testGetFileInfo()
      throws AccessControlException, IOException, ExecutionException, InterruptedException,
      TimeoutException {
    String fileContent = "foobar";
    String updatedFileContent = "foobarbaz";
    File f = mTestFolder.newFile();
    Files.write(f.toPath(), fileContent.getBytes());

    alluxio.wire.FileInfo result =
        mWorker.getFileInfo(f.getPath(), GetStatusPOptions.getDefaultInstance());
    List<PageId> cachedPages =
        mCacheManager.getCachedPageIdsByFileId(
            new AlluxioURI(f.getPath()).hash(), fileContent.length());
    assertEquals(fileContent.length(), result.getLength());
    assertEquals(0, cachedPages.size());
    assertFalse(Strings.isNullOrEmpty(result.getContentHash()));

    loadFileData(f.getPath());

    cachedPages =
        mCacheManager.getCachedPageIdsByFileId(
            new AlluxioURI(f.getPath()).hash(), fileContent.length());
    assertEquals(1, cachedPages.size());
    byte[] buff = new byte[fileContent.length()];
    mCacheManager.get(cachedPages.get(0), fileContent.length(), buff, 0);
    assertEquals(fileContent, new String(buff));

    result = mWorker.getFileInfo(f.getPath(), GET_STATUS_OPTIONS_MUST_SYNC);
    cachedPages =
        mCacheManager.getCachedPageIdsByFileId(
            new AlluxioURI(f.getPath()).hash(), fileContent.length());
    assertEquals(1, cachedPages.size());
    assertEquals(fileContent.length(), result.getLength());

    assertTrue(f.delete());
    assertTrue(f.createNewFile());
    Files.write(f.toPath(), updatedFileContent.getBytes());

    result = mWorker.getFileInfo(f.getPath(), GET_STATUS_OPTIONS_MUST_SYNC);
    cachedPages =
        mCacheManager.getCachedPageIdsByFileId(
            new AlluxioURI(f.getPath()).hash(), updatedFileContent.length());
    assertEquals(0, cachedPages.size());
    assertEquals(updatedFileContent.length(), result.getLength());
    assertFalse(Strings.isNullOrEmpty(result.getContentHash()));

    assertTrue(f.delete());
    result = mWorker.getFileInfo(f.getPath(), GetStatusPOptions.getDefaultInstance());
    assertEquals(0, cachedPages.size());
    assertEquals(updatedFileContent.length(), result.getLength());

    assertThrows(FileNotFoundException.class, () ->
        mWorker.getFileInfo(f.getPath(), GetStatusPOptions.newBuilder().setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0)).build()));
  }

  @Test
  public void testGetFileInfoDir()
      throws AccessControlException, IOException {
    File f = mTestFolder.newFolder();

    alluxio.wire.FileInfo result =
        mWorker.getFileInfo(f.getPath(), GetStatusPOptions.getDefaultInstance());
    assertTrue(result.isFolder());

    result = mWorker.getFileInfo(f.getPath(), GET_STATUS_OPTIONS_MUST_SYNC);
    assertTrue(result.isFolder());
  }

  @Test
  public void testCreateDeleteFile() throws Exception {
    File testDir = mTestFolder.newFolder("testDir");
    testDir.mkdirs();
    File testFile = new File(testDir, "a");
    int fileLength = 1024;
    createDummyFile(testFile, fileLength);

    assertTrue(testFile.exists());
    alluxio.wire.FileInfo fileInfo = mWorker.getFileInfo(testFile.getPath(),
        GetStatusPOptions.getDefaultInstance());
    assertEquals(fileInfo.getLength(), fileLength);

    mWorker.delete(testFile.getPath(), DeletePOptions.getDefaultInstance());
    assertThrows(FileNotFoundException.class, () -> {
      mWorker.getFileInfo(testFile.getPath(), GetStatusPOptions.getDefaultInstance());
    });
    assertFalse(testFile.exists());
  }

  @Test
  public void testCreateDeleteDirectory() throws Exception {
    File testBaseDir = mTestFolder.newFolder("testBaseDir");
    // Prepare the base dir in UFS because we create the dir without recursive=true
    testBaseDir.mkdirs();
    File testDir = new File(testBaseDir, "testDir");
    mWorker.createDirectory(testDir.getPath(), CreateDirectoryPOptions.getDefaultInstance());
    // The test dir should be created by Alluxio in UFS
    assertTrue(testDir.exists());
    alluxio.wire.FileInfo fileInfo = mWorker.getFileInfo(testDir.getPath(),
        GetStatusPOptions.getDefaultInstance());
    assertTrue(fileInfo.isFolder());

    mWorker.delete(testDir.getPath(), DeletePOptions.getDefaultInstance());
    assertThrows(FileNotFoundException.class, () -> {
      mWorker.getFileInfo(testDir.getPath(), GetStatusPOptions.getDefaultInstance());
    });
  }

  @Test
  public void testRecursiveCreateDeleteDirectory() throws Exception {
    File testBaseDir = mTestFolder.newFolder("testDir");
    File testDir = new File(testBaseDir, "a");
    File testNestedDir = new File(testDir, "b");
    // Through Alluxio, create the nested path recursively
    mWorker.createDirectory(testNestedDir.getPath(),
        CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    // Both directories should be created by Alluxio
    assertTrue(testNestedDir.exists());
    assertTrue(testDir.exists());
    alluxio.wire.FileInfo nestedDirInfo = mWorker.getFileInfo(testNestedDir.getPath(),
        GetStatusPOptions.getDefaultInstance());
    assertTrue(nestedDirInfo.isFolder());

    // Can create files under the nested dir, meaning the dir is created in UFS
    File testFile = new File(testNestedDir, "testFile");
    createDummyFile(testFile, 1024);
    alluxio.wire.FileInfo nestedFileInfo = mWorker.getFileInfo(testFile.getPath(),
        GetStatusPOptions.getDefaultInstance());
    assertEquals(nestedFileInfo.getLength(), 1024);

    // Delete the dir (containing nested files), the dir and nested files should all be gone
    mWorker.delete(testNestedDir.getPath(), DeletePOptions.newBuilder().setRecursive(true).build());
    assertThrows(FileNotFoundException.class, () -> {
      mWorker.getFileInfo(testNestedDir.getPath(), GetStatusPOptions.getDefaultInstance());
    });

    assertThrows(FileNotFoundException.class, () -> {
      // https://github.com/Alluxio/alluxio/issues/17741
      // Children under a recursively deleted directory may exist in cache for a while
      // So we need to manually ignore the cache
      mWorker.getFileInfo(testFile.getPath(), GET_STATUS_OPTIONS_MUST_SYNC);
    });
  }

  @Test
  public void testRecursiveListing() throws Exception {
    File rootFolder = mTestFolder.newFolder("root");
    String rootPath = rootFolder.getAbsolutePath();
    mTestFolder.newFolder("root/d1");
    mTestFolder.newFolder("root/d1/d1");
    mTestFolder.newFolder("root/d2");
    UfsStatus[] listResult =
        mWorker.listStatus(rootPath, ListStatusPOptions.newBuilder().setRecursive(true).build());
    assertEquals(3, listResult.length);
    assertFalse(mWorker.getMetaManager().listCached(rootPath, true).isPresent());
    listResult =
        mWorker.listStatus(rootPath, ListStatusPOptions.newBuilder().setRecursive(false).build());
    assertEquals(2, listResult.length);
  }

  @Test
  public void testListCacheConsistency()
      throws IOException, AccessControlException, ExecutionException, InterruptedException,
      TimeoutException {
    String fileContent = "foobar";
    File rootFolder = mTestFolder.newFolder("root");
    String rootPath = rootFolder.getAbsolutePath();
    File f = mTestFolder.newFile("root/f");
    Files.write(f.toPath(), fileContent.getBytes());

    UfsStatus[] listResult =
        mWorker.listStatus(rootPath, ListStatusPOptions.newBuilder().setRecursive(false).build());
    assertNotNull(listResult);
    assertEquals(1, listResult.length);

    FileInfo fileInfo = mWorker.getGrpcFileInfo(f.getPath(), 0);
    loadFileData(f.getPath());
    assertNotNull(fileInfo);

    // Assert that page cache, metadata cache & list cache all cached data properly
    assertTrue(mWorker.getMetaManager().getFromMetaStore(f.getPath()).isPresent());
    assertSame(listResult,
        mWorker.getMetaManager().listCached(rootPath, false).get().mUfsStatuses);
    List<PageId> cachedPages =
        mCacheManager.getCachedPageIdsByFileId(
            new AlluxioURI(f.getPath()).hash(), fileContent.length());
    assertEquals(1, cachedPages.size());

    mWorker.delete(f.getAbsolutePath(), DeletePOptions.getDefaultInstance());
    // Assert that page cache, metadata cache & list cache all removed stale data
    assertFalse(mWorker.getMetaManager().getFromMetaStore(f.getPath()).isPresent());
    assertFalse(mWorker.getMetaManager().listCached(rootPath, false).isPresent());
    cachedPages =
        mCacheManager.getCachedPageIdsByFileId(
            new AlluxioURI(f.getPath()).hash(), fileContent.length());
    assertEquals(0, cachedPages.size());
  }

  private void loadFileData(String path)
      throws ExecutionException, InterruptedException, TimeoutException, IOException,
      AccessControlException {
    UfsStatus ufsStatus = mWorker.getUfs().getStatus(path);
    ufsStatus.setUfsFullPath(new AlluxioURI(path));
    ListenableFuture<List<LoadFileFailure>> load =
        mWorker.load(true, Collections.singletonList(ufsStatus),
            UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false)
                .build());
    List<LoadFileFailure> fileFailures = load.get(30, TimeUnit.SECONDS);
    assertEquals(0, fileFailures.size());
  }

  private void createDummyFile(File testFile, int length) throws Exception {
    OpenFileHandle handle = mWorker.createFile(testFile.getPath(),
        CreateFilePOptions.getDefaultInstance());
    // create file and write some data directly to this file in UFS.
    byte[] buffer = BufferUtils.getIncreasingByteArray(length);
    BufferUtils.writeBufferToFile(testFile.getAbsolutePath(), buffer);

    mWorker.completeFile(testFile.getPath(), CompleteFilePOptions.getDefaultInstance(),
        handle.getUUID().toString());
  }
}
