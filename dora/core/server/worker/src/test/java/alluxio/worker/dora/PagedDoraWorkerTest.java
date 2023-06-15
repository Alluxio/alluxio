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
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.LoadFileFailure;
import alluxio.grpc.Route;
import alluxio.grpc.RouteFailure;
import alluxio.grpc.UfsReadOptions;
import alluxio.grpc.WriteOptions;
import alluxio.underfs.Fingerprint;
import alluxio.underfs.UfsStatus;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;
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
    Assert.assertFalse(b.exists());
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
    Assert.assertFalse(a.exists());
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
    Assert.assertFalse(b.exists());
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
    Assert.assertFalse(a.exists());
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
    Assert.assertFalse(a.exists());
    Assert.assertFalse(c.exists());
    Assert.assertFalse(d.exists());
    try (InputStream in = Files.newInputStream(dstC.toPath())) {
      byte[] readBuffer = new byte[length];
      while (in.read(readBuffer) != -1) {
      }
      Assert.assertArrayEquals(buffer, readBuffer);
    }
  }

  @Test
  public void testGetFileInfoNoFingerprint()
      throws AccessControlException, IOException, ExecutionException, InterruptedException,
      TimeoutException {
    testGetFileInfo(false);
    testGetFileInfoDir(false);
  }

  @Test
  public void testGetFileInfoPopulateFingerprint()
      throws AccessControlException, IOException, ExecutionException, InterruptedException,
      TimeoutException {
    testGetFileInfo(true);
    testGetFileInfoDir(true);
  }

  private void testGetFileInfo(boolean populateFingerprint)
      throws AccessControlException, IOException, ExecutionException, InterruptedException,
      TimeoutException {
    mWorker.setPopulateMetadataFingerprint(populateFingerprint);
    String fileContent = "foobar";
    String updatedFileContent = "foobarbaz";
    File f = mTestFolder.newFile();
    Files.write(f.toPath(), fileContent.getBytes());

    var result = mWorker.getFileInfo(f.getPath(), GetStatusPOptions.getDefaultInstance());
    List<PageId> cachedPages =
        mCacheManager.getCachedPageIdsByFileId(
            new AlluxioURI(f.getPath()).hash(), fileContent.length());
    assertEquals(fileContent.length(), result.getLength());
    assertEquals(0, cachedPages.size());
    if (populateFingerprint) {
      assertTrue(
          Preconditions.checkNotNull(Fingerprint.parse(result.getUfsFingerprint())).isValid());
    }

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
    assertEquals(populateFingerprint ? 1 : 0, cachedPages.size());
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
    if (populateFingerprint) {
      assertTrue(
          Preconditions.checkNotNull(Fingerprint.parse(result.getUfsFingerprint())).isValid());
    }

    assertTrue(f.delete());
    result = mWorker.getFileInfo(f.getPath(), GetStatusPOptions.getDefaultInstance());
    assertEquals(0, cachedPages.size());
    assertEquals(updatedFileContent.length(), result.getLength());

    assertThrows(FileNotFoundException.class, () ->
        mWorker.getFileInfo(f.getPath(), GetStatusPOptions.newBuilder().setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0)).build()));
  }

  private void testGetFileInfoDir(boolean populateFingerprint)
      throws AccessControlException, IOException {
    mWorker.setPopulateMetadataFingerprint(populateFingerprint);
    File f = mTestFolder.newFolder();

    var result = mWorker.getFileInfo(f.getPath(), GetStatusPOptions.getDefaultInstance());
    assertTrue(result.isFolder());

    result = mWorker.getFileInfo(f.getPath(), GET_STATUS_OPTIONS_MUST_SYNC);
    assertTrue(result.isFolder());
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
}
