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
import alluxio.PositionReader;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AccessControlException;
import alluxio.file.ReadTargetBuffer;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.ExistsPOptions;
import alluxio.grpc.FileInfo;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadDataSubTask;
import alluxio.grpc.LoadFileResponse;
import alluxio.grpc.LoadMetadataSubTask;
import alluxio.grpc.LoadSubTask;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.Route;
import alluxio.grpc.RouteFailure;
import alluxio.grpc.SetAttributePOptions;
import alluxio.grpc.UfsReadOptions;
import alluxio.grpc.WriteOptions;
import alluxio.membership.MembershipManager;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UfsStatus;
import alluxio.util.io.BufferUtils;
import alluxio.wire.WorkerIdentity;
import alluxio.worker.block.BlockMasterClientPool;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
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
  private MembershipManager mMembershipManager;
  private long mPageSize;
  private static final GetStatusPOptions GET_STATUS_OPTIONS_MUST_SYNC =
      GetStatusPOptions.newBuilder().setCommonOptions(
          FileSystemMasterCommonPOptions.newBuilder().setSyncIntervalMs(0)).build();

  @Before
  public void before() throws Exception {
    Configuration.set(PropertyKey.WORKER_FAST_DATA_LOAD_ENABLED, true);
    Configuration.set(PropertyKey.DORA_WORKER_METASTORE_ROCKSDB_DIR,
        mTestFolder.newFolder("rocks"));
    Configuration.set(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE, 10);
    Configuration.set(PropertyKey.UNDERFS_XATTR_CHANGE_ENABLED, false);
    CacheManagerOptions cacheManagerOptions =
        CacheManagerOptions.createForWorker(Configuration.global());
    mPageSize = Configuration.getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE);
    PageMetaStore pageMetaStore =
        PageMetaStore.create(CacheManagerOptions.createForWorker(Configuration.global()));
    mCacheManager =
        CacheManager.Factory.create(Configuration.global(), cacheManagerOptions, pageMetaStore);
    mMembershipManager =
        MembershipManager.Factory.create(Configuration.global());
    DoraUfsManager ufsManager = new DoraUfsManager();
    DoraMetaManager metaManager = new DoraMetaManager(Configuration.global(),
        mCacheManager, ufsManager);
    mWorker = new PagedDoraWorker(new AtomicReference<>(
            WorkerIdentity.ParserV0.INSTANCE.fromLong(1L)),
        Configuration.global(), mCacheManager, mMembershipManager,
        new BlockMasterClientPool(), ufsManager, metaManager, FileSystemContext.create());
  }

  @After
  public void after() throws Exception {
    mWorker.close();
  }

  @Test
  public void testLoad() throws Exception {
    int numPages = 10;
    long length = mPageSize * numPages;
    String ufsPath = mTestFolder.newFile("test").getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) length);
    BufferUtils.writeBufferToFile(ufsPath, buffer);
    loadFileData(ufsPath);
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
    assertTrue(mWorker.getMetaManager().getFromMetaStore(ufsPath).isPresent());
  }

  @Test
  public void testLoadDataWithOffsetLength() throws Exception {
    int numPages = 10;
    long length = mPageSize * numPages;
    String ufsPath = mTestFolder.newFile("test").getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) length);
    BufferUtils.writeBufferToFile(ufsPath, buffer);
    int numCachedPages = 4;
    UfsStatus ufsStatus = mWorker.getUfsInstance(ufsPath).getStatus(ufsPath);
    ufsStatus.setUfsFullPath(new AlluxioURI(ufsPath));

    LoadDataSubTask block = LoadDataSubTask.newBuilder().setOffsetInFile(mPageSize)
                                           .setLength(mPageSize * numCachedPages)
                                           .setUfsPath(ufsPath).setUfsStatus(ufsStatus.toProto())
                                           .build();
    ListenableFuture<LoadFileResponse> load = mWorker.load(
        Collections.singletonList(LoadSubTask.newBuilder().setLoadDataSubtask(block).build()),
        false,
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build());
    LoadFileResponse response = load.get(30, TimeUnit.SECONDS);
    assertEquals(0, response.getFailuresCount());
    List<PageId> cachedPages =
        mCacheManager.getCachedPageIdsByFileId(new AlluxioURI(ufsPath).hash(), length);
    assertEquals(numCachedPages, cachedPages.size());
    int start = (int) mPageSize;
    for (PageId pageId : cachedPages) {
      byte[] buff = new byte[(int) mPageSize];
      mCacheManager.get(pageId, (int) mPageSize, buff, 0);
      assertTrue(BufferUtils.equalIncreasingByteArray(start, (int) mPageSize, buff));
      start += mPageSize;
    }
    assertFalse(mWorker.getMetaManager().getFromMetaStore(ufsPath).isPresent());
  }

  @Test
  public void testLoadMetaDataOnly() throws Exception {
    int numPages = 10;
    long length = mPageSize * numPages;
    String ufsPath = mTestFolder.newFile("test").getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) length);
    BufferUtils.writeBufferToFile(ufsPath, buffer);
    UfsStatus ufsStatus = mWorker.getUfsInstance(ufsPath).getStatus(ufsPath);
    ufsStatus.setUfsFullPath(new AlluxioURI(ufsPath));
    ListenableFuture<LoadFileResponse> load = mWorker.load(Collections.singletonList(
            LoadSubTask.newBuilder().setLoadMetadataSubtask(
                LoadMetadataSubTask.newBuilder()
                                   .setUfsStatus(ufsStatus.toProto()).build()).build()), false,
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build());
    load.get(30, TimeUnit.SECONDS);
    List<PageId> cachedPages =
        mCacheManager.getCachedPageIdsByFileId(new AlluxioURI(ufsPath).hash(), length);
    assertEquals(0, cachedPages.size());
    assertTrue(mWorker.getMetaManager().getFromMetaStore(ufsPath).isPresent());
  }

  @Test
  public void testCacheData() throws Exception {
    int numPages = 10;
    long length = mPageSize * numPages;
    String ufsPath = mTestFolder.newFile("test").getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) length);
    BufferUtils.writeBufferToFile(ufsPath, buffer);

    mWorker.cacheData(ufsPath, length, 0, false);
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
  public void testCacheDataNotPageAligned() throws Exception {
    int numPages = 10;
    long length = mPageSize * numPages - 1;
    String ufsPath = mTestFolder.newFile("test").getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) length);
    BufferUtils.writeBufferToFile(ufsPath, buffer);

    mWorker.cacheData(ufsPath, length, 0, false);
    List<PageId> cachedPages =
        mCacheManager.getCachedPageIdsByFileId(new AlluxioURI(ufsPath).hash(), length);
    assertEquals(numPages, cachedPages.size());
    int start = 0;
    for (PageId pageId : cachedPages) {
      long size = numPages == pageId.getPageIndex() + 1 ? length % mPageSize : mPageSize;
      byte[] buff = new byte[(int) size];
      mCacheManager.get(pageId, (int) size, buff, 0);
      assertTrue(BufferUtils.equalIncreasingByteArray(start, (int) size, buff));
      start += mPageSize;
    }
  }

  @Test
  public void testCacheDataPartial() throws Exception {
    int numPages = 10;
    long length = mPageSize * numPages;
    String ufsPath = mTestFolder.newFile("test").getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) length);
    BufferUtils.writeBufferToFile(ufsPath, buffer);

    int startPage = 2;
    // Loading bytes [19, 40] -> page 1,2,3,4 will be loaded
    mWorker.cacheData(ufsPath, 2 * mPageSize + 2, startPage * mPageSize - 1, false);
    List<PageId> cachedPages =
        mCacheManager.getCachedPageIdsByFileId(new AlluxioURI(ufsPath).hash(), length);
    assertEquals(4, cachedPages.size());
    int start = (int) mPageSize;
    for (PageId pageId : cachedPages) {
      byte[] buff = new byte[(int) mPageSize];
      mCacheManager.get(pageId, (int) mPageSize, buff, 0);
      assertTrue(BufferUtils.equalIncreasingByteArray(start, (int) mPageSize, buff));
      start += mPageSize;
    }
  }

  @Test
  public void testLoadFromReader() throws IOException {
    String ufsPath = "testLoadRemote";
    mWorker.loadDataFromRemote(ufsPath, 0, 10, new TestDataReader(100), (int) mPageSize);
    byte[] buffer = new byte[10];
    String fileId = new AlluxioURI(ufsPath).hash();
    List<PageId> cachedPages = mCacheManager.getCachedPageIdsByFileId(fileId, 10);
    assertEquals(1, cachedPages.size());
    mCacheManager.get(new PageId(fileId, 0), 10, buffer, 0);
    assertTrue(BufferUtils.equalIncreasingByteArray(0, 10, buffer));
  }

  @Test
  public void testLoadBlockFromReader() throws IOException {
    String ufsPath = "testLoadBlockRemote";
    long offset = mPageSize;
    int numPages = 3;
    long lengthToLoad = numPages * mPageSize + 5;
    mWorker.loadDataFromRemote(ufsPath, offset, lengthToLoad,
        new TestDataReader((int) (5 * mPageSize)), (int) mPageSize);
    String fileId = new AlluxioURI(ufsPath).hash();
    List<PageId> cachedPages = mCacheManager.getCachedPageIdsByFileId(fileId, 5 * mPageSize);
    assertEquals(4, cachedPages.size());
    for (int i = 1; i < 4; i++) {
      byte[] buffer = new byte[(int) mPageSize];
      mCacheManager.get(new PageId(fileId, i), (int) mPageSize, buffer, 0);
      assertTrue(BufferUtils.equalIncreasingByteArray((int) (offset + (i - 1) * mPageSize),
          (int) mPageSize, buffer));
    }
    // test last page with 5 bytes
    byte[] buffer = new byte[(int) 5];
    mCacheManager.get(new PageId(fileId, 4), 5, buffer, 0);
    assertTrue(BufferUtils.equalIncreasingByteArray((int) (offset + 3 * mPageSize), 5, buffer));
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

    // assertEquals(0, failures.size()); There could be a chance that this will be pass
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
  public void testFolderWithFileCopyWithSkip()
      throws IOException, ExecutionException, InterruptedException {
    File srcRoot = mTestFolder.newFolder("src");
    File dstRoot = mTestFolder.newFolder("dst");
    // create test file under mSrcFolder
    File a = new File(srcRoot, "a");
    a.mkdirs();
    File c = new File(a, "c");
    c.createNewFile();
    File d = new File(a, "d");
    d.createNewFile();
    File b = new File(dstRoot, "b");
    b.mkdirs();
    File dstC = new File(b, "c");
    File dstD = new File(b, "d");
    dstD.createNewFile();
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

    // assertEquals(1, failures.size()); There could be a small chance that this will be 1
    assertTrue(failures.get(0).getIsSkip());
    Assert.assertTrue(dstC.exists());
    Assert.assertTrue(b.exists());
    Assert.assertTrue(b.isDirectory());
    Assert.assertTrue(dstD.exists());
    Assert.assertTrue(dstD.isFile());
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
  public void testSingleFileCopySkip() throws IOException, ExecutionException,
      InterruptedException {
    File srcRoot = mTestFolder.newFolder("src");
    File dstRoot = mTestFolder.newFolder("dst");
    // create test file under mSrcFolder
    File a = new File(srcRoot, "a");
    a.createNewFile();
    File b = new File(dstRoot, "b");
    b.createNewFile();
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
    ListenableFuture<List<RouteFailure>> copy =
        mWorker.copy(Collections.singletonList(route), read, writeOptions);
    List<RouteFailure> failures = copy.get();
    assertEquals(1, failures.size());
    assertTrue(failures.get(0).getIsSkip());
  }

  @Test
  public void testSingleFileMoveWithFileAlreadyExist() throws IOException, ExecutionException,
      InterruptedException {
    File srcRoot = mTestFolder.newFolder("src");
    File dstRoot = mTestFolder.newFolder("dst");
    // create test file under mSrcFolder
    File a = new File(srcRoot, "a");
    a.createNewFile();
    File b = new File(dstRoot, "b");
    b.createNewFile();
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
    assertEquals(1, failures.size());
    assertFalse(failures.get(0).hasIsSkip());
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
  public void testFolderWithFileMove()
      throws IOException, ExecutionException, InterruptedException {
    File srcRoot = mTestFolder.newFolder("src");
    File dstRoot = mTestFolder.newFolder("dst");
    // create test file under mSrcFolder
    File a = new File(srcRoot, "a");
    assertTrue(a.mkdirs());
    File c = new File(a, "c");
    assertTrue(c.createNewFile());
    File d = new File(a, "d");
    assertTrue(d.mkdirs());
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
        Route.newBuilder().setDst(dstD.getAbsolutePath()).setSrc(d.getAbsolutePath()).build();
    Route route3 =
        Route.newBuilder().setDst(b.getAbsolutePath()).setSrc(a.getAbsolutePath()).build();
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
    // assertFalse(a.exists()); We don't throw delete failure on the UFS side,
    // so there is a small chance that directory a will be deleted successfully.
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

  @Test
  public void testPageMetadataMetrics() throws Exception {
    String fileContent = "foobar";
    File rootFolder = mTestFolder.newFolder("root");
    String rootPath = rootFolder.getAbsolutePath();

    mWorker.listStatus(rootPath, ListStatusPOptions.newBuilder().setRecursive(false).build());
    assertEquals(1, MetricsSystem.counter(
        MetricKey.WORKER_LIST_STATUS_EXTERNAL_REQUESTS.getName()).getCount());
    assertEquals(0,
        MetricsSystem.counter(MetricKey.WORKER_LIST_STATUS_HIT_REQUESTS.getName()).getCount());
    mWorker.listStatus(rootPath, ListStatusPOptions.newBuilder().setRecursive(false).build());
    assertEquals(1, MetricsSystem.counter(
        MetricKey.WORKER_LIST_STATUS_EXTERNAL_REQUESTS.getName()).getCount());
    assertEquals(1,
        MetricsSystem.counter(MetricKey.WORKER_LIST_STATUS_HIT_REQUESTS.getName()).getCount());

    File f = mTestFolder.newFile("root/f");
    Files.write(f.toPath(), fileContent.getBytes());
    mWorker.getFileInfo(f.getPath(), GetStatusPOptions.newBuilder().build());
    assertEquals(1, MetricsSystem.counter(
        MetricKey.WORKER_GET_FILE_INFO_EXTERNAL_REQUESTS.getName()).getCount());
    assertEquals(0,
        MetricsSystem.counter(MetricKey.WORKER_GET_FILE_INFO_HIT_REQUESTS.getName()).getCount());
    mWorker.getFileInfo(f.getPath(), GetStatusPOptions.newBuilder().build());
    assertEquals(1, MetricsSystem.counter(
        MetricKey.WORKER_GET_FILE_INFO_EXTERNAL_REQUESTS.getName()).getCount());
    assertEquals(1,
        MetricsSystem.counter(MetricKey.WORKER_GET_FILE_INFO_HIT_REQUESTS.getName()).getCount());
  }

  private void loadFileData(String path)
      throws ExecutionException, InterruptedException, TimeoutException, IOException,
      AccessControlException {
    UfsStatus ufsStatus = mWorker.getUfsInstance(path).getStatus(path);
    ufsStatus.setUfsFullPath(new AlluxioURI(path));

    LoadDataSubTask block =
        LoadDataSubTask.newBuilder().setLength(ufsStatus.asUfsFileStatus().getContentLength())
                       .setOffsetInFile(0).setUfsPath(ufsStatus.getUfsFullPath().toString())
                       .setUfsStatus(ufsStatus.toProto()).build();
    ListenableFuture<LoadFileResponse> load = mWorker.load(
        Arrays.asList(LoadSubTask.newBuilder().setLoadMetadataSubtask(
                LoadMetadataSubTask.newBuilder().setUfsStatus(ufsStatus.toProto()).build()).build(),
            LoadSubTask.newBuilder().setLoadDataSubtask(block).build()), false,
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build());
    LoadFileResponse response = load.get(30, TimeUnit.SECONDS);
    assertEquals(0, response.getFailuresCount());
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

  @Test
  public void testExists() throws Exception {
    File rootFolder = mTestFolder.newFolder("root");
    String rootPath = rootFolder.getAbsolutePath();
    assertTrue(mWorker.exists(rootPath, ExistsPOptions.getDefaultInstance()));
    String fileContent = "foobar";
    File f = mTestFolder.newFile("root/f");
    Files.write(f.toPath(), fileContent.getBytes());
    assertTrue(mWorker.exists(f.getAbsolutePath(), ExistsPOptions.getDefaultInstance()));
    mWorker.delete(f.getAbsolutePath(), DeletePOptions.getDefaultInstance());
    assertFalse(mWorker.exists(f.getAbsolutePath(), ExistsPOptions.getDefaultInstance()));
    mWorker.delete(rootPath, DeletePOptions.getDefaultInstance());
    assertFalse(mWorker.exists(rootPath, ExistsPOptions.getDefaultInstance()));
  }

  @Test
  public void testRename() throws IOException, AccessControlException {
    File srcFolder = mTestFolder.newFolder("root");
    String rootPath = srcFolder.getAbsolutePath();
    mWorker.rename(rootPath, rootPath + "2", RenamePOptions.getDefaultInstance());
    assertFalse(mWorker.exists(rootPath, ExistsPOptions.getDefaultInstance()));
    assertTrue(mWorker.exists(rootPath + "2", ExistsPOptions.getDefaultInstance()));
    String fileContent = "foobar";
    File f = mTestFolder.newFile("root2/f");
    Files.write(f.toPath(), fileContent.getBytes());
    mWorker.rename(f.getAbsolutePath(), f.getAbsolutePath() + "2",
        RenamePOptions.getDefaultInstance());
    assertFalse(mWorker.exists(f.getAbsolutePath(), ExistsPOptions.getDefaultInstance()));
    assertTrue(mWorker.exists(f.getAbsolutePath() + "2", ExistsPOptions.getDefaultInstance()));
  }

  private class TestDataReader implements PositionReader {
    private final byte[] mBuffer;

    public TestDataReader(int length) {
      mBuffer = BufferUtils.getIncreasingByteArray(length);
    }

    @Override
    public int readInternal(long position, ReadTargetBuffer buffer, int length) {
      int start = (int) position;
      int end = start + length;
      if (end > mBuffer.length) {
        end = mBuffer.length;
      }
      int size = end - start;
      buffer.writeBytes(mBuffer, start, size);
      return size;
    }
  }
}
