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

import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.FileFailure;
import alluxio.grpc.Route;
import alluxio.grpc.RouteFailure;
import alluxio.grpc.UfsReadOptions;
import alluxio.grpc.WriteOptions;
import alluxio.util.io.BufferUtils;

import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.io.File;
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

  @Before
  public void before() throws Exception {
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
  public void testLoad()
      throws ExecutionException, InterruptedException, TimeoutException, IOException {
    int numPages = 10;
    long length = mPageSize * numPages;
    String ufsPath = mTestFolder.newFile("test").getAbsolutePath();
    byte[] buffer = BufferUtils.getIncreasingByteArray((int) length);
    BufferUtils.writeBufferToFile(ufsPath, buffer);
    alluxio.grpc.File file = alluxio.grpc.File.newBuilder().setUfsPath(ufsPath).setLength(length).setMountId(1).build();
    ListenableFuture<List<FileFailure>> load = mWorker.load(Collections.singletonList(file),
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build());
    List<FileFailure> fileFailures = load.get(30, TimeUnit.SECONDS);
    Assert.assertEquals(0, fileFailures.size());
    List<PageId> cachedPages =
        mCacheManager.getCachedPageIdsByFileId(new AlluxioURI(ufsPath).hash(), length);
    Assert.assertEquals(numPages, cachedPages.size());
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
        Route.newBuilder().setDst("/b").setSrc("/a").setDstUfsAddress(dstRoot.getAbsolutePath())
             .setSrcUfsAddress(srcRoot.getAbsolutePath()).setLength(length).build();

    WriteOptions writeOptions = WriteOptions.newBuilder().setOverwrite(false).build();
    UfsReadOptions read =
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build();
    ListenableFuture<List<RouteFailure>> copy =
        mWorker.copy(Collections.singletonList(route), read, writeOptions);
    List<RouteFailure> failures = copy.get();
    Assert.assertEquals(0, failures.size());
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
        Route.newBuilder().setDst("/b").setSrc("/a").setDstUfsAddress(dstRoot.getAbsolutePath())
             .setSrcUfsAddress(srcRoot.getAbsolutePath()).setLength(length).build();

    WriteOptions writeOptions = WriteOptions.newBuilder().setOverwrite(false).build();
    ListenableFuture<List<RouteFailure>> copy = mWorker.copy(Collections.singletonList(route),
        null, writeOptions);
    List<RouteFailure> failures = copy.get();
    Assert.assertEquals(1, failures.size());
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
        Route.newBuilder().setDst("/b").setSrc("/a").setDstUfsAddress(dstRoot.getAbsolutePath())
             .setSrcUfsAddress(srcRoot.getAbsolutePath()).build();
    WriteOptions writeOptions = WriteOptions.newBuilder().setOverwrite(false).build();
    UfsReadOptions read =
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build();
    ListenableFuture<List<RouteFailure>> copy =
        mWorker.copy(Collections.singletonList(route), read, writeOptions);
    List<RouteFailure> failures = copy.get();
    Assert.assertEquals(0, failures.size());
    Assert.assertTrue(b.exists());
    Assert.assertTrue(b.isDirectory());
  }

  @Test
  public void testFolderWithFileCopy() throws IOException, ExecutionException, InterruptedException {
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
    int length = 10;
    byte[] buffer = BufferUtils.getIncreasingByteArray(length);
    BufferUtils.writeBufferToFile(c.getAbsolutePath(), buffer);
    List<Route> routes = new ArrayList<>();
    Route route =
        Route.newBuilder().setDst("/b/c").setSrc("/a/c").setDstUfsAddress(dstRoot.getAbsolutePath())
             .setSrcUfsAddress(srcRoot.getAbsolutePath()).setLength(length).build();
    Route route2 =
        Route.newBuilder().setDst("/b").setSrc("/a").setDstUfsAddress(dstRoot.getAbsolutePath())
             .setSrcUfsAddress(srcRoot.getAbsolutePath()).build();
    Route route3 =
        Route.newBuilder().setDst("/b/d").setSrc("/a/d").setDstUfsAddress(dstRoot.getAbsolutePath())
             .setSrcUfsAddress(srcRoot.getAbsolutePath()).build();
    routes.add(route);
    routes.add(route2);
    routes.add(route3);
    WriteOptions writeOptions = WriteOptions.newBuilder().setOverwrite(false).build();
    UfsReadOptions read =
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build();
    ListenableFuture<List<RouteFailure>> copy =
        mWorker.copy(routes, read, writeOptions);
    List<RouteFailure> failures = copy.get();

    Assert.assertEquals(0, failures.size());
    Assert.assertTrue(new File(b, "c").exists());
    Assert.assertTrue(b.exists());
    Assert.assertTrue(b.isDirectory());
    Assert.assertTrue(new File(b, "d").exists());
    Assert.assertTrue(new File(b, "d").isDirectory());
    try (InputStream in = Files.newInputStream(new File(b, "c").toPath())) {
      byte[] readBuffer = new byte[length];
      while (in.read(readBuffer) != -1) {
      }
      Assert.assertArrayEquals(buffer, readBuffer);
    }
  }
}
