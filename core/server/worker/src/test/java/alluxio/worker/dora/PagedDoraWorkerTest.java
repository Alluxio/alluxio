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
import alluxio.grpc.File;
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
    File file = File.newBuilder().setUfsPath(ufsPath).setLength(length).setMountId(1).build();
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
    java.io.File srcFolder = mTestFolder.newFolder("src");
    java.io.File dstFolder = mTestFolder.newFolder("dst");
    // create test file under mSrcFolder
    java.io.File a = new java.io.File(srcFolder, "a");
    a.createNewFile();
    java.io.File b = new java.io.File(dstFolder, "b");
    int length = 10;
    byte[] buffer = BufferUtils.getIncreasingByteArray(length);
    BufferUtils.writeBufferToFile(a.getAbsolutePath(), buffer);
    Route route =
        Route.newBuilder().setDst("/b").setSrc("/a").setDstUfsAddress(dstFolder.getAbsolutePath())
             .setSrcUfsAddress(srcFolder.getAbsolutePath()).setLength(length).build();

    WriteOptions writeOptions = WriteOptions.newBuilder().setOverwrite(false).build();
    UfsReadOptions read =
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build();
    ListenableFuture<List<RouteFailure>> copy =
        mWorker.copy(Collections.singletonList(route), read, writeOptions);
    List<RouteFailure> failures = copy.get();
    Assert.assertEquals(0, failures.size());
    // check if the file is copied under mDstFolder
    Assert.assertTrue(b.exists());
    // open file b through java file input stream
    try (InputStream in = Files.newInputStream(b.toPath())) {
      byte[] readBuffer = new byte[length];
      while (in.read(readBuffer) != -1) {
      }
      Assert.assertArrayEquals(buffer, readBuffer);
    }
  }

  @Test
  public void testCopyException() throws IOException, ExecutionException, InterruptedException {
    java.io.File srcFolder = mTestFolder.newFolder("srcException");
    java.io.File dstFolder = mTestFolder.newFolder("dstException");
    // create test file under mSrcFolder
    java.io.File a = new java.io.File(srcFolder, "a");
    a.createNewFile();
    java.io.File b = new java.io.File(dstFolder, "b");
    int length = 10;
    byte[] buffer = BufferUtils.getIncreasingByteArray(length);
    BufferUtils.writeBufferToFile(a.getAbsolutePath(), buffer);
    Route route =
        Route.newBuilder().setDst("/b").setSrc("/a").setDstUfsAddress(dstFolder.getAbsolutePath())
             .setSrcUfsAddress(srcFolder.getAbsolutePath()).setLength(length).build();

    WriteOptions writeOptions = WriteOptions.newBuilder().setOverwrite(false).build();
    ListenableFuture<List<RouteFailure>> copy = mWorker.copy(Collections.singletonList(route),
        null, writeOptions);
    List<RouteFailure> failures = copy.get();
    Assert.assertEquals(1, failures.size());
    Assert.assertFalse(b.exists());
  }

  @Test
  public void testSingleFolderCopy() throws IOException, ExecutionException, InterruptedException {
    java.io.File srcFolder = mTestFolder.newFolder("src");
    java.io.File dstFolder = mTestFolder.newFolder("dst");
    // create test file under mSrcFolder
    java.io.File a = new java.io.File(srcFolder, "a");
    a.mkdirs();
    java.io.File b = new java.io.File(dstFolder, "b");
    int length = 10;
    Route route =
        Route.newBuilder().setDst("/b").setSrc("/a").setDstUfsAddress(dstFolder.getAbsolutePath())
             .setSrcUfsAddress(srcFolder.getAbsolutePath()).setLength(length).build();
    WriteOptions writeOptions = WriteOptions.newBuilder().setOverwrite(false).build();
    UfsReadOptions read =
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build();
    ListenableFuture<List<RouteFailure>> copy =
        mWorker.copy(Collections.singletonList(route), read, writeOptions);
    List<RouteFailure> failures = copy.get();
    Assert.assertEquals(0, failures.size());
    // check if the file is copied under mDstFolder
    Assert.assertTrue(b.exists());
    Assert.assertTrue(b.isDirectory());
  }

  @Test
  public void testFolderWithFileCopy() throws IOException, ExecutionException, InterruptedException {
    java.io.File srcFolder = mTestFolder.newFolder("src");
    java.io.File dstFolder = mTestFolder.newFolder("dst");
    // create test file under mSrcFolder
    java.io.File a = new java.io.File(srcFolder, "a");
    a.mkdirs();
    java.io.File c = new java.io.File(a, "c");
    c.createNewFile();
    java.io.File b = new java.io.File(dstFolder, "b");
    int length = 10;
    byte[] buffer = BufferUtils.getIncreasingByteArray(length);
    BufferUtils.writeBufferToFile(c.getAbsolutePath(), buffer);
    List<Route> routes = new ArrayList<>();
    Route route =
        Route.newBuilder().setDst("/b/c").setSrc("/a/c").setDstUfsAddress(dstFolder.getAbsolutePath())
             .setSrcUfsAddress(srcFolder.getAbsolutePath()).setLength(length).build();
    Route route2 =
        Route.newBuilder().setDst("/b").setSrc("/a").setDstUfsAddress(dstFolder.getAbsolutePath())
             .setSrcUfsAddress(srcFolder.getAbsolutePath()).setLength(length).build();
    routes.add(route);
    routes.add(route2);
    WriteOptions writeOptions = WriteOptions.newBuilder().setOverwrite(false).build();
    UfsReadOptions read =
        UfsReadOptions.newBuilder().setUser("test").setTag("1").setPositionShort(false).build();
    ListenableFuture<List<RouteFailure>> copy =
        mWorker.copy(Collections.singletonList(route), read, writeOptions);
    List<RouteFailure> failures = copy.get();
    Assert.assertEquals(0, failures.size());
    // check if the file is copied under mDstFolder
    Assert.assertTrue(c.exists());
    Assert.assertTrue(b.exists());
    Assert.assertTrue(b.isDirectory());
    // open file b through java file input stream
    try (InputStream in = Files.newInputStream(c.toPath())) {
      byte[] readBuffer = new byte[length];
      while (in.read(readBuffer) != -1) {
      }
      Assert.assertArrayEquals(buffer, readBuffer);
    }
  }
}
