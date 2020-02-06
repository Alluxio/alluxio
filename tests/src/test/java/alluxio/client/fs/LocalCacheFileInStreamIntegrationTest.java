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

package alluxio.client.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.BaseFileSystem;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheMode;
import alluxio.client.file.cache.LocalCacheFileInStream;
import alluxio.client.file.cache.LocalCacheFileSystem;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.WritePType;
import alluxio.metrics.ClientMetrics;
import alluxio.metrics.MetricsSystem;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import com.google.common.io.ByteStreams;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;

public final class LocalCacheFileInStreamIntegrationTest extends BaseIntegrationTest {
  private static final int PAGE_SIZE_BYTES = Constants.KB;
  private static final int PAGE_COUNT = 32;
  private static final int CACHE_SIZE_BYTES = PAGE_COUNT * PAGE_SIZE_BYTES;

  @Rule
  public LocalAlluxioClusterResource mClusterResource =
      new LocalAlluxioClusterResource.Builder()
          .setProperty(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, PAGE_SIZE_BYTES)
          .setProperty(PropertyKey.USER_CLIENT_CACHE_SIZE, CACHE_SIZE_BYTES)
          .setProperty(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT, Constants.MB)
          .setProperty(PropertyKey.USER_LOCAL_CACHE_MODE, CacheMode.ENABLED)
          .build();

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  private FileSystemContext mFsContext;
  private FileSystem mFileSystem;
  private FileSystem mExternalFileSystem;
  private String mFilePath;
  private CacheManager mCacheManager;

  @Before
  public void before() throws Exception {
    InstancedConfiguration conf =
        new InstancedConfiguration(mClusterResource.get().getClient().getConf());
    conf.set(PropertyKey.USER_CLIENT_CACHE_DIR, mFolder.getRoot());
    mFsContext = FileSystemContext.create(conf);
    mCacheManager = CacheManager.create(conf);
    // file system client without local cache
    mExternalFileSystem = new BaseFileSystem(mFsContext);
    mFileSystem = new LocalCacheFileSystem(mExternalFileSystem, conf);
    mFilePath = PathUtils.uniqPath();
  }

  @After
  public void after() throws Exception {
    mFsContext.close();
  }

  @Test
  public void read() throws Exception {
    AlluxioURI path = new AlluxioURI(mFilePath);
    FileSystemTestUtils.createByteFile(
        mFileSystem, mFilePath, WritePType.MUST_CACHE, PAGE_SIZE_BYTES);
    // read a file to populate the cache
    try (FileInStream stream = openFile(path)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          PAGE_SIZE_BYTES, ByteStreams.toByteArray(stream)));
    }
    mClusterResource.get().stopWorkers();
    // verify reading from local cache
    try (InputStream stream = openFile(path)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          PAGE_SIZE_BYTES, ByteStreams.toByteArray(stream)));
    }
  }

  @Test
  public void positionedRead() throws Exception {
    AlluxioURI path = new AlluxioURI(mFilePath);
    FileSystemTestUtils.createByteFile(
        mFileSystem, mFilePath, WritePType.MUST_CACHE, PAGE_SIZE_BYTES);
    try (FileInStream stream = openFile(path)) {
      byte[] buffer = new byte[PAGE_SIZE_BYTES / 4];
      int bytesRead = stream.positionedRead(PAGE_SIZE_BYTES / 10, buffer, 0, buffer.length);
      assertEquals(buffer.length, bytesRead);
      assertTrue(BufferUtils.equalIncreasingByteArray(PAGE_SIZE_BYTES / 10, buffer.length, buffer));
    }
    mClusterResource.get().stopWorkers();
    // verify reading whole page from local cache
    try (InputStream stream = openFile(path)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          PAGE_SIZE_BYTES, ByteStreams.toByteArray(stream)));
    }
  }

  @Test
  public void multiPageRead() throws Exception {
    AlluxioURI path = new AlluxioURI(mFilePath);
    int pageCount = 8;
    FileSystemTestUtils.createByteFile(
        mFileSystem, mFilePath, WritePType.MUST_CACHE, pageCount * PAGE_SIZE_BYTES);
    // position read from even pages
    try (FileInStream stream = openFile(path)) {
      byte[] buffer = new byte[PAGE_SIZE_BYTES / 4];
      for (int i = 0; i < pageCount; i += 2) {
        int bytesRead = stream.positionedRead(i * PAGE_SIZE_BYTES, buffer, 0, buffer.length);
        assertEquals(buffer.length, bytesRead);
        assertTrue(
            BufferUtils.equalIncreasingByteArray(i * PAGE_SIZE_BYTES, buffer.length, buffer));
      }
    }
    // verify reading the files from mixed sources
    try (InputStream stream = openFile(path)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          pageCount * PAGE_SIZE_BYTES, ByteStreams.toByteArray(stream)));
    }
  }

  @Test
  public void cacheAndEvict() throws Exception {
    AlluxioURI path = new AlluxioURI(mFilePath);
    FileSystemTestUtils.createByteFile(
        mFileSystem, mFilePath, WritePType.MUST_CACHE, CACHE_SIZE_BYTES * 2);
    // read a file larger than cache size
    try (InputStream stream = openFile(path)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          CACHE_SIZE_BYTES * 2, ByteStreams.toByteArray(stream)));
    }
    mClusterResource.get().stopWorkers();
    // reading second half of file from cache would succeed
    try (FileInStream stream = openFile(path)) {
      stream.seek(CACHE_SIZE_BYTES);
      assertTrue(BufferUtils.equalIncreasingByteArray(CACHE_SIZE_BYTES,
          CACHE_SIZE_BYTES, ByteStreams.toByteArray(stream)));
    }
    // reading first half from local cache would fail
    try (InputStream stream = openFile(path)) {
      mThrown.expect(UnavailableException.class);
      ByteStreams.toByteArray(stream);
    }
  }

  @Test
  @LocalAlluxioClusterResource.Config(
      confParams = {PropertyKey.Name.USER_LOCAL_CACHE_MODE, "DRYRUN"})
  public void dryRun() throws Exception {
    AlluxioURI path = new AlluxioURI(mFilePath);
    int pageCount = 8;
    FileSystemTestUtils.createByteFile(
        mFileSystem, mFilePath, WritePType.MUST_CACHE, pageCount * PAGE_SIZE_BYTES);
    // position read from multiple pages
    try (FileInStream stream = openFile(path)) {
      for (int i = 0; i < pageCount; i += 2) {
        byte[] buffer = new byte[PAGE_SIZE_BYTES / 4];
        int bytesRead = stream.positionedRead(i * PAGE_SIZE_BYTES, buffer, 0, buffer.length);
        assertEquals(buffer.length, bytesRead);
        assertTrue(
            BufferUtils.equalIncreasingByteArray(i * PAGE_SIZE_BYTES, buffer.length, buffer));
        Assert.assertEquals(0,
            MetricsSystem.counter(ClientMetrics.CACHE_BYTES_READ_CACHE).getCount());
        Assert.assertEquals((i / 2 + 1) * buffer.length,
            MetricsSystem.counter(ClientMetrics.CACHE_BYTES_REQUESTED_EXTERNAL).getCount());
        Assert.assertEquals((i / 2 + 1) * PAGE_SIZE_BYTES,
            MetricsSystem.counter(ClientMetrics.CACHE_BYTES_READ_EXTERNAL).getCount());
      }
    }

    // read whole file with some pages "cached"
    try (InputStream stream = openFile(path)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          PAGE_SIZE_BYTES * pageCount, ByteStreams.toByteArray(stream)));
    }
    Assert.assertEquals(4 * PAGE_SIZE_BYTES,
        MetricsSystem.counter(ClientMetrics.CACHE_BYTES_READ_CACHE).getCount());
    Assert.assertEquals(5 * PAGE_SIZE_BYTES,
        MetricsSystem.counter(ClientMetrics.CACHE_BYTES_REQUESTED_EXTERNAL).getCount());
    Assert.assertEquals(pageCount * PAGE_SIZE_BYTES,
        MetricsSystem.counter(ClientMetrics.CACHE_BYTES_READ_EXTERNAL).getCount());

    // checks no data is written
    String cacheDir = mFsContext.getClusterConf().get(PropertyKey.USER_CLIENT_CACHE_DIR);
    File file = new File(cacheDir);
    Assert.assertTrue(!file.exists() || (file.isDirectory() && file.list().length == 0));
  }

  private FileInStream openFile(AlluxioURI path) {
    // opens file stream with cache manager created in the test
    return new LocalCacheFileInStream(path,
        OpenFilePOptions.getDefaultInstance(), mExternalFileSystem, mCacheManager);
  }
}
