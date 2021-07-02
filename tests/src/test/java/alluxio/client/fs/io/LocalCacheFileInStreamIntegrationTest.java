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

package alluxio.client.fs.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemTestUtils;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.WritePType;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;
import alluxio.util.io.BufferUtils;
import alluxio.util.io.PathUtils;

import com.google.common.io.ByteStreams;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
          .setProperty(PropertyKey.USER_CLIENT_CACHE_ENABLED, true)
          .setProperty(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED, false)
          .setProperty(PropertyKey.USER_CLIENT_CACHE_STORE_OVERHEAD, 0)
          .build();

  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  private FileSystemContext mFsContext;
  private FileSystem mFileSystem;
  private String mFilePath;

  @Before
  public void before() throws Exception {
    mFsContext = FileSystemContext.create(mClusterResource.get().getClient().getConf());
    mFileSystem = mClusterResource.get().getClient(mFsContext);
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
    try (FileInStream stream = mFileSystem.openFile(path)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          PAGE_SIZE_BYTES, ByteStreams.toByteArray(stream)));
    }
    mClusterResource.get().stopWorkers();
    // verify reading from local cache
    try (InputStream stream = mFileSystem.openFile(path)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          PAGE_SIZE_BYTES, ByteStreams.toByteArray(stream)));
    }
  }

  @Test
  public void positionedRead() throws Exception {
    AlluxioURI path = new AlluxioURI(mFilePath);
    FileSystemTestUtils.createByteFile(
        mFileSystem, mFilePath, WritePType.MUST_CACHE, PAGE_SIZE_BYTES);
    try (FileInStream stream = mFileSystem.openFile(path)) {
      byte[] buffer = new byte[PAGE_SIZE_BYTES / 4];
      int bytesRead = stream.positionedRead(PAGE_SIZE_BYTES / 10, buffer, 0, buffer.length);
      assertEquals(buffer.length, bytesRead);
      assertTrue(BufferUtils.equalIncreasingByteArray(PAGE_SIZE_BYTES / 10, buffer.length, buffer));
    }
    mClusterResource.get().stopWorkers();
    // verify reading whole page from local cache
    try (InputStream stream = mFileSystem.openFile(path)) {
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
    try (FileInStream stream = mFileSystem.openFile(path)) {
      byte[] buffer = new byte[PAGE_SIZE_BYTES / 4];
      for (int i = 0; i < pageCount; i += 2) {
        int bytesRead = stream.positionedRead(i * PAGE_SIZE_BYTES, buffer, 0, buffer.length);
        assertEquals(buffer.length, bytesRead);
        assertTrue(
            BufferUtils.equalIncreasingByteArray(i * PAGE_SIZE_BYTES, buffer.length, buffer));
      }
    }
    // verify reading the files from mixed sources
    try (InputStream stream = mFileSystem.openFile(path)) {
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
    try (InputStream stream = mFileSystem.openFile(path)) {
      assertTrue(BufferUtils.equalIncreasingByteArray(
          CACHE_SIZE_BYTES * 2, ByteStreams.toByteArray(stream)));
    }
    mClusterResource.get().stopWorkers();
    // reading second half of file from cache would succeed
    try (FileInStream stream = mFileSystem.openFile(path)) {
      stream.seek(CACHE_SIZE_BYTES);
      assertTrue(BufferUtils.equalIncreasingByteArray(CACHE_SIZE_BYTES,
          CACHE_SIZE_BYTES, ByteStreams.toByteArray(stream)));
    }
    // reading first half from local cache would fail
    try (InputStream stream = mFileSystem.openFile(path)) {
      mThrown.expect(UnavailableException.class);
      ByteStreams.toByteArray(stream);
    }
  }
}
