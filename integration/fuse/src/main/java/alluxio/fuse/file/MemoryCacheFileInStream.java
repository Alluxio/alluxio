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

package alluxio.fuse.file;

import alluxio.client.file.FileInStream;
import alluxio.client.file.URIStatus;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * A class for handling memory caching.
 */
public class MemoryCacheFileInStream extends FileInStream {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryCacheFileInStream.class);

  private static final long CACHE_SIZE = Configuration.getBytes(
      PropertyKey.FUSE_MEMORY_CACHE_PAGE_SIZE);

  private static final Cache<CacheKey, byte[]> CACHE = CacheBuilder.newBuilder()
      .maximumSize(Configuration.getInt(PropertyKey.FUSE_MEMORY_CACHE_PAGE_COUNT))
      .concurrencyLevel(Configuration.getInt(PropertyKey.FUSE_MEMORY_CACHE_CONCURRENCY_LEVEL))
      .expireAfterWrite(Configuration.getLong(PropertyKey.FUSE_MEMORY_CACHE_EXPIRE_MS),
          TimeUnit.MILLISECONDS)
      .build();

  private final URIStatus mStatus;
  private final FileInStream mFileInStream;
  private long mPos;

  /**
   *
   * @param uriStatus
   * @param fileInStream
   * @throws IOException
   * @throws AlluxioException
   */
  public MemoryCacheFileInStream(URIStatus uriStatus, FileInStream fileInStream)
      throws IOException, AlluxioException {
    mStatus = uriStatus;
    mFileInStream = fileInStream;
  }

  @Override
  public long remaining() {
    return mStatus.getLength() - mPos;
  }

  @Override
  public int positionedRead(long position, byte[] buffer, int offset, int length)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getPos() throws IOException {
    return mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    mPos = pos;
  }

  @Override
  public int read() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int length = Math.min(len, b.length - off);
    int read = 0;
    while (length > 0) {
      byte[] bytes = getCacheBytes(length);
      if (bytes.length == 0) {
        break;
      }
      int copyLen = Math.min(length, bytes.length);
      System.arraycopy(bytes, 0, b, off + read, copyLen);
      mPos += copyLen;
      read += copyLen;
      length -= copyLen;
    }
    return read == 0 ? -1 : read;
  }

  @Override
  public int read(ByteBuffer buf) throws IOException {
    return super.read(buf);
  }

  @Override
  public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
    byteBuffer.position(off);
    int length = Math.min(len, byteBuffer.capacity() - off);
    byte[] data = new byte[length];
    int read = read(data);
    if (read > 0) {
      byteBuffer.put(data, 0, read);
    }
    return read;
  }

  @Override
  public void unbuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long skip(long n) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int available() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    mFileInStream.close();
  }

  @Override
  public synchronized void mark(int readlimit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void reset() throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean markSupported() {
    throw new UnsupportedOperationException();
  }

  private byte[] getCacheBytes(int length) throws IOException {
    long posKey = (mPos / CACHE_SIZE) * CACHE_SIZE;
    int start = (int) (mPos % CACHE_SIZE);
    try {
      if (mStatus.getLength() <= mPos) {
        return new byte[0];
      }
      byte[] bytes = CACHE.get(
          new CacheKey(mStatus.getPath(), mStatus.getLastModificationTimeMs(), posKey),
          () -> {
            mFileInStream.seek(posKey);
            byte[] data = new byte[(int) Math.min(CACHE_SIZE, mStatus.getLength() - posKey)];
            IOUtils.readFully(mFileInStream, data);
            return data;
          });
      return Arrays.copyOfRange(bytes, start, Math.min(bytes.length, start + length));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * A class used as a cache key.
   */
  public static class CacheKey {

    private final String mPath;
    private final long mLastModification;
    private final long mPos;

    /**
     *
     * @param path
     * @param lastModification
     * @param pos
     */
    public CacheKey(String path, long lastModification, long pos) {
      mPath = path;
      mLastModification = lastModification;
      mPos = pos;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CacheKey cacheKey = (CacheKey) o;
      return mLastModification == cacheKey.mLastModification && mPos == cacheKey.mPos
          && Objects.equal(mPath, cacheKey.mPath);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(mPath, mLastModification, mPos);
    }
  }

  /**
   * Discards all entries in the cache. Just use it in tests.
   */
  @VisibleForTesting
  protected static void invalidateAllCache() {
    CACHE.invalidateAll();
  }
}
