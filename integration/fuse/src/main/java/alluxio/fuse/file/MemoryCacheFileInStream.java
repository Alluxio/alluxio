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
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * A class for handling memory caching. Using an object pool to reuse byte arrays and reduce JVM
 * garbage collection frequency is an effective approach. However, it does introduce some issues
 * that need to be handled carefully. With a 4MB page size, garbage collection does not incur
 * significant overhead. Therefore, for the current situation, we have decided to prioritize
 * delivering a functional feature to meet the requirements. This is a reasonable decision because
 * implementing the functionality is the primary goal, while optimization can be done gradually.
 * This ensures obtaining efficient system performance without prematurely diving into excessive
 * optimization work. In subsequent iterations, the issues introduced by the object pool can be
 * gradually addressed based on needs and performance optimization requirements.
 */
public class MemoryCacheFileInStream extends FileInStream {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryCacheFileInStream.class);

  private static final int PAGE_SIZE;

  private static final Cache<CacheKey, byte[]> CACHE;

  static {
    long pageSize = Configuration.getBytes(PropertyKey.FUSE_MEMORY_CACHE_PAGE_SIZE);
    Preconditions.checkArgument(pageSize <= Integer.MAX_VALUE,
        PropertyKey.FUSE_MEMORY_CACHE_PAGE_SIZE.getName() + " must be less than "
            + Integer.MAX_VALUE);
    PAGE_SIZE = (int) pageSize;
    LOG.info("The page size of memory cache is set to: " + PAGE_SIZE);

    int pageCount = Configuration.getInt(PropertyKey.FUSE_MEMORY_CACHE_PAGE_COUNT);
    Preconditions.checkArgument(pageCount > 0,
        PropertyKey.FUSE_MEMORY_CACHE_PAGE_COUNT.getName() + " must be greater than 0");

    int concurrencyLevel = Configuration.getInt(PropertyKey.FUSE_MEMORY_CACHE_CONCURRENCY_LEVEL);
    Preconditions.checkArgument(concurrencyLevel > 0,
        PropertyKey.FUSE_MEMORY_CACHE_CONCURRENCY_LEVEL.getName() + " must be greater than 0");

    long expireMs = Configuration.getLong(PropertyKey.FUSE_MEMORY_CACHE_EXPIRE_MS);
    Preconditions.checkArgument(expireMs > 0,
        PropertyKey.FUSE_MEMORY_CACHE_EXPIRE_MS.getName() + " must be greater than 0");

    LOG.info("Building memory cache with pageCount = {}, concurrencyLevel = {}, expireMs = {}",
        pageCount, concurrencyLevel, expireMs);
    CACHE = CacheBuilder.newBuilder()
        .maximumSize(pageCount)
        .concurrencyLevel(concurrencyLevel)
        .expireAfterWrite(expireMs, TimeUnit.MILLISECONDS)
        .build();
  }

  private final URIStatus mStatus;
  private final FileInStream mFileInStream;
  private long mPos;

  /**
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
  public int read(final byte[] b, final int off, final int len) throws IOException {
    int length = Math.min(len, b.length - off);
    int offset = off;
    int read = 0;
    while (length > 0) {
      int copyLen = copyCacheBytes(b, offset, length);
      if (copyLen <= 0) {
        break;
      }
      mPos += copyLen;
      read += copyLen;
      length -= copyLen;
      offset += copyLen;
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

  private int copyCacheBytes(byte[] dest, int offset, int length) throws IOException {
    int start = (int) (mPos % PAGE_SIZE);
    long posKey = mPos - start;
    try {
      if (mStatus.getLength() <= mPos) {
        return -1;
      }
      byte[] src = CACHE.get(
          new CacheKey(mStatus.getPath(), mStatus.getLastModificationTimeMs(), posKey),
          () -> {
            mFileInStream.seek(posKey);
            byte[] data = new byte[(int) Math.min(PAGE_SIZE, mStatus.getLength() - posKey)];
            IOUtils.readFully(mFileInStream, data);
            return data;
          });
      int read = Math.min(src.length - start, length);
      read = Math.min(read, dest.length - offset);
      System.arraycopy(src, start, dest, offset, read);
      return read;
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
