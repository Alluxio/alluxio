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

import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.store.PageReadTargetBuffer;
import alluxio.conf.AlluxioConfiguration;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.NioDirectBufferPool;
import alluxio.underfs.FileId;
import alluxio.underfs.PagedUfsReader;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.page.NettyBufTargetBuffer;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * Paged file reader.
 */
public class PagedFileReader extends BlockReader {
  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);
  private final FileId mFileId;
  private final long mFileSize;
  private final long mPageSize;
  private final CacheManager mCacheManager;
  private final PagedUfsReader mUfsReader;
  private long mPos;
  private boolean mClosed = false;

  /**
   * Constructor.
   * @param conf
   * @param cacheManager
   * @param pagedUfsReader
   * @param fileId
   * @param fileSize
   * @param startPosition
   * @param pageSize
   */
  public PagedFileReader(AlluxioConfiguration conf, CacheManager cacheManager,
      PagedUfsReader pagedUfsReader, FileId fileId,
      long fileSize, long startPosition, long pageSize) {
    mCacheManager = Preconditions.checkNotNull(cacheManager, "cacheManager");
    mFileId = Preconditions.checkNotNull(fileId, "fileId");
    mFileSize = fileSize;
    mPos = startPosition;
    mPageSize = pageSize;
    mUfsReader = pagedUfsReader;
  }

  // contract:
  // 1.
  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    if (length == 0 || offset >= mFileSize) {
      return EMPTY_BYTE_BUFFER;
    }

    // cap length to the remaining of block, as the caller may pass in a longer length than what
    // is left in the block, but expect as many bytes as there is
    length = Math.min(length, mPos - offset);
    ensureReadable(offset, length);

    // must not use pooled buffer, see interface implementation note
    ByteBuffer buffer = ByteBuffer.allocateDirect((int) length);
    ByteBuf buf = Unpooled.wrappedBuffer(buffer);
    // Unpooled.wrappedBuffer returns a buffer with writer index set to capacity, so writable
    // bytes is 0, needs explicit clear
    buf.clear();
    long bytesRead = read(buf, offset, length);
    if (bytesRead < 0) {
      return EMPTY_BYTE_BUFFER;
    }
    buffer.position(0);
    buffer.limit((int) bytesRead);
    return buffer;
  }

  /**
   * Preconditions:
   * 1. reader not closed
   * 2. offset and length must be valid, check them with ensureReadable
   * 3. enough space left in buffer for the bytes to read
   */
  private long read(ByteBuf byteBuf, long offset, long length) throws IOException {
    Preconditions.checkArgument(byteBuf.writableBytes() >= length,
        "buffer overflow, trying to write %s bytes, only %s writable",
        length, byteBuf.writableBytes());
    PageReadTargetBuffer target = new NettyBufTargetBuffer(byteBuf);
    int bytesRead = 0;
    while (bytesRead < length) {
      long pos = offset + bytesRead;
      long pageIndex = pos / mPageSize;
      PageId pageId = new PageId(mFileId.toString(), pageIndex);
      int currentPageOffset = (int) (pos % mPageSize);
      int bytesLeftInPage =
          (int) Math.min(mPageSize - currentPageOffset, length - bytesRead);
      int bytesReadFromCache = mCacheManager.get(
          pageId, currentPageOffset, bytesLeftInPage, target, CacheContext.defaults());
      if (bytesReadFromCache > 0) {
        bytesRead += bytesReadFromCache;
        MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE.getName()).mark(bytesRead);
      } else {
        // get the page at pageIndex as a whole from UFS
        ByteBuffer ufsBuf = NioDirectBufferPool.acquire((int) mPageSize);
        try {
          int pageBytesRead = mUfsReader.readPageAtIndex(ufsBuf, pageIndex);
          if (pageBytesRead > 0) {
            ufsBuf.position(currentPageOffset);
            ufsBuf.limit(currentPageOffset + bytesLeftInPage);
            byteBuf.writeBytes(ufsBuf);
            bytesRead += bytesLeftInPage;
            MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL.getName())
                .mark(bytesLeftInPage);
            ufsBuf.rewind();
            ufsBuf.limit(pageBytesRead);
            if (mUfsReader.getUfsReadOptions().isCacheIntoAlluxio()) {
              mCacheManager.put(pageId, ufsBuf);
            }
          }
        } finally {
          NioDirectBufferPool.release(ufsBuf);
        }
      }
    }
    return bytesRead;
  }

  private void ensureReadable(long offset, long length) {
    Preconditions.checkState(!mClosed, "reader closed");
    Preconditions.checkArgument(length >= 0, "negative read length %s", length);
    Preconditions.checkArgument(offset >= 0, "negative offset %s", offset);
    Preconditions.checkArgument(offset <= mFileSize,
        "offset (%s) exceeds block size (%s)", offset, mFileSize);
    Preconditions.checkArgument(
        offset + length >= 0 && offset + length <= mFileSize,
        "read end %s exceed block size %s", offset + length, mFileSize);
  }

  @Override
  public long getLength() {
    return mFileSize;
  }

  @Override
  public ReadableByteChannel getChannel() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    if (mFileSize <= mPos) {
      return -1;
    }
    int bytesToTransfer =
        (int) Math.min(buf.writableBytes(), mFileSize - mPos);
    ensureReadable(mPos, bytesToTransfer);
    long bytesRead = read(buf, mPos, bytesToTransfer);
    mPos += bytesRead;
    return (int) bytesRead;
  }

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      super.close();
      mClosed = true;
    }
  }

  @Override
  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public String getLocation() {
    return null;
  }
}
