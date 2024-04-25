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

package alluxio.worker.page;

import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.store.PageReadTargetBuffer;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.grpc.ErrorType;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.NioDirectBufferPool;
import alluxio.worker.block.io.BlockReader;

import com.google.common.base.Preconditions;
import io.grpc.Status;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A paged implementation of BlockReader interface. The read operations will fall back to the
 * under storage when the requested data is not in the local storage.
 */
@NotThreadSafe
public class PagedBlockReader extends BlockReader {
  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);
  private final long mPageSize;
  private final CacheManager mCacheManager;
  private final Optional<PagedUfsBlockReader> mUfsBlockReader;
  private final PagedBlockMeta mBlockMeta;
  private boolean mClosed = false;
  private boolean mReadFromLocalCache = false;
  private boolean mReadFromUfs = false;
  private long mPosition;

  /**
   * Constructor for PagedBlockReader.
   *
   * @param cacheManager paging cache manager
   * @param blockMeta block meta
   * @param offset initial offset within the block to begin the read from
   * @param ufsBlockReader ufs block reader
   * @param pageSize page size
   */
  public PagedBlockReader(CacheManager cacheManager, PagedBlockMeta blockMeta, long offset,
      Optional<PagedUfsBlockReader> ufsBlockReader, long pageSize) {
    Preconditions.checkArgument(offset >= 0 && offset <= blockMeta.getBlockSize(),
        "Attempt to read block %d which is %d bytes long at invalid byte offset %d",
        blockMeta.getBlockId(), blockMeta.getBlockSize(), offset);
    mCacheManager = cacheManager;
    mUfsBlockReader = ufsBlockReader;
    mBlockMeta = blockMeta;
    mPageSize = pageSize;
    mPosition = offset;
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    if (length == 0 || offset >= mBlockMeta.getBlockSize()) {
      return EMPTY_BYTE_BUFFER;
    }
    // cap length to the remaining of block, as the caller may pass in a longer length than what
    // is left in the block, but expect as many bytes as there is
    length = Math.min(length, mBlockMeta.getBlockSize() - offset);
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
    long bytesRead = 0;
    while (bytesRead < length) {
      long pos = offset + bytesRead;
      long pageIndex = pos / mPageSize;
      PageId pageId =
          new BlockPageId(mBlockMeta.getBlockId(), pageIndex, mBlockMeta.getBlockSize());
      int currentPageOffset = (int) (pos % mPageSize);
      int bytesLeftInPage =
          (int) Math.min(mPageSize - currentPageOffset, length - bytesRead);
      int bytesReadFromCache = mCacheManager.get(
          pageId, currentPageOffset, bytesLeftInPage, target, CacheContext.defaults());
      if (bytesReadFromCache > 0) {
        bytesRead += bytesReadFromCache;
        MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE.getName()).mark(bytesRead);
        mReadFromLocalCache = true;
        MetricsSystem.counter(MetricKey.WORKER_BYTES_READ_CACHE.getName()).inc(bytesReadFromCache);
      } else {
        if (!mUfsBlockReader.isPresent()) {
          throw new AlluxioRuntimeException(
              Status.INTERNAL,
              String.format("Block %d cannot be read from UFS as UFS reader is missing, "
                      + "this is most likely a bug", mBlockMeta.getBlockId()),
              null,
              ErrorType.Internal,
              false
          );
        }
        PagedUfsBlockReader ufsBlockReader = mUfsBlockReader.get();
        // get the page at pageIndex as a whole from UFS
        ByteBuffer ufsBuf = NioDirectBufferPool.acquire((int) mPageSize);
        try {
          int pageBytesRead = ufsBlockReader.readPageAtIndex(ufsBuf, pageIndex);
          if (pageBytesRead > 0) {
            ufsBuf.position(currentPageOffset);
            ufsBuf.limit(currentPageOffset + bytesLeftInPage);
            byteBuf.writeBytes(ufsBuf);
            bytesRead += bytesLeftInPage;
            MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL.getName())
                .mark(bytesLeftInPage);
            mReadFromUfs = true;
            ufsBuf.rewind();
            ufsBuf.limit(pageBytesRead);
            if (ufsBlockReader.getUfsReadOptions().isCacheIntoAlluxio()) {
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

  @Override
  public long getLength() {
    return mBlockMeta.getBlockSize();
  }

  @Override
  public ReadableByteChannel getChannel() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    if (mBlockMeta.getBlockSize() <= mPosition) {
      return -1;
    }
    int bytesToTransfer =
        (int) Math.min(buf.writableBytes(), mBlockMeta.getBlockSize() - mPosition);
    ensureReadable(mPosition, bytesToTransfer);
    long bytesRead = read(buf, mPosition, bytesToTransfer);
    mPosition += bytesRead;
    return (int) bytesRead;
  }

  @Override
  public boolean isClosed() {
    return mClosed;
  }

  @Override
  public String getLocation() {
    return mBlockMeta.getPath();
  }

  @Override
  public void close() throws IOException {
    if (!isClosed()) {
      if (mReadFromLocalCache) {
        MetricsSystem.counter(MetricKey.WORKER_BLOCKS_READ_LOCAL.getName()).inc();
      }
      if (mReadFromUfs) {
        MetricsSystem.counter(MetricKey.WORKER_BLOCKS_READ_UFS.getName()).inc();
      }
    }
    mClosed = true;
  }

  private void ensureReadable(long offset, long length) {
    Preconditions.checkState(!mClosed, "reader closed");
    Preconditions.checkArgument(length >= 0, "negative read length %s", length);
    Preconditions.checkArgument(offset >= 0, "negative offset %s", offset);
    Preconditions.checkArgument(offset <= mBlockMeta.getBlockSize(),
        "offset (%s) exceeds block size (%s)", offset, mBlockMeta.getBlockSize());
    Preconditions.checkArgument(
        offset + length >= 0 && offset + length <= mBlockMeta.getBlockSize(),
        "read end %s exceed block size %s", offset + length, mBlockMeta.getBlockSize());
  }
}
