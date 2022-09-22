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

import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.PageId;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.worker.block.io.BlockReader;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

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
   * @param cacheManager paging cache manager
   * @param conf alluxio configurations
   * @param blockMeta block meta
   * @param offset initial offset within the block to begin the read from
   * @param ufsBlockReader ufs block reader
   */
  public PagedBlockReader(CacheManager cacheManager, AlluxioConfiguration conf,
      PagedBlockMeta blockMeta, long offset, Optional<PagedUfsBlockReader> ufsBlockReader) {
    Preconditions.checkArgument(offset >= 0 && offset <= blockMeta.getBlockSize(),
        "Attempt to read block %d which is %d bytes long at invalid byte offset %d",
        blockMeta.getBlockId(), blockMeta.getBlockSize(), offset);
    mCacheManager = cacheManager;
    mUfsBlockReader = ufsBlockReader;
    mBlockMeta = blockMeta;
    mPageSize = conf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE);
    mPosition = offset;
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    Preconditions.checkState(!mClosed);
    Preconditions.checkArgument(length >= 0, "length should be non-negative");
    Preconditions.checkArgument(offset >= 0, "offset should be non-negative");

    if (length == 0 || offset >= mBlockMeta.getBlockSize()) {
      return EMPTY_BYTE_BUFFER;
    }

    byte[] buf = new byte[(int) length];
    long bytesRead = 0;
    while (bytesRead < length) {
      long pos = offset + bytesRead;
      long pageIndex = pos / mPageSize;
      PageId pageId = new PageId(String.valueOf(mBlockMeta.getBlockId()), pageIndex);
      int currentPageOffset = (int) (pos % mPageSize);
      int bytesLeftInPage =
          (int) Math.min(mPageSize - currentPageOffset, length - bytesRead);
      int bytesReadFromCache = mCacheManager.get(
          pageId, currentPageOffset, bytesLeftInPage, buf, (int) bytesRead);
      if (bytesReadFromCache > 0) {
        bytesRead += bytesReadFromCache;
        MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE.getName()).mark(bytesRead);
        mReadFromLocalCache = true;
      } else {
        if (!mUfsBlockReader.isPresent()) {
          throw new IOException(String.format("Block %d cannot be read from UFS as UFS reader is "
              + "missing", mBlockMeta.getBlockId()));
        }
        PagedUfsBlockReader ufsBlockReader = mUfsBlockReader.get();
        long pageStart = pos - (pos % mPageSize);
        int pageSize = (int) Math.min(mPageSize, mBlockMeta.getBlockSize() - pageStart);
        byte[] page = new byte[(int) mPageSize];
        int pageBytesRead = ufsBlockReader.readPageAtIndex(ByteBuffer.wrap(page), pageIndex);
        if (pageBytesRead > 0) {
          System.arraycopy(page, currentPageOffset, buf, (int) bytesRead, bytesLeftInPage);
          bytesRead += bytesLeftInPage;
          MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL.getName())
              .mark(bytesLeftInPage);
          mReadFromUfs = true;
          if (ufsBlockReader.getUfsReadOptions().isCacheIntoAlluxio()) {
            byte[] pageToCache = page;
            if (pageBytesRead < mPageSize) {
              pageToCache = new byte[(int) pageBytesRead];
              System.arraycopy(page, 0, pageToCache, 0, pageBytesRead);
            }
            mCacheManager.put(pageId, pageToCache);
          }
        }
      }
    }
    return ByteBuffer.wrap(buf);
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
    Preconditions.checkState(!mClosed);
    if (mBlockMeta.getBlockSize() <= mPosition) {
      return -1;
    }
    int bytesToTransfer =
        (int) Math.min(buf.writableBytes(), mBlockMeta.getBlockSize() - mPosition);
    ByteBuffer srcBuf = read(mPosition, bytesToTransfer);
    buf.writeBytes(srcBuf);
    mPosition += bytesToTransfer;
    return bytesToTransfer;
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
}
