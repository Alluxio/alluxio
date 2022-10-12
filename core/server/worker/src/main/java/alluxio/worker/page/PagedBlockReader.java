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
import alluxio.client.file.cache.store.ByteBufferTargetBuffer;
import alluxio.client.file.cache.store.PageReadTargetBuffer;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.grpc.ErrorType;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.NioDirectBufferPool;
import alluxio.worker.block.io.BlockReader;

import com.google.common.base.Preconditions;
import io.grpc.Status;
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
  private final boolean mIsBufferPooled;

  /**
   * Constructor for PagedBlockReader.
   *
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
    mIsBufferPooled = conf.getBoolean(PropertyKey.WORKER_NETWORK_READER_BUFFER_POOLED);
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    Preconditions.checkState(!mClosed);
    Preconditions.checkArgument(length >= 0, "length should be non-negative");
    Preconditions.checkArgument(offset >= 0, "offset should be non-negative");

    if (length == 0 || offset >= mBlockMeta.getBlockSize()) {
      return EMPTY_BYTE_BUFFER;
    }

    length = Math.min(length, mBlockMeta.getBlockSize() - offset);
    ByteBuffer buf = getOutputBuffer((int) length);
    PageReadTargetBuffer target = new ByteBufferTargetBuffer(buf);
    long bytesRead = 0;
    while (bytesRead < length) {
      long pos = offset + bytesRead;
      long pageIndex = pos / mPageSize;
      PageId pageId = new PageId(String.valueOf(mBlockMeta.getBlockId()), pageIndex);
      int currentPageOffset = (int) (pos % mPageSize);
      int bytesLeftInPage =
          (int) Math.min(mPageSize - currentPageOffset, length - bytesRead);
      int bytesReadFromCache = mCacheManager.get(
          pageId, currentPageOffset, bytesLeftInPage, target, CacheContext.defaults());
      if (bytesReadFromCache > 0) {
        bytesRead += bytesReadFromCache;
        MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_READ_CACHE.getName()).mark(bytesRead);
        mReadFromLocalCache = true;
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
        int pageBytesRead = ufsBlockReader.readPageAtIndex(ufsBuf, pageIndex);
        if (pageBytesRead > 0) {
          ufsBuf.position(currentPageOffset);
          ufsBuf.limit(currentPageOffset + bytesLeftInPage);
          buf.put(ufsBuf);
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
      }
    }
    buf.flip();
    return buf;
  }

  private ByteBuffer getOutputBuffer(int length) {
    if (mIsBufferPooled) {
      return NioDirectBufferPool.acquire(length);
    } else {
      return ByteBuffer.allocateDirect(length);
    }
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
