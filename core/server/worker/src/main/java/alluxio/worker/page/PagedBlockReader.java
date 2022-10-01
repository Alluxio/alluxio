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
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.NioDirectBufferPool;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.IdUtils;
import alluxio.worker.block.UfsInputStreamCache;
import alluxio.worker.block.io.BlockReader;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
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
  private final UfsManager mUfsManager;
  private final UfsInputStreamCache mUfsInStreamCache;
  private final PagedBlockMeta mBlockMeta;
  private final Optional<UfsBlockReadOptions> mUfsBlockOptions;
  private boolean mClosed = false;
  private boolean mReadFromLocalCache = false;
  private boolean mReadFromUfs = false;
  private long mPosition;

  /**
   * Constructor for PagedBlockReader.
   *
   * @param cacheManager paging cache manager
   * @param ufsManager under file storage manager
   * @param ufsInStreamCache a cache for the in streams from ufs
   * @param conf alluxio configurations
   * @param blockMeta block meta
   * @param offset initial offset within the block to begin the read from
   * @param ufsBlockReadOptions options to open a ufs block
   */
  public PagedBlockReader(CacheManager cacheManager,
      UfsManager ufsManager, UfsInputStreamCache ufsInStreamCache, AlluxioConfiguration conf,
      PagedBlockMeta blockMeta, long offset, Optional<UfsBlockReadOptions> ufsBlockReadOptions) {
    Preconditions.checkArgument(offset >= 0 && offset <= blockMeta.getBlockSize(),
        "Attempt to read block %d which is %d bytes long at invalid byte offset %d",
        blockMeta.getBlockId(), blockMeta.getBlockSize(), offset);
    mCacheManager = cacheManager;
    mUfsManager = ufsManager;
    mUfsInStreamCache = ufsInStreamCache;
    mBlockMeta = blockMeta;
    mUfsBlockOptions = ufsBlockReadOptions;
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

    ByteBuffer buf = NioDirectBufferPool.acquire((int) length);
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
        if (!mUfsBlockOptions.isPresent()) {
          throw new IOException(String.format("Block %d does not have UFS read options, "
              + "therefore cannot be read from UFS", mBlockMeta.getBlockId()));
        }
        long pageStart = pos - (pos % mPageSize);
        int pageSize = (int) Math.min(mPageSize, mBlockMeta.getBlockSize() - pageStart);
        ByteBuffer ufsBuf = NioDirectBufferPool.acquire(pageSize);
        try {
          int bytesReadFromUFS = readPageFromUFS(pageStart, pageSize, ufsBuf);
          if (ufsBuf.remaining() > 0) {
            ufsBuf.mark();
            ufsBuf.position(currentPageOffset);
            ufsBuf.limit(currentPageOffset + bytesLeftInPage);
            buf.put(ufsBuf);
            bytesRead += bytesLeftInPage;
            MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_REQUESTED_EXTERNAL.getName())
                .mark(bytesLeftInPage);
            mReadFromUfs = true;
            ufsBuf.reset();
            ufsBuf.limit(bytesReadFromUFS);
            mCacheManager.put(pageId, ufsBuf);
          }
        } finally {
          NioDirectBufferPool.release(ufsBuf);
        }
      }
    }
    buf.flip();
    return buf;
  }

  private int readPageFromUFS(long pageStart, int pageSize, ByteBuffer target) throws IOException {
    InputStream ufsInputStream =
        seekUfsInputStream(mUfsBlockOptions.get().getOffsetInFile() + pageStart);
    int totalBytesRead = 0;
    try {
      while (totalBytesRead < pageSize) {
        int bytesRead = Channels.newChannel(ufsInputStream).read(target);
        if (bytesRead <= 0) {
          break;
        }
        totalBytesRead += bytesRead;
      }
    } finally {
      mUfsInStreamCache.release(ufsInputStream);
      target.flip();
    }
    return totalBytesRead;
  }

  private InputStream seekUfsInputStream(long posInFile)
      throws IOException {
    UfsManager.UfsClient ufsClient = mUfsManager.get(mUfsBlockOptions.get().getMountId());
    try (CloseableResource<UnderFileSystem> ufsResource =
             ufsClient.acquireUfsResource()) {
      return mUfsInStreamCache.acquire(
          ufsResource.get(),
          mUfsBlockOptions.get().getUfsPath(),
          IdUtils.fileIdFromBlockId(mBlockMeta.getBlockId()),
          OpenOptions.defaults()
              .setOffset(posInFile)
              .setPositionShort(true));
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
    throw new UnsupportedOperationException();
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
