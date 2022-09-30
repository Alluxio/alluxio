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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.network.protocol.databuffer.NioDirectBufferPool;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.OpenOptions;
import alluxio.util.IdUtils;
import alluxio.worker.block.UfsInputStreamCache;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.meta.BlockMeta;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;

/**
 * Block reader that reads from UFS.
 */
public class PagedUfsBlockReader extends BlockReader {
  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);
  private final long mPageSize;
  private final UfsManager mUfsManager;
  private final UfsInputStreamCache mUfsInStreamCache;
  private final BlockMeta mBlockMeta;
  private final UfsBlockReadOptions mUfsBlockOptions;
  private final long mInitialOffset;
  private final ByteBuffer mLastPage;
  private long mLastPageIndex = -1;
  private boolean mClosed = false;
  private long mPosition;

  /**
   * @param ufsManager
   * @param ufsInStreamCache
   * @param conf
   * @param blockMeta
   * @param offset
   * @param ufsBlockReadOptions
   */
  public PagedUfsBlockReader(UfsManager ufsManager,
      UfsInputStreamCache ufsInStreamCache, AlluxioConfiguration conf, BlockMeta blockMeta,
      long offset, UfsBlockReadOptions ufsBlockReadOptions) {
    Preconditions.checkArgument(offset >= 0 && offset <= blockMeta.getBlockSize(),
        "Attempt to read block %s which is %s bytes long at invalid byte offset %s",
        blockMeta.getBlockId(), blockMeta.getBlockSize(), offset);
    mUfsManager = ufsManager;
    mUfsInStreamCache = ufsInStreamCache;
    mBlockMeta = blockMeta;
    mUfsBlockOptions = ufsBlockReadOptions;
    mPageSize = conf.getBytes(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE);
    mInitialOffset = offset;
    mLastPage = ByteBuffer.allocateDirect((int) mPageSize);
    mPosition = offset;
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    Preconditions.checkState(!mClosed);
    Preconditions.checkArgument(length >= 0, "length should be non-negative");
    Preconditions.checkArgument(offset >= 0, "offset should be non-negative");
    Preconditions.checkArgument(offset + length > 0 && offset + length < mBlockMeta.getBlockSize(),
        "offset=%s, length=%s, exceeds block size=%s", offset, length, mBlockMeta.getBlockSize());

    if (length == 0 || offset >= mBlockMeta.getBlockSize()) {
      return EMPTY_BYTE_BUFFER;
    }

    // todo(bowen): this pooled buffer will likely not get released, so will still be GCed instead
    //  of reused.
    ByteBuffer buffer = NioDirectBufferPool.acquire((int) length);
    int totalBytesRead = fillWithCachedPage(buffer, offset, length);
    offset += totalBytesRead;
    try (ReadableByteChannel channel = getChannel(offset)) {
      while (totalBytesRead < length) {
        int bytesRead = channel.read(buffer);
        if (bytesRead < 0) {
          throw new IOException(String.format(
              "Unexpected EOF when reading %d bytes from offset %d of block %d",
              length, offset, mBlockMeta.getBlockId()));
        }
        totalBytesRead += bytesRead;
      }
    }
    return buffer;
  }

  /**
   * Reads a page from the UFS block at index {@code pageIndex}. This method will try to read as
   * many bytes as the page size designated by {@link PropertyKey#USER_CLIENT_CACHE_PAGE_SIZE},
   * and append to {@code buffer}. If {@code pageIndex} points to the last page of the block,
   * the size of the data read can be smaller than the page size.
   *
   * @param buffer writable output buffer, must have enough remaining space for a page
   * @param pageIndex the index of the page within the block
   * @return number of bytes read, or -1 if end of block is reached
   */
  public int readPageAtIndex(ByteBuffer buffer, long pageIndex) throws IOException {
    Preconditions.checkState(!mClosed);
    Preconditions.checkArgument(!buffer.isReadOnly(), "read-only buffer");
    Preconditions.checkArgument(buffer.remaining() >= mPageSize,
        "%s bytes available in buffer, not enough for a page of size %s",
        buffer.remaining(), mPageSize);
    Preconditions.checkArgument(pageIndex >= 0 && pageIndex * mPageSize < mBlockMeta.getBlockSize(),
        "page index (%s) is out of bound", pageIndex);

    if (pageIndex == mLastPageIndex) {
      return fillWithCachedPage(buffer, pageIndex * mPageSize, mLastPage.remaining());
    }
    int totalBytesRead = 0;
    mLastPage.clear();
    mLastPageIndex = -1;
    try (ReadableByteChannel channel = getChannel(pageIndex * mPageSize)) {
      while (totalBytesRead < mPageSize) {
        int bytesRead = channel.read(mLastPage);
        if (bytesRead < 0) {
          // reached eof
          if (totalBytesRead == 0) {
            // not a single byte has been read; report this to caller
            return bytesRead;
          }
          break;
        }
        totalBytesRead += bytesRead;
      }
    }
    mLastPage.flip();
    mLastPageIndex = pageIndex;
    fillWithCachedPage(buffer, pageIndex * mPageSize, totalBytesRead);
    return totalBytesRead;
  }

  /**
   * Fills the output buffer with the content from the cached paged.
   * @param outBuffer output buffer
   * @param offset offset with the block
   * @param length how many bytes to read
   * @return how many bytes was filled in the output buffer, 0 when the cached page does not
   *         content of the requested range
   */
  private int fillWithCachedPage(ByteBuffer outBuffer, long offset, long length) {
    long pageIndex = offset / mPageSize;
    if (pageIndex != mLastPageIndex) {
      return 0;
    }
    int pageSize = Math.min(mLastPage.remaining(), (int) length);
    ByteBuffer slice = outBuffer.slice();
    slice.limit(pageSize);
    slice.put(mLastPage);
    mLastPage.rewind();
    outBuffer.position(outBuffer.position() + pageSize);
    return pageSize;
  }

  @Override
  public long getLength() {
    return mBlockMeta.getBlockSize();
  }

  @Override
  public ReadableByteChannel getChannel() {
    return getChannel(mInitialOffset);
  }

  /**
   * @param offset offset within the block
   * @return readable channel
   */
  public ReadableByteChannel getChannel(long offset) {
    Preconditions.checkState(!mClosed);
    return new UfsReadableChannel(offset);
  }

  /**
   * @return ufs block read options which this read was created with
   */
  public UfsBlockReadOptions getUfsReadOptions() {
    return mUfsBlockOptions;
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    Preconditions.checkState(!mClosed);
    // todo(bowen): eliminate copy
    ByteBuffer buffer = NioDirectBufferPool.acquire(buf.writableBytes());
    int bytesRead = transferTo(buffer);
    buffer.flip();
    buf.writeBytes(buffer);
    NioDirectBufferPool.release(buffer);
    return bytesRead;
  }

  int transferTo(ByteBuffer byteBuffer) throws IOException {
    Preconditions.checkState(!mClosed);
    int bytesRead = fillWithCachedPage(byteBuffer, mPosition, byteBuffer.remaining());
    mPosition += bytesRead;
    try (ReadableByteChannel channel = getChannel(mPosition)) {
      bytesRead = channel.read(byteBuffer);
      if (bytesRead < 0) { // eof
        return bytesRead;
      }
      mPosition += bytesRead;
      return bytesRead;
    }
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
    if (mClosed) {
      return;
    }
    mClosed = true;
  }

  private class UfsReadableChannel implements ReadableByteChannel {
    private final long mOffset;
    private volatile InputStream mUfsInStream;
    private volatile ReadableByteChannel mUfsChannel;
    private volatile boolean mClosed = false;

    UfsReadableChannel(long offset) {
      mOffset = offset;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      if (mClosed) {
        throw new ClosedChannelException();
      }
      if (mUfsInStream == null) {
        synchronized (this) {
          if (mUfsInStream == null) {
            UfsManager.UfsClient ufsClient = mUfsManager.get(mUfsBlockOptions.getMountId());
            try (CloseableResource<UnderFileSystem> ufsResource = ufsClient.acquireUfsResource()) {
              mUfsInStream = mUfsInStreamCache.acquire(
                  ufsResource.get(),
                  mUfsBlockOptions.getUfsPath(),
                  IdUtils.fileIdFromBlockId(mBlockMeta.getBlockId()),
                  OpenOptions.defaults()
                      .setOffset(mUfsBlockOptions.getOffsetInFile() + mOffset)
                      .setPositionShort(true));
              mUfsChannel = Channels.newChannel(mUfsInStream);
            }
          }
        }
      }
      return mUfsChannel.read(dst);
    }

    @Override
    public boolean isOpen() {
      return !mClosed;
    }

    @Override
    public void close() throws IOException {
      if (mClosed) {
        return;
      }
      synchronized (this) {
        if (mClosed) {
          return;
        }
        if (mUfsInStream != null) {
          // do not close mChannel as it will close the underlying stream transitively
          mUfsInStreamCache.release(mUfsInStream);
          mUfsInStream = null;
          mUfsChannel = null;
        }
        mClosed = true;
      }
    }
  }
}
