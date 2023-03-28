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

package alluxio.underfs;

import alluxio.conf.AlluxioConfiguration;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.databuffer.NioDirectBufferPool;
import alluxio.resource.CloseableResource;
import alluxio.underfs.options.OpenOptions;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.page.UfsBlockReadOptions;

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
public class PagedUfsReader extends BlockReader {
  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);
  private final long mPageSize;
  private final UfsManager.UfsClient mUfsClient;
  private final UfsInputStreamCache mUfsInStreamCache;
  private final FileId mFileId;
  private final long mFileSize;
  private final UfsBlockReadOptions mUfsBlockOptions;
  private final long mInitialOffset;
  private boolean mClosed = false;
  private long mPosition;

  /**
   * @param conf
   * @param ufsClient
   * @param ufsInStreamCache
   * @param fileId
   * @param fileSize
   * @param offset
   * @param ufsBlockReadOptions
   * @param pageSize
   */
  public PagedUfsReader(AlluxioConfiguration conf, UfsManager.UfsClient ufsClient,
      UfsInputStreamCache ufsInStreamCache, FileId fileId, long fileSize,
      long offset, UfsBlockReadOptions ufsBlockReadOptions, long pageSize) {
    Preconditions.checkArgument(offset >= 0 && offset <= fileSize,
        "Attempt to read file %s which is %s bytes long at invalid byte offset %s",
        fileId, fileSize, offset);
    mUfsClient = ufsClient;
    mUfsInStreamCache = ufsInStreamCache;
    mFileId = fileId;
    mFileSize = fileSize;
    mUfsBlockOptions = ufsBlockReadOptions;
    mPageSize = pageSize;
    mInitialOffset = offset;
    mPosition = offset;
  }

  @Override
  public ByteBuffer read(long offset, long length) throws IOException {
    Preconditions.checkState(!mClosed);
    Preconditions.checkArgument(length >= 0, "length should be non-negative");
    Preconditions.checkArgument(offset >= 0, "offset should be non-negative");

    if (length == 0 || offset >= mFileSize) {
      return EMPTY_BYTE_BUFFER;
    }

    length = Math.min(length, mFileSize - offset);
    // todo(bowen): this pooled buffer will likely not get released, so will still be GCed instead
    //  of reused.
    ByteBuffer buffer = NioDirectBufferPool.acquire((int) length);
    int totalBytesRead = 0;
    try (ReadableByteChannel channel = getChannel(offset)) {
      while (totalBytesRead < length) {
        int bytesRead = channel.read(buffer);
        if (bytesRead < 0) {
          throw new IOException(String.format(
              "Unexpected EOF when reading %d bytes from offset %d of block %d",
              length, offset, mFileId));
        }
        totalBytesRead += bytesRead;
      }
    }
    buffer.flip();
    return buffer;
  }

  /**
   * Reads a page from the UFS block at index {@code pageIndex}.
   *
   * @param pageSize the target page size
   * @param pageIndex the index of the page within the block
   * @return the page read
   */
  public byte[] readPageAtIndex(int pageSize, long pageIndex) throws IOException {
    Preconditions.checkState(!mClosed);
    Preconditions.checkArgument(pageIndex >= 0 && pageIndex * mPageSize < mFileSize,
        "page index (%s) is out of bound", pageIndex);
    int totalBytesRead = 0;
    byte[] page = new byte[pageSize];
    ByteBuffer pageBuffer = ByteBuffer.wrap(page);
    try (ReadableByteChannel channel = getChannel(pageIndex * mPageSize)) {
      while (totalBytesRead < pageSize) {
        int bytesRead = channel.read(pageBuffer);
        if (bytesRead <= 0) {
          break;
        }
        totalBytesRead += bytesRead;
      }
    }
    // Bytes read from external, may be larger than requests due to reading complete pages
    MetricsSystem.meter(MetricKey.CLIENT_CACHE_BYTES_READ_EXTERNAL.getName())
        .mark(totalBytesRead);
    if (totalBytesRead != pageSize) {
      throw new IOException("Failed to read complete page from external storage. Bytes read: "
          + totalBytesRead + " Page size: " + pageSize);
    }
    return page;
  }

  @Override
  public long getLength() {
    return mFileSize;
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
    int bytesRead;
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
    return mUfsBlockOptions.getUfsPath();
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
      String ufsPath = mUfsBlockOptions.getUfsPath();
      if (mUfsInStream == null) {
        synchronized (this) {
          if (mUfsInStream == null) {
            try (CloseableResource<UnderFileSystem> ufsResource = mUfsClient.acquireUfsResource()) {
              mUfsInStream = mUfsInStreamCache.acquire(
                  ufsResource.get(),
                  ufsPath,
                  mFileId,
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
          // todo(bowen): cannot release the stream if the channel is being concurrently read from.
          //  needs to interrupt the reader before releasing
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
