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

import alluxio.CloseableSupplier;
import alluxio.PositionReader;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.LocalCachePositionReader;
import alluxio.conf.AlluxioConfiguration;
import alluxio.file.FileId;
import alluxio.file.NettyBufTargetBuffer;
import alluxio.file.ReadTargetBuffer;
import alluxio.worker.block.io.BlockReadableChannel;
import alluxio.worker.block.io.BlockReader;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * Paged file reader.
 */
public class PagedFileReader extends BlockReader implements PositionReader {
  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);
  private final long mFileSize;
  private final LocalCachePositionReader mPositionReader;
  private long mPos;
  private boolean mClosed = false;

  /**
   * Constructor.
   * @param conf
   * @param cacheManager
   * @param fallbackReader
   * @param fileId
   * @param fileSize
   * @param startPosition
   */
  public PagedFileReader(AlluxioConfiguration conf, CacheManager cacheManager,
      CloseableSupplier<PositionReader> fallbackReader, FileId fileId,
      long fileSize, long startPosition) {
    Preconditions.checkNotNull(cacheManager, "cacheManager");
    Preconditions.checkNotNull(fileId, "fileId");
    mFileSize = fileSize;
    mPos = startPosition;
    mPositionReader = LocalCachePositionReader.create(conf, cacheManager,
        fallbackReader, fileId, mFileSize);
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
    ReadTargetBuffer targetBuffer = new NettyBufTargetBuffer(buf);
    int bytesRead = mPositionReader.read(offset, targetBuffer, (int) length);
    if (bytesRead < 0) {
      return EMPTY_BYTE_BUFFER;
    }
    buffer.position(0);
    buffer.limit(bytesRead);
    mPos += bytesRead;
    return buffer;
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
    return new BlockReadableChannel(this);
  }

  @Override
  public int transferTo(ByteBuf buf) throws IOException {
    if (mFileSize <= mPos) {
      return -1;
    }
    int bytesToTransfer =
        (int) Math.min(buf.writableBytes(), mFileSize - mPos);
    ReadTargetBuffer targetBuffer = new NettyBufTargetBuffer(buf);
    int bytesRead = mPositionReader.read(mPos, targetBuffer, bytesToTransfer);
    if (bytesRead > 0) {
      mPos += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public int readInternal(long position, ReadTargetBuffer buffer, int length)
      throws IOException {
    return mPositionReader.read(position, buffer, length);
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
