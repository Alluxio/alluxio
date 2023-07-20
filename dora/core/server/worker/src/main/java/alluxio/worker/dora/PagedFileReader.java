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
import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.LocalCachePositionReader;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.file.FileId;
import alluxio.file.NettyBufTargetBuffer;
import alluxio.file.ReadTargetBuffer;
import alluxio.network.protocol.databuffer.CompositeDataBuffer;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.network.protocol.databuffer.NettyDataBuffer;
import alluxio.resource.CloseableResource;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.worker.block.io.BlockReadableChannel;
import alluxio.worker.block.io.BlockReader;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Paged file reader.
 */
public class PagedFileReader extends BlockReader implements PositionReader {
  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);
  private final long mFileSize;
  private final LocalCachePositionReader mPositionReader;
  private final CloseableResource<UnderFileSystem> mUfs;
  private long mPos;
  private volatile boolean mClosed = false;

  /**
   * Creates a new {@link PagedFileReader}.
   *
   * @param conf
   * @param cacheManager
   * @param ufsClient
   * @param fileId
   * @param ufsPath
   * @param fileSize
   * @param startPosition
   * @return a new {@link PagedFileReader}
   */
  public static PagedFileReader create(AlluxioConfiguration conf, CacheManager cacheManager,
                                       UfsManager.UfsClient ufsClient, String fileId,
                                       String ufsPath, long fileSize, long startPosition) {
    FileId fileIdField = FileId.of(fileId);
    CloseableResource<UnderFileSystem> ufs = ufsClient.acquireUfsResource();
    try {
      return new PagedFileReader(ufs, LocalCachePositionReader.create(cacheManager,
          new CloseableSupplier<>(() -> ufs.get().openPositionRead(ufsPath, fileSize)),
          fileIdField, fileSize, conf.getBytes(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE),
          CacheContext.defaults()), fileSize, startPosition);
    } catch (Throwable t) {
      try {
        ufs.close();
      } catch (Throwable t1) {
        t.addSuppressed(t1);
      }
      throw t;
    }
  }

  /**
   * Constructor.
   *
   * @param ufs
   * @param localCachePositionReader
   * @param fileSize
   * @param startPosition
   */
  public PagedFileReader(CloseableResource<UnderFileSystem> ufs,
                         LocalCachePositionReader localCachePositionReader,
                         long fileSize, long startPosition) {
    mUfs = Preconditions.checkNotNull(ufs);
    mPositionReader = Preconditions.checkNotNull(localCachePositionReader);
    mFileSize = fileSize;
    mPos = startPosition;
  }

  /**
   * Get a {@link CompositeDataBuffer} which has a list of {@link DataFileChannel}.
   *
   * @param channel the Channel object which is used for allocating ByteBuf
   * @param length the bytes to read
   * @return {@link CompositeDataBuffer}
   */
  public CompositeDataBuffer getMultipleDataFileChannel(Channel channel, long length)
      throws IOException {
    if (mFileSize <= mPos) {
      // TODO(JiamingMai): consider throwing exception directly
      return null;
    }
    List<DataBuffer> dataBufferList = new ArrayList<>();
    long bytesToTransfer = Math.min(length, mFileSize - mPos);
    long bytesToTransferLeft = bytesToTransfer;
    while (bytesToTransferLeft > 0) {
      long lengthPerOp = Math.min(bytesToTransferLeft, mPositionReader.getPageSize());
      DataBuffer dataBuffer;
      Optional<DataFileChannel> dataFileChannel =
          mPositionReader.getDataFileChannel(mPos, (int) lengthPerOp);
      if (!dataFileChannel.isPresent()) {
        dataBuffer = getDataBufferByCopying(channel, (int) lengthPerOp);
      } else {
        // update mPos
        // TODO(JiamingMai): need to lock page files since the openFile op is called in netty latter
        dataBuffer = dataFileChannel.get();
        if (dataBuffer.getLength() > 0) {
          mPos += dataBuffer.getLength();
        }
      }
      // update bytesToTransferLeft
      bytesToTransferLeft -= dataBuffer.getLength();
      dataBufferList.add(dataBuffer);
    }
    CompositeDataBuffer compositeDataBuffer = new CompositeDataBuffer(dataBufferList);
    return compositeDataBuffer;
  }

  private DataBuffer getDataBufferByCopying(Channel channel, int len) throws IOException {
    ByteBuf buf = channel.alloc().buffer(len, len);
    try {
      while (buf.writableBytes() > 0 && transferTo(buf) != -1) {
      }
      return new NettyDataBuffer(buf);
    } catch (Throwable e) {
      buf.release();
      throw e;
    }
  }

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
  public synchronized void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mPositionReader.close();
    mUfs.close();
    super.close();
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
