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

package alluxio.client.block.stream;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.Seekable;
import alluxio.client.BoundedStream;
import alluxio.client.Locatable;
import alluxio.client.PositionedReadable;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.NotFoundException;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.io.BufferUtils;
import alluxio.util.network.NettyUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides an {@link InputStream} implementation that is based on {@link PacketReader}s to
 * stream data packet by packet.
 */
@NotThreadSafe
public class BlockInStream extends InputStream implements BoundedStream, Seekable,
    PositionedReadable, Locatable {
  /** The id of the block or UFS file to which this instream provides access. */
  private final long mId;
  /** The size in bytes of the block. */
  private final long mLength;

  private final byte[] mSingleByte = new byte[1];
  private final boolean mLocal;
  private final WorkerNetAddress mAddress;

  /** Current position of the stream, relative to the start of the block. */
  private long mPos = 0;
  /** The current packet. */
  private DataBuffer mCurrentPacket;

  private PacketReader mPacketReader;
  private PacketReader.Factory mPacketReaderFactory;

  private boolean mClosed = false;
  private boolean mEOF = false;

  /**
   * Creates an {@link BlockInStream} that reads from a local block.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param address the Alluxio worker address
   * @param openUfsBlockOptions the options to open a UFS block, set to null if this is block is
   *        not persisted in UFS
   * @param options the in stream options
   * @return the {@link InputStream} object
   */
  public static BlockInStream create(FileSystemContext context, long blockId, long blockSize,
      WorkerNetAddress address, Protocol.OpenUfsBlockOptions openUfsBlockOptions,
      InStreamOptions options) throws IOException {
    if (CommonUtils.isLocalHost(address) && Configuration
        .getBoolean(PropertyKey.USER_SHORT_CIRCUIT_ENABLED) && !NettyUtils
        .isDomainSocketSupported(address)) {
      try {
        return createLocalBlockInStream(context, address, blockId, blockSize, options);
      } catch (NotFoundException e) {
        // Failed to do short circuit read because the block is not available in Alluxio.
        // We will try to read from UFS via netty. So this exception is ignored.
      }
    }
    Protocol.ReadRequest.Builder builder = Protocol.ReadRequest.newBuilder().setBlockId(blockId)
        .setPromote(options.getAlluxioStorageType().isPromote());
    if (openUfsBlockOptions != null) {
      builder.setOpenUfsBlockOptions(openUfsBlockOptions);
    }

    return createNettyBlockInStream(context, address, builder.buildPartial(), blockSize, options);
  }

  /**
   * Creates a {@link BlockInStream} to read from a local file.
   *
   * @param context the file system context
   * @param address the network address of the netty data server
   * @param blockId the block ID
   * @param length the block length
   * @param options the in stream options
   * @return the {@link BlockInStream} created
   */
  private static BlockInStream createLocalBlockInStream(FileSystemContext context,
      WorkerNetAddress address, long blockId, long length, InStreamOptions options)
      throws IOException {
    long packetSize = Configuration.getBytes(PropertyKey.USER_LOCAL_READER_PACKET_SIZE_BYTES);
    return new BlockInStream(
        new LocalFilePacketReader.Factory(context, address, blockId, packetSize, options), address,
        blockId, length);
  }

  /**
   * Creates a {@link BlockInStream} to read from a netty data server.
   *
   * @param context the file system context
   * @param address the address of the netty data server
   * @param blockSize the block size
   * @param readRequestPartial the partial read request
   * @param options the in stream options
   * @return the {@link BlockInStream} created
   */
  private static BlockInStream createNettyBlockInStream(FileSystemContext context,
      WorkerNetAddress address, Protocol.ReadRequest readRequestPartial, long blockSize,
      InStreamOptions options) {
    long packetSize =
        Configuration.getBytes(PropertyKey.USER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES);
    PacketReader.Factory factory = new NettyPacketReader.Factory(context, address,
        readRequestPartial.toBuilder().setPacketSize(packetSize).buildPartial(), options);
    return new BlockInStream(factory, address, readRequestPartial.getBlockId(), blockSize);
  }

  /**
   * Creates an instance of {@link BlockInStream}.
   *
   * @param packetReaderFactory the packet reader factory
   * @param address the worker network address
   * @param id the ID (either block ID or UFS file ID)
   * @param length the length
   */
  protected BlockInStream(PacketReader.Factory packetReaderFactory, WorkerNetAddress address,
      long id, long length) {
    mPacketReaderFactory = packetReaderFactory;
    mId = id;
    mLength = length;
    mAddress = address;
    mLocal = CommonUtils.isLocalHost(mAddress);
  }

  @Override
  public long getPos() {
    return mPos;
  }

  @Override
  public int read() throws IOException {
    int bytesRead = read(mSingleByte);
    if (bytesRead == -1) {
      return -1;
    }
    Preconditions.checkState(bytesRead == 1);
    return BufferUtils.byteToInt(mSingleByte[0]);
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkIfClosed();
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_READ_BUFFER_NULL);
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    if (len == 0) {
      return 0;
    }

    readPacket();
    if (mCurrentPacket == null) {
      mEOF = true;
    }
    if (mEOF) {
      closePacketReader();
      return -1;
    }
    int toRead = Math.min(len, mCurrentPacket.readableBytes());
    mCurrentPacket.readBytes(b, off, toRead);
    mPos += toRead;
    return toRead;
  }

  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    if (pos < 0 || pos >= mLength) {
      return -1;
    }

    int lenCopy = len;
    try (PacketReader reader = mPacketReaderFactory.create(pos, len)) {
      // We try to read len bytes instead of returning after reading one packet because
      // it is not free to create/close a PacketReader.
      while (len > 0) {
        DataBuffer dataBuffer = null;
        try {
          dataBuffer = reader.readPacket();
          if (dataBuffer == null) {
            break;
          }
          Preconditions.checkState(dataBuffer.readableBytes() <= len);
          int toRead = dataBuffer.readableBytes();
          dataBuffer.readBytes(b, off, toRead);
          len -= toRead;
          off += toRead;
        } finally {
          if (dataBuffer != null) {
            dataBuffer.release();
          }
        }
      }
    }
    if (lenCopy == len) {
      return -1;
    }
    return lenCopy - len;
  }

  @Override
  public long remaining() {
    return mEOF ? 0 : mLength - mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    checkIfClosed();
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions
        .checkArgument(pos <= mLength, PreconditionMessage.ERR_SEEK_PAST_END_OF_REGION.toString(),
            mId);
    if (pos == mPos) {
      return;
    }
    if (pos < mPos) {
      mEOF = false;
    }

    closePacketReader();
    mPos = pos;
  }

  @Override
  public long skip(long n) throws IOException {
    checkIfClosed();
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(remaining(), n);
    mPos += toSkip;

    closePacketReader();
    return toSkip;
  }

  @Override
  public void close() throws IOException {
    try {
      closePacketReader();
    } finally {
      mPacketReaderFactory.close();
    }
    mClosed = true;
  }

  /**
   * @return whether the packet in stream is reading packets directly from a local file
   */
  public boolean isShortCircuit() {
    return mPacketReaderFactory.isShortCircuit();
  }

  /**
   * Reads a new packet from the channel if all of the current packet is read.
   */
  private void readPacket() throws IOException {
    if (mPacketReader == null) {
      mPacketReader = mPacketReaderFactory.create(mPos, mLength - mPos);
    }

    if (mCurrentPacket != null && mCurrentPacket.readableBytes() == 0) {
      mCurrentPacket.release();
      mCurrentPacket = null;
    }
    if (mCurrentPacket == null) {
      mCurrentPacket = mPacketReader.readPacket();
    }
  }

  /**
   * Close the current packet reader.
   */
  private void closePacketReader() throws IOException {
    if (mCurrentPacket != null) {
      mCurrentPacket.release();
      mCurrentPacket = null;
    }
    if (mPacketReader != null) {
      mPacketReader.close();
    }
    mPacketReader = null;
  }

  /**
   * Convenience method to ensure the stream is not closed.
   */
  private void checkIfClosed() {
    Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
  }

  @Override
  public WorkerNetAddress location() {
    return mAddress;
  }

  @Override
  public boolean isLocal() {
    return mLocal;
  }
}
