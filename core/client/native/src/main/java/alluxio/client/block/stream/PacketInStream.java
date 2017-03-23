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

import alluxio.Seekable;
import alluxio.client.BoundedStream;
import alluxio.client.PositionedReadable;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.PreconditionMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides an {@link InputStream} implementation that is based on {@link PacketReader}s to
 * stream data packet by packet.
 */
@NotThreadSafe
public final class PacketInStream extends InputStream implements BoundedStream, Seekable,
    PositionedReadable {
  /** The id of the block or UFS file to which this instream provides access. */
  private final long mId;
  /** The size in bytes of the block. */
  private final long mLength;

  private final byte[] mSingleByte = new byte[1];

  /** Current position of the stream, relative to the start of the block. */
  private long mPos = 0;
  /** The current packet. */
  private DataBuffer mCurrentPacket;

  private PacketReader mPacketReader;
  private PacketReader.Factory mPacketReaderFactory;

  private boolean mClosed = false;
  private boolean mEOF = false;

  /**
   * Creates a {@link PacketInStream} to read from a local file.
   *
   * @param path the local file path
   * @param id the ID
   * @param length the block or file length
   * @return the {@link PacketInStream} created
   * @throws IOException if it fails to create the object
   */
  public static PacketInStream createLocalPacketInstream(String path, long id, long length)
      throws IOException {
    return new PacketInStream(new LocalFilePacketReader.Factory(path), id, length);
  }

  /**
   * Creates a {@link PacketInStream} to read from a netty data server.
   *
   * @param context the file system context
   * @param address the network address of the netty data server
   * @param id the ID
   * @param lockId the lock ID (set to -1 if not applicable)
   * @param sessionId the session ID (set to -1 if not applicable)
   * @param length the block or file length
   * @param noCache do not cache the block to the Alluxio worker if read from UFS when this is set
   * @param type the read request type (either block read or UFS file read)
   * @return the {@link PacketInStream} created
   */
  public static PacketInStream createNettyPacketInStream(FileSystemContext context,
      InetSocketAddress address, long id, long lockId, long sessionId, long length,
      boolean noCache, Protocol.RequestType type) {
    PacketReader.Factory factory =
        new NettyPacketReader.Factory(context, address, id, lockId, sessionId, noCache, type);
    return new PacketInStream(factory, id, length);
  }

  /**
   * Creates an instance of {@link PacketInStream}.
   *
   * @param packetReaderFactory the packet reader factory
   * @param id the ID (either block ID or UFS file ID)
   * @param length the length
   */
  public PacketInStream(PacketReader.Factory packetReaderFactory, long id, long length) {
    mPacketReaderFactory = packetReaderFactory;
    mId = id;
    mLength = length;
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
    closePacketReader();
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
   *
   * @throws IOException if it fails to read the packet
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
}
