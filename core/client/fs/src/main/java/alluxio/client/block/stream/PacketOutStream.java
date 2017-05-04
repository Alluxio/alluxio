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
import alluxio.client.BoundedStream;
import alluxio.client.QuietlyCancelable;
import alluxio.client.block.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides an {@link OutputStream} implementation that is based on {@link PacketWriter} which
 * streams data packet by packet.
 */
@NotThreadSafe
public class PacketOutStream extends OutputStream implements BoundedStream, QuietlyCancelable {
  private final Closer mCloser;
  /** Length of the stream. If unknown, set to Long.MAX_VALUE. */
  private final long mLength;
  private ByteBuf mCurrentPacket = null;

  private final List<PacketWriter> mPacketWriters;
  private boolean mClosed;

  /**
   * Creates a {@link PacketOutStream} that writes to a local file.
   *
   * @param client the block worker client
   * @param id the ID
   * @param length the block or file length
   * @param options the out stream options
   * @return the {@link PacketOutStream} created
   */
  public static PacketOutStream createLocalPacketOutStream(BlockWorkerClient client,
      long id, long length, OutStreamOptions options) {
    long packetSize = Configuration.getBytes(PropertyKey.USER_LOCAL_WRITER_PACKET_SIZE_BYTES);
    PacketWriter packetWriter =
        LocalFilePacketWriter.create(client, id, options.getWriteTier(), packetSize);
    return new PacketOutStream(packetWriter, length);
  }

  /**
   * Creates a {@link PacketOutStream} that writes to a netty data server.
   *
   * @param context the file system context
   * @param address the netty data server address
   * @param sessionId the session ID
   * @param id the ID (block ID or UFS file ID)
   * @param length the block or file length
   * @param type the request type (either block write or UFS file write)
   * @param options the out stream options
   * @return the {@link PacketOutStream} created
   */
  public static PacketOutStream createNettyPacketOutStream(FileSystemContext context,
      WorkerNetAddress address, long sessionId, long id, long length,
      Protocol.RequestType type, OutStreamOptions options) {
    long packetSize =
        Configuration.getBytes(PropertyKey.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES);
    PacketWriter packetWriter = new NettyPacketWriter(
        context, address, id, length, sessionId, options.getWriteTier(), type, packetSize);
    return new PacketOutStream(packetWriter, length);
  }

  /**
   * Creates a {@link PacketOutStream} that writes to a netty data server.
   *
   * @param context the file system context
   * @param address the netty data server address
   * @param length the block or file length
   * @param partialRequest details of the write request which are constant for all requests
   * @param options the out stream options
   * @return the {@link PacketOutStream} created
   */
  public static PacketOutStream createNettyPacketOutStream(FileSystemContext context,
      WorkerNetAddress address, long length, Protocol.WriteRequest partialRequest,
      OutStreamOptions options) {
    long packetSize =
        Configuration.getBytes(PropertyKey.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES);
    PacketWriter packetWriter =
        new NettyPacketWriter(context, address, length, partialRequest, packetSize);
    return new PacketOutStream(packetWriter, length);
  }

  /**
   * Constructs a new {@link PacketOutStream} with only one {@link PacketWriter}.
   *
   * @param packetWriter the packet writer
   * @param length the length of the stream
   */
  protected PacketOutStream(PacketWriter packetWriter, long length) {
    mCloser = Closer.create();
    mLength = length;
    mPacketWriters = new ArrayList<>(1);
    mPacketWriters.add(packetWriter);
    mCloser.register(packetWriter);
    mClosed = false;
  }

  /**
   * @return the remaining size of the block
   */
  @Override
  public long remaining() {
    long pos = Long.MAX_VALUE;
    for (PacketWriter packetWriter : mPacketWriters) {
      pos = Math.min(pos, packetWriter.pos());
    }
    return mLength - pos - (mCurrentPacket != null ? mCurrentPacket.readableBytes() : 0);
  }

  @Override
  public void write(int b) {
    Preconditions.checkState(remaining() > 0, PreconditionMessage.ERR_END_OF_BLOCK);
    updateCurrentPacket(false);
    mCurrentPacket.writeByte(b);
  }

  @Override
  public void write(byte[] b) {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    if (len == 0) {
      return;
    }

    while (len > 0) {
      updateCurrentPacket(false);
      int toWrite = Math.min(len, mCurrentPacket.writableBytes());
      mCurrentPacket.writeBytes(b, off, toWrite);
      off += toWrite;
      len -= toWrite;
    }
    updateCurrentPacket(false);
  }

  @Override
  public void flush() {
    if (mClosed) {
      return;
    }
    updateCurrentPacket(true);
    for (PacketWriter packetWriter : mPacketWriters) {
      packetWriter.flush();
    }

    // Release the channel used in the packet writer early. This is required to avoid holding the
    // netty channel unnecessarily because the block out streams are closed after all the blocks
    // are written.
    if (remaining() == 0) {
      close();
    }
  }

  @Override
  public void cancel() {
    if (mClosed) {
      return;
    }
    releaseCurrentPacket();

    Exception exception = null;
    for (PacketWriter packetWriter : mPacketWriters) {
      try {
        packetWriter.cancel();
      } catch (Exception e) {
        exception = e;
      }
    }
    if (exception != null) {
      throw AlluxioStatusException.from(exception);
    }

    // NOTE: PacketOutStream#cancel doesn't imply PacketOutStream#close. PacketOutStream#close
    // must be closed for every PacketOutStream instance.
  }

  @Override
  public void close() {
    try {
      updateCurrentPacket(true);
    } finally {
      mClosed = true;
      CommonUtils.close(mCloser);
    }
  }

  /**
   * Updates the current packet.
   *
   * @param lastPacket if the current packet is the last packet
   */
  private void updateCurrentPacket(boolean lastPacket) {
    // Early return for the most common case.
    if (mCurrentPacket != null && mCurrentPacket.writableBytes() > 0 && !lastPacket) {
      return;
    }

    if (mCurrentPacket == null) {
      if (!lastPacket) {
        mCurrentPacket = allocateBuffer();
      }
      return;
    }

    if (mCurrentPacket.writableBytes() == 0 || lastPacket) {
      try {
        if (mCurrentPacket.readableBytes() > 0) {
          for (PacketWriter packetWriter : mPacketWriters) {
            mCurrentPacket.retain();
            packetWriter.writePacket(mCurrentPacket.duplicate());
          }
        } else {
          Preconditions.checkState(lastPacket);
        }
      } finally {
        // If the packet has bytes to read, we increment its refcount explicitly for every packet
        // writer. So we need to release here. If the packet has no bytes to read, then it has
        // to be the last packet. It needs to be released as well.
        mCurrentPacket.release();
      }
      mCurrentPacket = null;
    }
    if (!lastPacket) {
      mCurrentPacket = allocateBuffer();
    }
  }

  /**
   * Releases the current packet.
   */
  private void releaseCurrentPacket() {
    if (mCurrentPacket != null) {
      mCurrentPacket.release();
      mCurrentPacket = null;
    }
  }

  /**
   * @return a newly allocated byte buffer of the user defined default size
   */
  private ByteBuf allocateBuffer() {
    return PooledByteBufAllocator.DEFAULT.buffer(mPacketWriters.get(0).packetSize());
  }
}
