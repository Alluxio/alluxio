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

import alluxio.client.BoundedStream;
import alluxio.client.Cancelable;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.PreconditionMessage;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides an {@link OutputStream} implementation that is based on {@link PacketWriter} which
 * streams data packet by packet.
 */
@NotThreadSafe
public class BlockOutStream extends OutputStream implements BoundedStream, Cancelable {
  private static final Logger LOG = LoggerFactory.getLogger(BlockOutStream.class);

  private final Closer mCloser;
  /** Length of the stream. If unknown, set to Long.MAX_VALUE. */
  private final long mLength;
  private final WorkerNetAddress mAddress;
  private ByteBuf mCurrentPacket = null;

  private final List<PacketWriter> mPacketWriters;
  private boolean mClosed;

  /**
   * Creates an {@link BlockOutStream}.
   *
   * @param context the file system context
   * @param blockId the block ID
   * @param blockSize the block size in bytes
   * @param address the Alluxio worker address
   * @param options the out stream options
   * @return the {@link OutputStream} object
   */
  public static BlockOutStream create(FileSystemContext context, long blockId, long blockSize,
      WorkerNetAddress address, OutStreamOptions options) throws IOException {
    PacketWriter packetWriter =
        PacketWriter.Factory.create(context, blockId, blockSize, address, options);
    return new BlockOutStream(packetWriter, blockSize, address);
  }

  /**
   * Constructs a new {@link BlockOutStream} with only one {@link PacketWriter}.
   *
   * @param packetWriter the packet writer
   * @param length the length of the stream
   * @param address the Alluxio worker address
   */
  protected BlockOutStream(PacketWriter packetWriter, long length, WorkerNetAddress address) {
    mCloser = Closer.create();
    mLength = length;
    mAddress = address;
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
  public void write(int b) throws IOException {
    Preconditions.checkState(remaining() > 0, PreconditionMessage.ERR_END_OF_BLOCK);
    updateCurrentPacket(false);
    mCurrentPacket.writeByte(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
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
  public void flush() throws IOException {
    if (mClosed) {
      return;
    }
    updateCurrentPacket(true);
    for (PacketWriter packetWriter : mPacketWriters) {
      packetWriter.flush();
    }
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    releaseCurrentPacket();

    IOException exception = null;
    for (PacketWriter packetWriter : mPacketWriters) {
      try {
        packetWriter.cancel();
      } catch (IOException e) {
        if (exception != null) {
          exception.addSuppressed(e);
        }
      }
    }
    if (exception != null) {
      throw exception;
    }

    close();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      updateCurrentPacket(true);
    } catch (Throwable t) {
      throw mCloser.rethrow(t);
    } finally {
      mClosed = true;
      mCloser.close();
    }
  }

  /**
   * @return the worker address for this stream
   */
  public WorkerNetAddress getAddress() {
    return mAddress;
  }

  /**
   * Updates the current packet.
   *
   * @param lastPacket if the current packet is the last packet
   */
  private void updateCurrentPacket(boolean lastPacket) throws IOException {
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
        mCurrentPacket = null;
      }
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
