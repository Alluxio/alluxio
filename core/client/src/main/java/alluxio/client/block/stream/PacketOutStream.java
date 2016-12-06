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

import alluxio.client.Cancelable;
import alluxio.exception.PreconditionMessage;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides a stream API to write a block to Alluxio. An instance of this class can be obtained by
 * calling {@link AlluxioBlockStore#getOutStream}.
 *
 * <p>
 * The type of {@link PacketOutStream} returned will depend on the user configuration and
 * cluster setup. A {@link LocalPacketOutStream} is returned if the client is co-located with an
 * Alluxio worker and the user has enabled this optimization. Otherwise,
 * {@link RemotePacketOutStream} will be returned which will write the data through an Alluxio
 * worker.
 */
@NotThreadSafe
public abstract class PacketOutStream extends OutputStream implements Cancelable {
  /** The block id of the block being written. */
  protected final long mId;
  /** Size of the block. */
  protected final long mBlockSize;
  protected ByteBuf mCurrentPacket = null;

  /** If the stream is closed, this can only go from false to true. */
  protected boolean mClosed;

  protected final Closer mCloser;
  protected final PacketWriter mPacketWriter;

  /**
   * Constructs a new {@link PacketOutStream}.
   *
   * @param id the block ID or the ufs file ID
   * @param blockSize the size of the block
   */
  public PacketOutStream(long id, long blockSize) throws IOException {
    mCloser = Closer.create();

    mId = id;
    mBlockSize = blockSize;
    mClosed = false;

    mPacketWriter = mCloser.register(createPacketWriter());
  }

  /**
   * @return the remaining size of the block
   */
  public long remaining() {
    return mBlockSize - mPacketWriter.pos() - (mCurrentPacket != null ?
        mCurrentPacket.readableBytes() : 0);
  }

  @Override
  public void write(int b) throws IOException {
    checkIfClosed();
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
  public abstract void cancel() throws IOException;

  @Override
  public void flush() throws IOException {
    updateCurrentPacket(true);
    mPacketWriter.flush();
  }

  @Override
  public abstract void close() throws IOException;

  protected void updateCurrentPacket(boolean force) throws IOException {
    if (mCurrentPacket == null) {
      mCurrentPacket = allocateBuffer();
    } else if (mCurrentPacket.writableBytes() == 0 || (mCurrentPacket.readableBytes() > 0
        && force)) {
      mPacketWriter.writePacket(mCurrentPacket);
      mCurrentPacket = allocateBuffer();
    }
  }

  /**
   * Convenience method for checking the state of the stream.
   */
  private void checkIfClosed() {
    Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_OUT_STREAM);
  }

  /**
   * @return a newly allocated byte buffer of the user defined default size
   */
  private ByteBuf allocateBuffer() {
    return PooledByteBufAllocator.DEFAULT.buffer(mPacketWriter.packetSize());
  }

  protected abstract PacketWriter createPacketWriter() throws IOException;
}
