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
import alluxio.client.Cancelable;
import alluxio.exception.PreconditionMessage;

import com.google.common.base.Preconditions;
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
 * The type of {@link BlockOutStream} returned will depend on the user configuration and
 * cluster setup. A {@link LocalBlockOutStream} is returned if the client is co-located with an
 * Alluxio worker and the user has enabled this optimization. Otherwise,
 * {@link RemoteBlockOutStream} will be returned which will write the data through an Alluxio
 * worker.
 */
@NotThreadSafe
public abstract class BlockOutStream extends OutputStream implements Cancelable {
  /** The block id of the block being written. */
  protected final long mBlockId;
  /** Size of the block. */
  protected final long mBlockSize;
  protected ByteBuf mCurrentPacket = null;

  /** If the stream is closed, this can only go from false to true. */
  protected boolean mClosed;

  private long mPos = 0;

  /**
   * Constructs a new {@link BlockOutStream}.
   *
   * @param blockId the id of the block
   * @param blockSize the size of the block
   */
  public BlockOutStream(long blockId, long blockSize) {
    mBlockId = blockId;
    mBlockSize = blockSize;
    mClosed = false;
  }

  /**
   * @return the remaining size of the block
   */
  public long remaining() {
    return mBlockSize - mPos;
  }

  @Override
  public void write(int b) throws IOException {
    checkIfClosed();
    Preconditions.checkState(mPos < mBlockSize, PreconditionMessage.ERR_END_OF_BLOCK);
    updateCurrentPacket();
    mCurrentPacket.writeByte(b);
    mPos++;
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
      updateCurrentPacket();
      int toWrite = Math.min(len, mCurrentPacket.writableBytes());
      mCurrentPacket.writeBytes(b, off, toWrite);
      off += toWrite;
      len -= toWrite;
      mPos += toWrite;
    }
    updateCurrentPacket();
  }

  private void updateCurrentPacket() {
    if (mCurrentPacket == null) {
      mCurrentPacket = allocateBuffer();
      return;
    }
    if (mCurrentPacket.writableBytes() == 0) {
      sendPacket();
      mCurrentPacket = allocateBuffer();
      return;
    }
  }

  private void sendPacket() {

  }

  /**
   * Convenience method for checking the state of the stream.
   */
  protected void checkIfClosed() {
    Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_OUT_STREAM);
  }

  /**
   * @return a newly allocated byte buffer of the user defined default size
   */
  private ByteBuf allocateBuffer() {
    // TODO(now): Use the correct property here (packet size).
    return PooledByteBufAllocator.DEFAULT
        .buffer((int) Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES));
  }
}
