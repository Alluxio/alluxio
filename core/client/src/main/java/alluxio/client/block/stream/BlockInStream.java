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
import alluxio.client.Seekable;
import alluxio.exception.PreconditionMessage;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public abstract class BlockInStream extends InputStream implements BoundedStream, Seekable {
  /** The id of the block to which this instream provides access. */
  protected final long mBlockId;
  /** The size in bytes of the block. */
  protected final long mBlockSize;

  /** Current position of the stream, relative to the start of the block. */
  private long mPos = 0;
  /** The current packet. */
  private ByteBuf mCurrentPacket = null;

  private PacketReader mReader = null;

  private boolean mClosed = false;

  private boolean mBlockIsRead = false;

  public BlockInStream(long blockId, long blockSize) throws IOException {
    mBlockId = blockId;
    mBlockSize = blockSize;
  }

  @Override
  public void close() throws IOException {
    if (mCurrentPacket != null) {
      ReferenceCountUtil.release(mCurrentPacket);
    }
  }

  @Override
  public int read() throws IOException {
    checkIfClosed();
    if (remaining() == 0) {
      close();
      return -1;
    }

    readPacket();

    mPos++;
    mBlockIsRead = true;
    return BufferUtils.byteToInt(mCurrentPacket.readByte());
  }

  /**
   * Reads a new packet from the channel.
   *
   * @throws IOException if it fails to read the packet
   */
  private void readPacket() throws IOException {
    if (mCurrentPacket.readableBytes() == 0) {
      destroyPacket(mCurrentPacket);
      if (mReader == null) {
        mReader = createBlockReader(mPos, mBlockSize - mPos);
      }
      mCurrentPacket = mReader.readPacket();
    }
  }

  protected void destroyPacket(ByteBuf packet) {
    ReferenceCountUtil.release(packet);
  }

  protected abstract PacketReader createBlockReader(long offset, long len);

  /**
   * Close the current block reader.
   */
  private void closeReader() {
    mReader.close();
    mReader = null;
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
    } else if (remaining() == 0) { // End of block
      return -1;
    }

    readPacket();
    int toRead = Math.min(len, mCurrentPacket.readableBytes());
    mCurrentPacket.readBytes(b, off, toRead);
    mPos += toRead;
    return toRead;
  }

  @Override
  public long remaining() {
    return mBlockSize - mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    checkIfClosed();
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mBlockSize,
        PreconditionMessage.ERR_SEEK_PAST_END_OF_BLOCK.toString(), mBlockSize);
    if (pos == mPos) {
      return;
    }

    closeReader();
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

    closeReader();
    return toSkip;
  }

  /**
   * @return the current position of the stream
   */
  protected long getPosition() {
    return mPos;
  }

  /**
   * Increments the number of bytes read metric. Inheriting classes should implement this to
   * increment the correct metric.
   *
   * @param bytes number of bytes to record as read
   */
  protected abstract void incrementBytesReadMetric(int bytes);

  /**
   * Convenience method to ensure the stream is not closed.
   */
  private void checkIfClosed() {
    Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
  }
}
