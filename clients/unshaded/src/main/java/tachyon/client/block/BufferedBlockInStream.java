/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.block;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.conf.TachyonConf;
import tachyon.util.io.BufferUtils;

/**
 * Provides a stream API to read a block from Tachyon. An instance extending this class can be
 * obtained by calling {@link TachyonBlockStore#getInStream}. Multiple BlockInStreams can be opened
 * for a block. This class is not thread safe and should only be used by one thread.
 *
 * This class provides the same methods as a Java {@link InputStream} with additional methods from
 * Tachyon Stream interfaces.
 */
public abstract class BufferedBlockInStream extends BlockInStream {
  /** Value to set mBufferPos to mark the buffer as invalid. */
  private static final long INVALID_BUFFER_POS = -1L;

  /** The id of the block to which this instream provides access. */
  protected final long mBlockId;
  /** The size in bytes of the block. */
  protected final long mBlockSize;
  /** The address of the worker to read the data from. */
  protected final InetSocketAddress mLocation;
  /** The block store context which provides block worker clients. */
  protected final BlockStoreContext mContext;

  /** Internal buffer to improve small read performance. */
  protected ByteBuffer mBuffer;
  /** Flag indicating if the stream is closed, can only go from false to true. */
  protected boolean mClosed;
  /** Current position of the buffer's next byte, relative to the start of the block */
  protected long mBufferPos;
  /** Current position of the stream, relative to the start of the block. */
  protected long mPos;

  // TODO: Get the block lock here when the remote instream locks at a stream level
  public BufferedBlockInStream(long blockId, long blockSize, InetSocketAddress location) {
    mBlockId = blockId;
    mBlockSize = blockSize;
    mLocation = location;
    mBuffer = allocateBuffer();
    mBufferPos = INVALID_BUFFER_POS; // No data in buffer
    mClosed = false;
    mContext = BlockStoreContext.INSTANCE;
  }

  private ByteBuffer allocateBuffer() {
    TachyonConf conf = ClientContext.getConf();
    return ByteBuffer.allocate((int) conf.getBytes(Constants.USER_REMOTE_READ_BUFFER_SIZE_BYTE));
  }

  protected void checkIfClosed() {
    Preconditions.checkState(!mClosed, "Cannot do operations on a closed BlockInStream");
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
  }

  @Override
  public int read() throws IOException {
    checkIfClosed();
    if (remaining() == 0) {
      close();
      return -1;
    }
    if (remainingInBuffer() == 0) {
      updateBuffer();
    }
    mPos ++;
    mBufferPos ++;
    return BufferUtils.byteToInt(mBuffer.get());
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkIfClosed();
    Preconditions.checkArgument(b != null, "Read buffer cannot be null");
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        String.format("Buffer length (%d), offset(%d), len(%d)", b.length, off, len));
    if (len == 0) {
      return 0;
    } else if (remaining() == 0) { // End of block
      return -1;
    }

    int toRead = (int) Math.min(len, remaining());
    if (remainingInBuffer() > toRead) { // data is fully contained in the buffer
      mBuffer.get(b, off, toRead);
      mPos += toRead;
      mBufferPos += toRead;
      return toRead;
    }

    if (toRead > mBuffer.limit() / 2) { // directly read if request is > one-half buffer size
      mBufferPos = INVALID_BUFFER_POS; // Invalidate the buffer
      int bytesRead = directRead(b, off, toRead);
      mPos += bytesRead;
      return bytesRead;
    }

    // For a read <= half the buffer size, fill the buffer first, then read from the buffer.
    updateBuffer();
    mBuffer.get(b, off, toRead);
    mPos += toRead;
    mBufferPos += toRead;
    return toRead;
  }

  @Override
  public long remaining() {
    return mBlockSize - mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    checkIfClosed();
    Preconditions.checkArgument(pos >= 0, "Seek position is negative: " + pos);
    Preconditions.checkArgument(pos <= mBlockSize, "Seek position is past end of block: "
        + mBlockSize);
    mBufferPos = INVALID_BUFFER_POS;
    mPos = pos;
  }

  @Override
  public long skip(long n) throws IOException {
    checkIfClosed();
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(remaining(), n);
    mBufferPos = INVALID_BUFFER_POS;
    mPos += toSkip;
    return toSkip;
  }

  protected long remainingInBuffer() {
    if (mBufferPos == INVALID_BUFFER_POS) {
      return 0;
    }

    return mBuffer.remaining();
  }

  protected abstract int directRead(byte[] b, int off, int len) throws IOException;

  protected abstract void updateBuffer() throws IOException;
}
