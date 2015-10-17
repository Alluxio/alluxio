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
 * obtained by calling {@link TachyonBlockStore#getInStream}. The buffer size of the stream can be
 * set through configuration. Multiple BufferedBlockInStreams can be opened for a block. This class
 * is not thread safe and should only be used by one thread.
 *
 * This class provides the same methods as a Java {@link InputStream} with additional methods from
 * Tachyon Stream interfaces.
 */
public abstract class BufferedBlockInStream extends BlockInStream {
  // Error strings for preconditions in order to improve performance
  private static final String ERR_BUFFER_NULL = "Read buffer cannot be null";
  private static final String ERR_BUFFER_STATE = "Buffer length: %s, offset: %s, len: %s";
  private static final String ERR_CLOSED = "Cannot do operations on a closed BlockInStream";
  private static final String ERR_SEEK_PAST_END_OF_BLOCK = "Seek position past end of block: %s";
  private static final String ERR_SEEK_NEGATIVE = "Seek position is negative: %s";

  /** Current position of the stream, relative to the start of the block. */
  private long mPos;
  /** Flag indicating if the buffer has valid data. */
  private boolean mBufferIsValid;

  /** The id of the block to which this instream provides access. */
  protected final long mBlockId;
  /** The size in bytes of the block. */
  protected final long mBlockSize;
  /** The address of the worker to read the data from. */
  protected final InetSocketAddress mLocation;

  /** Internal buffer to improve small read performance. */
  protected ByteBuffer mBuffer;
  /** Flag indicating if the stream is closed, can only go from false to true. */
  protected boolean mClosed;

  /**
   * Basic constructor for a BufferedBlockInStream. This sets the necessary variables and creates
   * the initial buffer which is empty and invalid.
   *
   * @param blockId block id for this stream
   * @param blockSize size of the block in bytes
   * @param location worker address to read the block from
   */
  // TODO: Get the block lock here when the remote instream locks at a stream level
  public BufferedBlockInStream(long blockId, long blockSize, InetSocketAddress location) {
    mBlockId = blockId;
    mBlockSize = blockSize;
    mLocation = location;
    mBuffer = allocateBuffer();
    mBufferIsValid = false; // No data in buffer
    mClosed = false;
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
    if (!mBufferIsValid || mBuffer.remaining() == 0) {
      updateBuffer();
    }
    mPos ++;
    return BufferUtils.byteToInt(mBuffer.get());
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    checkIfClosed();
    Preconditions.checkArgument(b != null, ERR_BUFFER_NULL);
    Preconditions.checkArgument(
        off >= 0 && len >= 0 && len + off <= b.length, ERR_BUFFER_STATE, b.length, off, len);
    if (len == 0) {
      return 0;
    } else if (remaining() == 0) { // End of block
      return -1;
    }

    int toRead = (int) Math.min(len, remaining());
    if (mBufferIsValid && mBuffer.remaining() > toRead) { // data is fully contained in the buffer
      mBuffer.get(b, off, toRead);
      mPos += toRead;
      return toRead;
    }

    if (toRead > mBuffer.capacity() / 2) { // directly read if request is > one-half buffer size
      mBufferIsValid = false;
      int bytesRead = directRead(b, off, toRead);
      mPos += bytesRead;
      incrementBytesReadMetric(bytesRead);
      return bytesRead;
    }

    // For a read <= half the buffer size, fill the buffer first, then read from the buffer.
    updateBuffer();
    mBuffer.get(b, off, toRead);
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
    Preconditions.checkArgument(pos >= 0, ERR_SEEK_NEGATIVE, pos);
    Preconditions.checkArgument(pos <= mBlockSize, ERR_SEEK_PAST_END_OF_BLOCK, mBlockSize);
    mBufferIsValid = false;
    mPos = pos;
  }

  @Override
  public long skip(long n) throws IOException {
    checkIfClosed();
    if (n <= 0) {
      return 0;
    }

    long toSkip = Math.min(remaining(), n);
    mBufferIsValid = false;
    mPos += toSkip;
    return toSkip;
  }

  /**
   * @return the current position of the stream
   */
  protected long getPosition() {
    return mPos;
  }

  /**
   * Reads from the data source into the buffer. The buffer should be at position 0 and have len
   * valid bytes available after this method is called. This method should not modify mBufferPos,
   * mPos, or increment any metrics.
   *
   * @param len length of data to fill in the buffer, must always be <= buffer size
   * @throws IOException if the read failed to buffer the requested number of bytes
   */
  protected abstract void bufferedRead(int len) throws IOException;

  /**
   * Directly reads data to the given byte array. The data will not go through the internal buffer.
   * This method should not modify mPos or update any metrics collection for bytes read.
   *
   * @param b the byte array to write the data to
   * @param off the offset in the array to write to
   * @param len the length of data to write into the array must always be valid within the block
   * @return the number of bytes successfully read
   * @throws IOException if an error occurs reading the data
   */
  protected abstract int directRead(byte[] b, int off, int len) throws IOException;

  /**
   * Increments the number of bytes read metric. Inheriting classes should implement this to
   * increment the correct metric.
   *
   * @param bytes number of bytes to record as read
   */
  protected abstract void incrementBytesReadMetric(int bytes);

  /**
   * Initializes the internal buffer based on the user's specified size. Any reads above half
   * this size will not be buffered.
   *
   * @return a heap buffer of user configured size
   */
  private ByteBuffer allocateBuffer() {
    TachyonConf conf = ClientContext.getConf();
    return ByteBuffer.allocate(
        (int) conf.getBytes(Constants.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES));
  }

  /**
   * Convenience method to ensure the stream is not closed.
   */
  private void checkIfClosed() {
    Preconditions.checkState(!mClosed, ERR_CLOSED);
  }

  /**
   * Updates the buffer so it is ready to be read from. After calling this method, the buffer will
   * be positioned at 0 and mBufferIsValid will be true. Inheriting classes should implement
   * bufferedRead(int) for their read specific logic.
   *
   * @throws IOException if an error occurs reading the data
   */
  private void updateBuffer() throws IOException {
    int toRead = (int) Math.min(mBuffer.capacity(), remaining());
    bufferedRead(toRead);
    Preconditions.checkState(mBuffer.remaining() == toRead);
    mBufferIsValid = true;
    incrementBytesReadMetric(toRead);
  }
}
