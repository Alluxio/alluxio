/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.client.netty.NettyUnderFileSystemFileReader;
import alluxio.exception.PreconditionMessage;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Provides a streaming API to read a file in the under file system through an Alluxio worker's data
 * server. This class is based off of {@link alluxio.client.block.BufferedBlockInStream}.
 */
// TODO(calvin): See if common logic in this class and buffered block in stream can be abstracted
@NotThreadSafe
public final class UnderFileSystemFileInStream extends InputStream {
  /** Current position of the stream, relative to the start of the block. */
  private long mPos;
  /** If the bytes in the internal buffer are valid. */
  private boolean mIsBufferValid;
  /** Flag indicating EOF has been reached. */
  private boolean mEOF;
  /** Reader to the worker, currently only implemented through Netty. */
  private final NettyUnderFileSystemFileReader mReader;
  /** Address of the worker to write to. */
  private final InetSocketAddress mAddress;
  /** Worker file id referencing the file to write to. */
  private final long mUfsFileId;

  /** Internal buffer to improve small read performance. */
  private ByteBuffer mBuffer;
  /** Flag indicating if the stream is closed, can only go from false to true. */
  private boolean mClosed;

  /**
   * Constructs a new input stream for the under file system file. Creates the initial buffer
   * which is empty and invalid.
   *
   * @param address worker address to read from
   * @param ufsFileId worker specific file id referencing the file to read
   */
  public UnderFileSystemFileInStream(InetSocketAddress address, long ufsFileId) {
    mAddress = address;
    mUfsFileId = ufsFileId;
    mReader = new NettyUnderFileSystemFileReader();
    mBuffer = allocateBuffer();
    mIsBufferValid = false; // No data in buffer
    mEOF = false;
    mClosed = false;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mReader.close();
    mClosed = true;
  }

  @Override
  public int read() throws IOException {
    checkIfClosed();
    if (!mEOF && (!mIsBufferValid || mBuffer.remaining() == 0)) {
      updateBuffer();
    }
    if (mEOF) {
      return -1;
    }
    mPos++;
    return BufferUtils.byteToInt(mBuffer.get());
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
        PreconditionMessage.ERR_BUFFER_STATE, b.length, off, len);
    if (len == 0) {
      return 0;
    } else if (mEOF) { // At end of file
      return -1;
    }

    if (mIsBufferValid && mBuffer.remaining() > len) { // data is fully contained in the buffer
      mBuffer.get(b, off, len);
      mPos += len;
      return len;
    }

    if (len > mBuffer.capacity() / 2) { // directly read if request is > one-half buffer size
      mIsBufferValid = false;
      int bytesRead = directRead(b, off, len);
      if (bytesRead != -1) {
        mPos += bytesRead;
      } else { // Hit end of file, set flag
        mEOF = true;
      }
      return bytesRead;
    }

    // For a read <= half the buffer size, fill the buffer first, then read from the buffer.
    updateBuffer();
    int toRead = Math.min(mBuffer.remaining(), len);
    mBuffer.get(b, off, toRead);
    mPos += toRead;
    return toRead;
  }

  @Override
  public long skip(long n) throws IOException {
    checkIfClosed();
    if (n <= 0) {
      return 0;
    }

    mIsBufferValid = false;
    mPos += n;
    return n;
  }

   /**
   * Initializes the internal buffer based on the user's specified size. Any reads above half
   * this size will not be buffered.
   *
   * @return a heap buffer of user configured size
   */
  private ByteBuffer allocateBuffer() {
    Configuration conf = ClientContext.getConf();
    return ByteBuffer.allocate(
        (int) conf.getBytes(Constants.USER_BLOCK_REMOTE_READ_BUFFER_SIZE_BYTES));
  }

  /**
   * Convenience method to ensure the stream is not closed.
   */
  private void checkIfClosed() {
    Preconditions.checkState(!mClosed, PreconditionMessage.ERR_CLOSED_BLOCK_IN_STREAM);
  }

  /**
   * Directly reads data to the given byte array. The data will not go through the internal buffer.
   * This method should not modify mPos or update any metrics collection for bytes read.
   *
   * @param b the byte array to write the data to
   * @param off the offset in the array to write to
   * @param len the length of data to write into the array must always be valid within the block
   * @return the number of bytes successfully read, -1 if at EOF before reading
   * @throws IOException if an error occurs reading the data
   */
  // TODO(calvin): This may be better implemented with a Bytebuffer instead of byte array parameter
  private int directRead(byte[] b, int off, int len) throws IOException {
    int bytesLeft = len;
    int bytesRead = 0;
    int offset = off;
    while (bytesLeft > 0) {
      // mPos shouldn't be modified, so keep track of our updated position
      long currentPosition = mPos + bytesRead;

      ByteBuffer data = mReader.read(mAddress, mUfsFileId, currentPosition, bytesLeft);
      if (data == null) { // No more data
        if (bytesRead == 0) { // Did not read any bytes, at EOF
          return -1;
        }
        break;
      }
      int read = data.remaining();
      data.get(b, offset, read);
      offset += read;
      bytesRead += read;
      bytesLeft -= read;
    }
    return bytesRead;
  }

  /**
   * Updates the buffer so it is ready to be read from. After calling this method, the buffer will
   * be positioned at 0. If the buffer does not have any valid data, the EOF flag will be set.
   *
   * @throws IOException if an error occurs reading the data
   */
  private void updateBuffer() throws IOException {
    mBuffer.clear();
    int bytesRead = directRead(mBuffer.array(), 0, mBuffer.capacity());
    if (bytesRead != -1) {
      mBuffer.limit(bytesRead);
      mIsBufferValid = true;
    } else {
      mEOF = true;
    }
  }
}
