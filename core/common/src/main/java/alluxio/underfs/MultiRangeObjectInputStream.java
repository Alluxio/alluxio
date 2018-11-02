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

package alluxio.underfs;

import alluxio.Configuration;
import alluxio.PropertyKey;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A stream for reading data using range queries.
 */
@NotThreadSafe
public abstract class MultiRangeObjectInputStream extends InputStream {

  /** Has the stream been closed. */
  protected boolean mClosed;
  /** The backing input stream. */
  protected InputStream mStream;
  /** The current position of the stream. */
  protected long mPos;
  /** Position the current stream was open till (exclusive). */
  protected long mEndPos;

  @Override
  public void close() throws IOException {
    if (!mClosed) {
      closeStream();
    }
    mClosed = true;
  }

  @Override
  public int read() throws IOException {
    openStream();
    int value = mStream.read();
    if (value != -1) { // valid data read
      mPos++;
      closeStreamIfBoundary();
    }
    return value;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int offset, int length) throws IOException {
    if (length == 0) {
      return 0;
    }
    openStream();
    int read = mStream.read(b, offset, length);
    if (read != -1) {
      mPos += read;
      closeStreamIfBoundary();
    }
    return read;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    closeStream();
    mPos += n;
    openStream();
    return n;
  }

  /**
   * Close the current stream if the boundary for a range is crossed.
   */
  private void closeStreamIfBoundary() throws IOException {
    if (mPos >= mEndPos) {
      closeStream();
    }
  }

  /**
   * Closes the current stream.
   */
  private void closeStream() throws IOException {
    if (mStream == null) {
      return;
    }
    mStream.close();
    mStream = null;
  }

  /**
   * Open a new stream reading a range. When endPos > content length, the returned stream should
   * read till the last valid byte of the input. The behaviour is undefined when (startPos < 0),
   * (startPos >= content length), or (endPos <= 0).
   *
   * @param startPos start position in bytes (inclusive)
   * @param endPos end position in bytes (exclusive)
   * @return a new {@link InputStream}
   */
  protected abstract InputStream createStream(long startPos, long endPos) throws IOException;

  /**
   * Block size for reading an object in chunks.
   *
   * @return block size in bytes
   */
  private long getBlockSize() {
    return Configuration.getBytes(PropertyKey.USER_BLOCK_SIZE_BYTES_DEFAULT);
  }

  /**
   * Opens a new stream at mPos if the wrapped stream mStream is null.
   */
  private void openStream() throws IOException {
    if (mClosed) {
      throw new IOException("Stream closed");
    }
    if (mStream != null) { // stream is already open
      return;
    }
    final long blockSize = getBlockSize();
    final long endPos = mPos + blockSize - (mPos % blockSize);
    mEndPos = endPos;
    mStream = createStream(mPos, endPos);
  }
}
