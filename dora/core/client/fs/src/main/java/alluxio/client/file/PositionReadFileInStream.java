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

package alluxio.client.file;

import alluxio.PositionReader;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.PreconditionMessage;

import com.amazonaws.annotation.NotThreadSafe;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * This class extends the {@link FileInStream} class and provides the functionality to read a file
 * from a specific position. It maintains the current position, length of the file, and a cache
 * for prefetching data. It uses a PositionReader to read the file positions.
 */
@NotThreadSafe
public class PositionReadFileInStream extends FileInStream {
  private final long mLength; // length of the file
  private long mPos = 0; // current position
  private boolean mClosed; // flag indicating if the stream is closed
  private final PositionReader mPositionReader; // reader for file positions
  private final PrefetchCache mPrefetchCache; // cache for prefetching data

  /**
   * Initializes a new instance of the PositionReadFileInStream class with the given PositionReader
   * and length parameters. It sets the PositionReader, length, and creates a PrefetchCache object
   * with the specified streaming multiplier and length.
   *
   * @param reader the PositionReader used for reading the file positions
   * @param length the length of the file
   */
  public PositionReadFileInStream(PositionReader reader,
                                  long length) {
    mPositionReader = reader;
    mLength = length;
    mPrefetchCache = new PrefetchCache(
            Configuration.getInt(PropertyKey.USER_POSITION_READER_STREAMING_MULTIPLIER), mLength);
  }
  @VisibleForTesting
  public PositionReadFileInStream(PositionReader reader, long length,
                                  PrefetchCache prefetchCache) {
    mPositionReader = reader;
    mLength = length;
    mPrefetchCache = prefetchCache;
  }
  /**
   * Returns the number of remaining bytes to be read from the stream.
   *
   * @return the number of remaining bytes
   */
  @Override
  public long remaining() {
    return mLength - mPos;
  }

//  @VisibleForTesting
//  int getBufferedLength() {
//    return mPrefetchCache.getCache().readableBytes();
//  }
//
//  @VisibleForTesting
//  long getBufferedPosition() {
//    return mPrefetchCache.getCacheStartPos();
//  }
//
//  @VisibleForTesting
//  int getPrefetchSize() {
//    return mPrefetchCache.getPrefetchSize();
//  }

  /**
   * Reads data from the stream into the provided byte array.
   *
   * @param b the byte array to read data into
   * @param off the starting offset in the byte array
   * @param len the maximum number of bytes to read
   * @return the number of bytes read, or -1 if the end of the stream is reached
   * @throws IOException if an I/O error occurs
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Objects.requireNonNull(b, "Read buffer cannot be null");
    return read(ByteBuffer.wrap(b), off, len);
  }

  /**
   * Reads data from the stream into the provided ByteBuffer.
   *
   * @param byteBuffer the ByteBuffer to read data into
   * @param off the starting offset in the ByteBuffer
   * @param len the maximum number of bytes to read
   * @return the number of bytes read, or -1 if the end of the stream is reached
   * @throws IOException if an I/O error occurs
   */
  @Override
  public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
    byteBuffer.position(off).limit(off + len);
    mPrefetchCache.addTrace(mPos, len);
    return readDataFromCacheAndSource(mPos, byteBuffer,true);
  }

  /**
   * Reads data from the stream at the specified position into the provided byte array.
   *
   * @param position the position in the stream to read from
   * @param buffer the byte array to read data into
   * @param offset the starting offset in the byte array
   * @param len the maximum number of bytes to read
   * @return the number of bytes read, or -1 if the end of the stream is reached
   * @throws IOException if an I/O error occurs
   */
  @Override
  public int positionedRead(long position, byte[] buffer, int offset, int len)
          throws IOException {
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer, offset, len);
    mPrefetchCache.addTrace(position, len);
    return readDataFromCacheAndSource(position, byteBuffer,false);
  }

  /**
   * Returns the current position in the stream.
   *
   * @return the current position
   * @throws IOException if an I/O error occurs
   */
  @Override
  public long getPos() throws IOException {
    return mPos;
  }

  /**
   * Sets the current position in the stream.
   *
   * @param pos the position to seek to
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void seek(long pos) throws IOException {
    Preconditions.checkState(!mClosed, "Cannot do operations on a closed BlockInStream");
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mLength,
            "Seek position past the end of the read region (block or file).");
    if (pos == mPos) {
      return;
    }
    mPos = pos;
  }

  /**
   * Skips over and discards a specified number of bytes from the stream.
   *
   * @param n the number of bytes to skip
   * @return the actual number of bytes skipped
   * @throws IOException if an I/O error occurs
   */
  @Override
  public long skip(long n) throws IOException {
    Preconditions.checkState(!mClosed, "Cannot do operations on a closed BlockInStream");
    if (n <= 0) {
      return 0;
    }
    long toSkip = Math.min(remaining(), n);
    seek(mPos + toSkip);
    return toSkip;
  }

  /**
   * Closes the stream and releases any system resources associated with it.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mPositionReader.close();
    mPrefetchCache.close();
  }

  /**
   * Reads data from the cache and the source into the provided ByteBuffer.
   *
   * @param pos the position to read data from
   * @param byteBuffer the ByteBuffer to read data into
   * @param updatePosition whether to update the position of the stream
   * @return the number of bytes read, or -1 if the end of the stream is reached
   * @throws IOException if an I/O error occurs
   */
  public int readDataFromCacheAndSource(long pos, ByteBuffer byteBuffer,boolean updatePosition) throws IOException {
    int totalBytesRead = 0;
    int bytesReadFromCache = mPrefetchCache.fillWithCache(pos, byteBuffer);
    totalBytesRead += bytesReadFromCache;
    pos += bytesReadFromCache;

    if (!byteBuffer.hasRemaining()) {
      return totalBytesRead;
    }

    int bytesPrefetched = mPrefetchCache.prefetch(mPositionReader, pos, byteBuffer.remaining());
    if (bytesPrefetched < 0) {
      if (totalBytesRead == 0) {
        return -1;
      }
      return totalBytesRead;
    }

    bytesReadFromCache = mPrefetchCache.fillWithCache(pos, byteBuffer);
    totalBytesRead += bytesReadFromCache;
    pos += bytesReadFromCache;

    if (!byteBuffer.hasRemaining()) {
      return totalBytesRead;
    }

    int bytesRead = mPositionReader.read(pos, byteBuffer, byteBuffer.remaining());
    if (bytesRead < 0) {
      if (totalBytesRead == 0) {
        return -1;
      }
      return totalBytesRead;
    }

    totalBytesRead += bytesRead;
    pos += bytesRead;

    if (updatePosition) {
      mPos = pos;
    }

    return totalBytesRead;
  }
}


