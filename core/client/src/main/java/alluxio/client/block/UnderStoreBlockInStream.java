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

package alluxio.client.block;

import alluxio.Constants;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.PreconditionMessage;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class provides a streaming API to read a fixed chunk from a file in the under storage
 * system. The user may freely seek and skip within the fixed chunk of data. The under storage
 * system read does not guarantee any locality and is dependent on the implementation of the under
 * storage client.
 */
@NotThreadSafe
public final class UnderStoreBlockInStream extends BlockInStream {
  /**
   * The block size of the file. See {@link #getLength()} for more length information.
   */
  private final long mFileBlockSize;
  /** The start of this block. This is the absolute position within the UFS file. */
  private final long mInitPos;
  /** The factory to use to create under storage input streams. */
  private final UnderStoreStreamFactory mUnderStoreStreamFactory;
  /**
   * The length of this current block. This may be {@link Constants#UNKNOWN_SIZE}, and may be
   * updated to a valid length. See {@link #getLength()} for more length information.
   */
  private long mLength;
  /**
   * The current position for this block stream. This is the position within this block, and not
   * the absolute position within the UFS file.
   */
  private long mPos;
  /** The current under store stream. */
  private InputStream mUnderStoreStream;

  /**
   * A factory which can create an input stream to under storage.
   */
  public interface UnderStoreStreamFactory extends AutoCloseable {
    /**
     * @return an input stream to under storage
     * @throws IOException if an IO exception occurs
     */
    InputStream create() throws IOException;

    /**
     * Closes the factory, releasing any resources it was holding.
     *
     * @throws IOException if an IO exception occurs
     */
    void close() throws IOException;
  }

  /**
   * Creates an under store block in stream which will read from the streams created by the given
   * {@link UnderStoreStreamFactory}. The stream will be set to the beginning of the block.
   *
   * @param initPos the initial position
   * @param length the length of this current block (allowed to be {@link Constants#UNKNOWN_SIZE})
   * @param fileBlockSize the block size for the file
   * @param underStoreStreamFactory a factory for getting input streams from the under storage; the
   *        constructed {@link UnderStoreBlockInStream} is responsible for closing it
   *
   * @throws IOException if an IO exception occurs while creating the under storage input stream
   */
  public UnderStoreBlockInStream(long initPos, long length, long fileBlockSize,
      UnderStoreStreamFactory underStoreStreamFactory) throws IOException {
    mInitPos = initPos;
    mLength = length;
    mFileBlockSize = fileBlockSize;
    mUnderStoreStreamFactory = underStoreStreamFactory;
    setUnderStoreStream(0);
  }

  @Override
  public void close() throws IOException {
    mUnderStoreStreamFactory.close();
    mUnderStoreStream.close();
  }

  @Override
  public int read() throws IOException {
    if (remaining() == 0) {
      return -1;
    }
    int data = mUnderStoreStream.read();
    if (data == -1) {
      if (mLength == Constants.UNKNOWN_SIZE) {
        // End of stream. Compute the length.
        mLength = mPos;
      }
    } else {
      // Read a valid byte, update the position.
      mPos++;
    }
    return data;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (remaining() == 0) {
      return -1;
    }
    int bytesRead = mUnderStoreStream.read(b, off, len);
    if (bytesRead == -1) {
      if (mLength == Constants.UNKNOWN_SIZE) {
        // End of stream. Compute the length.
        mLength = mPos;
      }
    } else {
      // Read valid data, update the position.
      mPos += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public long remaining() {
    return getLength() - mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < mPos) {
      setUnderStoreStream(pos);
    } else {
      long toSkip = pos - mPos;
      if (skip(toSkip) != toSkip) {
        throw new IOException(ExceptionMessage.FAILED_SEEK.getMessage(pos));
      }
    }
  }

  @Override
  public long skip(long n) throws IOException {
    // Negative skip returns 0
    if (n <= 0) {
      return 0;
    }
    // Cannot skip beyond boundary
    long toSkip = Math.min(getLength() - mPos, n);
    long skipped = mUnderStoreStream.skip(toSkip);
    if (mLength != Constants.UNKNOWN_SIZE && toSkip != skipped) {
      throw new IOException(ExceptionMessage.FAILED_SKIP.getMessage(toSkip));
    }
    mPos += skipped;
    return skipped;
  }

  /**
   * Sets {@link #mUnderStoreStream} to the appropriate UFS stream starting from the specified
   * position. The specified position is the position within the block, and not the absolute
   * position within the entire file.
   *
   * @param pos the position within this block
   * @throws IOException if the stream from the position cannot be created
   */
  private void setUnderStoreStream(long pos) throws IOException {
    if (mUnderStoreStream != null) {
      mUnderStoreStream.close();
    }
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mLength,
        PreconditionMessage.ERR_SEEK_PAST_END_OF_BLOCK.toString(), pos);
    mUnderStoreStream = mUnderStoreStreamFactory.create();
    long streamStart = mInitPos + pos;
    // The stream is at the beginning of the file, so skip to the correct absolute position.
    if (streamStart != 0 && streamStart != mUnderStoreStream.skip(streamStart)) {
      mUnderStoreStream.close();
      throw new IOException(ExceptionMessage.FAILED_SKIP.getMessage(pos));
    }
    // Set the current block position to the specified block position.
    mPos = pos;
  }

  /**
   * Returns the length of the current UFS block. This method handles the situation when the UFS
   * file has an unknown length. If the UFS file has an unknown length, the length returned will
   * be the file block size. If the block is completely read, the length will be updated to the
   * correct block size.
   *
   * @return the length of this current block
   */
  private long getLength() {
    if (mLength != Constants.UNKNOWN_SIZE) {
      return mLength;
    }
    // The length is unknown. Use the max block size until the computed length is known.
    return mFileBlockSize;
  }
}
