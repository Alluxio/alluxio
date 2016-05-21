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
import alluxio.client.ClientContext;
import alluxio.exception.ExceptionMessage;

import java.io.IOException;
import java.io.InputStream;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

/**
 * This class provides a streaming API to read a fixed chunk from a file in the under storage
 * system. The user may freely seek and skip within the fixed chunk of data. The under storage
 * system read does not guarantee any locality and is dependent on the implementation of the under
 * storage client.
 */
@NotThreadSafe
public abstract class UnderStoreBlockInStream extends BlockInStream {
  /**
   * The block size of the file. See {@link #getLength()} for more length information.
   */
  private final long mFileBlockSize;
  /** The start of this block. This is the absolute position within the UFS file. */
  protected final long mInitPos;
  /** The UFS path for this block. */
  protected final String mUfsPath;
  /**
   * The length of this current block. This may be {@link Constants#UNKNOWN_SIZE}, and may be
   * updated to a valid length. See {@link #getLength()} for more length information.
   */
  protected long mLength;
  /**
   * The current position for this block stream. This is the position within this block, and not
   * the absolute position within the UFS file.
   */
  protected long mPos;
  /** The current under store stream. */
  protected InputStream mUnderStoreStream;

  /**
   * Creates a new under storage file input stream.
   *
   * @param initPos the initial position
   * @param length the length of this current block (allowed to be {@link Constants#UNKNOWN_SIZE})
   * @param fileBlockSize the block size for the file
   * @param ufsPath the under file system path
   */
  protected UnderStoreBlockInStream(long initPos, long length, long fileBlockSize, String ufsPath) {
    mInitPos = initPos;
    mLength = length;
    mFileBlockSize = fileBlockSize;
    mUfsPath = ufsPath;
  }

  /**
   * Factory for creating {@link UnderStoreBlockInStream}s.
   */
  @ThreadSafe
  public static final class Factory {

    private Factory() {} // prevent instantiation

    /**
     * Creates an under store block in stream, if ufs operation delegation is enabled, the stream
     * will read from an Alluxio worker, if not the stream will be directly from an under storage
     * system client. The stream will be set to the beginning of the block.
     *
     * @param blockStart the start position of the block stream relative to the entire file
     * @param length length of this block
     * @param blockSize the block size of the file
     * @param path the path of the file in the under storage
     * @return a stream which can access data from blockStart to blockStart + length
     * @throws IOException if an error occurs creating the stream
     */
    public static UnderStoreBlockInStream create(long blockStart, long length, long blockSize,
        String path) throws IOException {
      UnderStoreBlockInStream stream;
      if (ClientContext.getConf().getBoolean(Constants.USER_UFS_OPERATION_DELEGATION)) {
        stream = new DelegatedUnderStoreBlockInStream(blockStart, length, blockSize, path);
      } else {
        stream = new DirectUnderStoreBlockInStream(blockStart, length, blockSize, path);
      }
      stream.setUnderStoreStream(0);
      return stream;
    }
  }

  @Override
  public void close() throws IOException {
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
  protected abstract void setUnderStoreStream(long pos) throws IOException;

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
