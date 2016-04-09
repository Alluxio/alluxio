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

package alluxio.client.block;

import alluxio.client.ClientContext;
import alluxio.exception.ExceptionMessage;
import alluxio.underfs.UnderFileSystem;

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
  /** The start of this block. This is the absolute position within the under store stream. */
  private final long mInitPos;
  /** The length of the block. */
  private final long mLength;
  /** The UFS path for this block. */
  private final String mUfsPath;

  /**
   * The current position for this block stream. This is the absolute position within the under
   * store stream.
   */
  private long mPos;
  /** The current under store stream. */
  private InputStream mUnderStoreStream;

  /**
   * Creates a new under storage file input stream.
   *
   * @param initPos the initial position
   * @param length the length
   * @param ufsPath the under file system path
   * @throws IOException if an I/O error occurs
   */
  public UnderStoreBlockInStream(long initPos, long length, String ufsPath) throws IOException {
    mInitPos = initPos;
    mLength = length;
    mUfsPath = ufsPath;
    setUnderStoreStream(initPos);
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
    if (data != -1) {
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
    if (bytesRead != -1) {
      mPos += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public long remaining() {
    return mInitPos + mLength - mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    long offset = mPos - mInitPos;
    if (pos < offset) {
      setUnderStoreStream(mInitPos + pos);
    } else {
      long toSkip = pos - offset;
      if (skip(toSkip) != toSkip) {
        throw new IOException(ExceptionMessage.FAILED_SEEK_FORWARD.getMessage(pos));
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
    long toSkip = Math.min(mInitPos + mLength - mPos, n);
    long skipped = mUnderStoreStream.skip(toSkip);
    if (toSkip != skipped) {
      throw new IOException(ExceptionMessage.FAILED_SKIP.getMessage(toSkip));
    }
    mPos += skipped;
    return skipped;
  }

  /**
   * Sets {@link #mUnderStoreStream} to the appropriate UFS stream starting from the specified
   * position. The specified position is the absolute position within the file, and not the
   * position relative to the start of the block.
   *
   * @param absPos the absolute position of the UFS file
   * @throws IOException if the stream from the position cannot be created
   */
  private void setUnderStoreStream(long absPos) throws IOException {
    if (mUnderStoreStream != null) {
      mUnderStoreStream.close();
    }
    UnderFileSystem ufs = UnderFileSystem.get(mUfsPath, ClientContext.getConf());
    mUnderStoreStream = ufs.open(mUfsPath);
    mPos = 0;
    if (absPos != skip(absPos)) {
      throw new IOException(ExceptionMessage.FAILED_SKIP.getMessage(absPos));
    }
  }
}
