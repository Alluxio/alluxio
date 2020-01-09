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

package alluxio.client.file.cache;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;
import alluxio.grpc.OpenFilePOptions;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import java.io.IOException;

public class LocalCacheFileInStream extends FileInStream {

  /** Page size in bytes. Needs to be configurable */
  private static final long PAGE_SIZE = 1 * Constants.MB;

  /** Local store to store pages */
  private final CacheManager mCacheManager;

  private FileInStream mExternalFileInStream;

  private final byte[] mSingleByte = new byte[1];

  private final Closer mCloser = Closer.create();
  /** The id of the block or UFS file to which this instream provides access. */
  private final long mId;
  /** The size in bytes of the file. */
  private final long mLength;
  /** Current position of the stream, relative to the start of the block. */
  private long mPosition = 0;
  private boolean mClosed = false;
  private boolean mEOF = false;

  private final FileSystem mDelegatedFs;
  private final AlluxioURI mFilePath;
  private final OpenFilePOptions mOpenOptions;

  public LocalCacheFileInStream(AlluxioURI path, OpenFilePOptions options, FileSystem delegatedFs,
                                CacheManager cacheManager) {
    mFilePath = path;
    mOpenOptions = options;
    mDelegatedFs = delegatedFs;
    mCacheManager = cacheManager;
  }

  @Override
  public int read() throws IOException {
    int bytesRead = read(mSingleByte);
    if (bytesRead == -1) {
      return -1;
    }
    Preconditions.checkState(bytesRead == 1);
    return BufferUtils.byteToInt(mSingleByte[0]);
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    // based on offset and length, loop over different pages
  }

  @Override
  public long skip(long n) {
    checkIfClosed();
    if (n <= 0) {
      return 0;
    }
    long toSkip = Math.min(remaining(), n);
    mPosition += toSkip;
    return toSkip;
  }

  @Override
  public void close() throws IOException {
    mCloser.close();
  }

  @Override
  public long remaining() {
    return mEOF ? 0 : mLength - mPosition;
  }

  @Override
  public int positionedRead(long pos, byte[] b, int off, int len) throws IOException {
    // Implement me
    return -1;
  }

  @Override
  public long getPos() {
    return mPosition;
  }

  @Override
  public void seek(long pos) {
    checkIfClosed();
    Preconditions.checkArgument(pos >= 0, "Seek position is negative: %s", pos);
    Preconditions
        .checkArgument(pos <= mLength, "Seek position (%s) exceeds the length of the file (%s)",
            pos, mLength);
    if (pos == mPosition) {
      return;
    }
    if (pos < mPosition) {
      mEOF = false;
    }
    mPosition = pos;
  }

  /**
   * Convenience method to ensure the stream is not closed.
   */
  private void checkIfClosed() {
    Preconditions.checkState(!mClosed, "Cannot operate on a closed stream");
  }

  /**
   * Convenience method to get external file reader with lazy init.
   */
  synchronized private FileInStream getExternalFileInStream() throws IOException,
      AlluxioException {
    if (mExternalFileInStream == null) {
      mExternalFileInStream = mDelegatedFs.openFile(mFilePath, mOpenOptions);
    }
    return mCloser.register(mExternalFileInStream);
  }
}
