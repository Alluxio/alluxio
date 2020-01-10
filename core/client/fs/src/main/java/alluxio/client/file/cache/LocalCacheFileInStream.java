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
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.grpc.OpenFilePOptions;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import net.jcip.annotations.NotThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

@NotThreadSafe
public class LocalCacheFileInStream extends FileInStream {

  /** Page size in bytes. Needs to be configurable */
  protected static final long PAGE_SIZE = 1 * Constants.MB;

  /** Local store to store pages. */
  private final CacheManager mCacheManager;

  private FileInStream mExternalFileInStream;

  private final byte[] mSingleByte = new byte[1];

  private final Closer mCloser = Closer.create();

  /** The size in bytes of the file. */
  private final long mLength;
  /** Current position of the stream, relative to the start of the file. */
  private long mPosition = 0;
  private boolean mClosed = false;
  private boolean mEOF = false;

  private final FileSystem mDelegatedFs;
  private final URIStatus mStatus;
  private final OpenFilePOptions mOpenOptions;

  public LocalCacheFileInStream(URIStatus status, OpenFilePOptions options, FileSystem delegatedFs,
      CacheManager cacheManager) {
    mStatus = status;
    mLength = status.getLength();
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
    int bytesRead = 0;
    // for each page, check if it is available in the cache
    while (bytesRead < len) {
      long currentPage = mPosition / PAGE_SIZE;
      int currentPageOffset = (int) (mPosition % PAGE_SIZE);
      int bytesLeftInPage = (int) Math.min(PAGE_SIZE - currentPageOffset, len - bytesRead);
      ReadableByteChannel cachedData = mCacheManager.get(mStatus.getFileId(), currentPage);
      if (cachedData != null) { // cache hit
        // wrap return byte array in a bytebuffer and set the pos/limit for the page read
        ByteBuffer buf = ByteBuffer.wrap(b);
        buf.position(off + bytesRead);
        buf.limit(off + bytesRead + bytesLeftInPage);
        // read data from cache
        while (buf.position() != buf.limit()) {
          if (cachedData.read(buf) == -1) {
            break;
          }
        }
        Preconditions.checkState(buf.position() == buf.limit());
        bytesRead += bytesLeftInPage;
        mPosition += bytesLeftInPage;
      } else { // cache miss
        byte[] page = readExternalPage(mPosition);
        mCacheManager.put(mStatus.getFileId(), currentPage, page);
        System.arraycopy(page, currentPageOffset, b, off + bytesRead, bytesLeftInPage);
        bytesRead += bytesLeftInPage;
        mPosition += bytesLeftInPage;
      }
    }
    Preconditions.checkState(bytesRead == len, "Invalid number of bytes read");
    return bytesRead;
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
    // TODO(calvin): Implement based on read
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
   *
   * @param pos position to set the external stream to
   */
  private FileInStream getExternalFileInStream(long pos) throws IOException {
    try {
      if (mExternalFileInStream == null) {
        AlluxioURI path = new AlluxioURI(mStatus.getPath());
        mExternalFileInStream = mDelegatedFs.openFile(path, mOpenOptions);
        mCloser.register(mExternalFileInStream);
      }
    } catch (AlluxioException e) {
      throw new IOException(e);
    }
    long pageStart = pos - (pos % PAGE_SIZE);
    if (mExternalFileInStream.getPos() != pageStart) {
      mExternalFileInStream.seek(pageStart);
    }
    return mExternalFileInStream;
  }

  /**
   * Reads a page from external storage which contains the position specified. Note that this makes
   * a copy of the page.
   *
   * TODO(calvin): Consider a more efficient API which does not require a data copy.
   *
   * @param pos the position which the page will contain
   * @return a byte array of the page data
   */
  private byte[] readExternalPage(long pos) throws IOException {
    long pageStart = pos - (pos % PAGE_SIZE);
    FileInStream stream = getExternalFileInStream(pageStart);
    int pageSize = (int) Math.min(PAGE_SIZE, mStatus.getLength() - pageStart);
    byte[] page = new byte[pageSize];
    if (stream.read(page) != pageSize) {
      throw new IOException("Failed to read complete page from external storage.");
    }
    return page;
  }
}
