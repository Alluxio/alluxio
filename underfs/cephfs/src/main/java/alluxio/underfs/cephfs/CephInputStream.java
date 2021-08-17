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

package alluxio.underfs.cephfs;

import com.ceph.fs.CephMount;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.InputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A stream for reading a file from CephFS.
 */
@NotThreadSafe
public final class CephInputStream extends InputStream {
  private CephMount mMount;
  private final int mFileHandle;
  private final long mFileLength;
  private long mCephPos = 0;
  private byte[] mBuf;
  private int mBufPos = 0;
  private int mBufValid = 0;
  private byte[] mOneByteBuf = new byte[1];
  /** Flag to indicate this stream has been closed, to ensure close is only done once. */
  private final AtomicBoolean mClosed = new AtomicBoolean(false);

  /**
   * Create a new CephInputStream.
   *
   * @param mount CephFileSystem mount
   * @param fh The filehandle provided by Ceph to reference
   * @param flength The current length of the file. If the length changes
   *                you will need to close and re-open it to access the new data
   */
  public CephInputStream(CephMount mount, int fh, long flength) {
    mFileLength = flength;
    mFileHandle = fh;
    mMount = mount;
    mBuf = new byte[1 << 21];
  }

  /**
   * Gets the current position of the stream.
   *
   * @return current positon of the stream
   */
  public synchronized long getPos() throws IOException {
    return mCephPos - mBufValid + mBufPos;
  }

  @Override
  public synchronized int available() throws IOException {
    if (mClosed.get()) {
      throw new IOException("file is closed");
    }
    return (int) (mFileLength - getPos());
  }

  /**
   * Seek.
   *
   * @param targetPos Position
   * @throws IOException throws
   */
  public synchronized void seek(long targetPos) throws IOException {
    if (targetPos > mFileLength) {
      throw new IOException(
        String.format("CephInputStream.seek: failed seek to position %d "
          + "on fd %d : Cannot seek after EOF fileSize %d",
          targetPos, mFileHandle, mFileLength));
    }

    long oldPos = mCephPos;
    mCephPos = mMount.lseek(mFileHandle, targetPos, CephMount.SEEK_SET);
    if (mCephPos < 0) {
      int ret = (int) mCephPos;
      mCephPos = oldPos;
      throw new IOException(String.format("ceph.lseek: failed ret = %d", ret));
    }
    mBufValid = 0;
    mBufPos = 0;
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    long targetPos = getPos() + n;
    seek(targetPos);
    return n;
  }

  @Override
  public synchronized int read() throws IOException {
    if (getPos() >= mFileLength) {
      return -1;
    }
    if (-1 == read(mOneByteBuf, 0, 1)) {
      return -1;
    }
    if (mOneByteBuf[0] < 0) {
      return 256 + (int) mOneByteBuf[0];
    } else {
      return mOneByteBuf[0];
    }
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    if (mClosed.get()) {
      throw new IOException(String.format("CephInputStream.read: cannot read %d "
        + "bytes from fd %d : stream closed",
        len, mFileHandle));
    }
    if (buf == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > buf.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    // ensure we're not past the end of the file
    if (getPos() >= mFileLength) {
      return -1;
    }
    int totalRead = 0;
    int initialLen = len;
    int read;

    do {
      read = Math.min(len, mBufValid - mBufPos);
      try {
        System.arraycopy(mBuf, mBufPos, buf, off, read);
      } catch (IndexOutOfBoundsException ie) {
        throw new IOException(String.format("CephInputStream.read: Indices out of bounds: "
          + "read length is %d, buffer offset is %d, and buffer size is %d",
          len, off, buf.length));
      } catch (ArrayStoreException ae) {
        throw new IOException(String.format("CephInputStream.read: failed to do an array"
          + "copy due to type mismatch..."));
      } catch (NullPointerException ne) {
        throw new IOException(String.format("CephInputStream.read: cannot read %d "
          + "bytes from fd %d: mBuf is null",
          len, mFileHandle));
      }
      mBufPos += read;
      len -= read;
      off += read;
      totalRead += read;
    } while (len > 0 && fillBuffer());

    return totalRead;
  }

  @Override
  public void close() throws IOException {
    if (mClosed.getAndSet(true)) {
      return;
    }
    mMount.close(mFileHandle);
  }

  private synchronized boolean fillBuffer() throws IOException {
    mBufValid = (int) mMount.read(mFileHandle, mBuf, mBuf.length, -1);
    mBufPos = 0;
    if (mBufValid < 0) {
      int err = mBufValid;
      mBufValid = 0;
      // attempt to reset to old position. If it fails, too bad.
      mMount.lseek(mFileHandle, mCephPos, CephMount.SEEK_SET);
      throw new IOException(String.format("Failed to fill read buffer! Error code: %d", err));
    }
    mCephPos += mBufValid;
    return (mBufValid != 0);
  }
}

