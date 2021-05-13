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
  private long mPos = 0;
  private byte[] mBuf = new byte[1];
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
  }

  @Override
  public int available() throws IOException {
    return (int) (mFileLength - mPos);
  }

  /**
   * Seek.
   *
   * @param targetPos Position
   * @throws IOException throws
   */
  public void seek(long targetPos) throws IOException {
    if (targetPos > mFileLength) {
      throw new IOException(
          "CephInputStream.seek: failed seek to position " + targetPos
          + " on fd " + mFileHandle + ": Cannot seek after EOF " + mFileLength);
    }

    long oldPos = mPos;
    mPos = mMount.lseek(mFileHandle, targetPos, CephMount.SEEK_SET);
    if (mPos < 0) {
      int ret = (int) mPos;
      mPos = oldPos;
      throw new IOException("ceph.lseek: ret = " + ret);
    }
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    long targetPos = mPos + n;
    seek(targetPos);
    return n;
  }

  @Override
  public int read() throws IOException {
    if (-1 == read(mBuf, 0, 1)) {
      return -1;
    }
    if (mBuf[0] < 0) {
      return 256 + (int) mBuf[0];
    } else {
      return mBuf[0];
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > b.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    // ensure we're not past the end of the file
    if (mPos >= mFileLength) {
      return -1;
    }

    int ret = (int) mMount.read(mFileHandle, b, len, -1);
    if (ret < 0) {
      // attempt to reset to old position.
      mMount.lseek(mFileHandle, mPos, CephMount.SEEK_SET);
      throw new IOException("ceph.read: ret = " + ret);
    }
    mPos += ret;
    return ret;
  }

  @Override
  public void close() throws IOException {
    if (mClosed.getAndSet(true)) {
      return;
    }
    mMount.close(mFileHandle);
  }
}

