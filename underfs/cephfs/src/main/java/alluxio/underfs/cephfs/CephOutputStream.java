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

import java.io.IOException;
import java.io.OutputStream;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A stream for writing a file into CephFS. The data will be persisted to a temporary
 * directory on the local disk and copied as a complete file when the {@link #close()}
 * method is called.
 */
@NotThreadSafe
public final class CephOutputStream extends OutputStream {
  private CephMount mMount;
  private int mFileHandle;

  /** Flag to indicate this stream has been closed, to ensure close is only done once. */
  private AtomicBoolean mClosed = new AtomicBoolean(false);

  /**
   * Construct the CephOutputStream.
   *
   * @param mount CephFileSystem mount
   * @param fh The Ceph filehandle to connect to
   */
  public CephOutputStream(CephMount mount, int fh) {
    mMount = mount;
    mFileHandle = fh;
  }

  @Override
  public void write(int b) throws IOException {
    byte[] buf = new byte[1];
    buf[0] = (byte) b;
    write(buf, 0, 1);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (b == null) {
      throw new IOException("ceph.write: data buffer is null pointer");
    } else if ((off < 0) || (off > b.length) || (len < 0)
        || ((off + len) > b.length) || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    if (off == 0) {
      int ret = (int) mMount.write(mFileHandle, b, len, -1);
      if (ret < 0) {
        throw new IOException("ceph.write: ret = " + ret);
      }
    } else {
      byte[] sub = new byte[len];
      System.arraycopy(b, off, sub, 0, len);
      int ret = (int) mMount.write(mFileHandle, sub, len, -1);
      if (ret < 0) {
        throw new IOException("ceph.write: ret = " + ret);
      }
    }
  }

  @Override
  public void flush() throws IOException {
    mMount.fsync(mFileHandle, false);
  }

  @Override
  public void close() throws IOException {
    if (mClosed.getAndSet(true)) {
      return;
    }
    flush();
    mMount.close(mFileHandle);
  }
}

