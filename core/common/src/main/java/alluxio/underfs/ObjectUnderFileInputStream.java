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

import alluxio.exception.ExceptionMessage;
import alluxio.underfs.options.OpenOptions;

import com.google.common.io.CountingInputStream;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A default implementation for {@link UnderFileInputStream}.
 */
@NotThreadSafe
public class ObjectUnderFileInputStream extends UnderFileInputStream {

  /** The initial position of the stream. */
  private long mInitPos;

  /** UFS to open a file. */
  private ObjectUnderFileSystem mUfs;

  /** Key of file in UFS. */
  private String mKey;

  /** The underlying stream to read data from. */
  private CountingInputStream mStream;

  /**
   * Create a new instance of {@link ObjectUnderFileInputStream}.
   *
   * @param ufs Object UFS for input stream
   * @param key key in the underlying object store
   * @param options to open to stream
   * @throws IOException when a non-Alluxio error occurs
   */
  public ObjectUnderFileInputStream(ObjectUnderFileSystem ufs, String key, OpenOptions options)
      throws IOException {
    mUfs = ufs;
    mKey = key;
    openStream(options);
  }

  @Override
  public void close() throws IOException {
    mStream.close();
  }

  @Override
  public int read() throws IOException {
    return mStream.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return mStream.read(b, off, len);
  }

  @Override
  public void seek(long position) throws IOException {
    long currentPos = mInitPos + mStream.getCount();
    if (position > currentPos) {
      long toSkip = position - currentPos;
      if (toSkip != skip(toSkip)) {
        throw new IOException(ExceptionMessage.FAILED_SKIP.getMessage(toSkip));
      }
    } else {
      openStream(OpenOptions.defaults().setOffset(position));
    }
  }

  @Override
  public long skip(long n) throws IOException {
    return mStream.skip(n);
  }

  /**
   * Open a new stream.
   *
   * @param options for opening a stream
   * @throws IOException if a non-Alluxio error occurs
   */
  private void openStream(OpenOptions options) throws IOException {
    if (mStream != null) {
      mStream.close();
    }
    mInitPos = options.getOffset();
    mStream = new CountingInputStream(mUfs.openObject(mKey, options));
  }
}
