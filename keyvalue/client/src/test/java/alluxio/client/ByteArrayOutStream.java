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

package alluxio.client;

import java.io.ByteArrayOutputStream;

/**
 * A mock implementation of {@link AbstractOutStream} backed by a byte stream. Supposed to
 * be only used for tests.
 */
public final class ByteArrayOutStream extends AbstractOutStream {
  private ByteArrayOutputStream mOut;
  private boolean mClosed;
  private boolean mCanceled;

  public ByteArrayOutStream() {
    /** set an initial size of underlying ByteArray, which will grow automatically */
    mOut = new ByteArrayOutputStream(1000);
    mClosed = false;
    mCanceled = false;
  }

  @Override
  public void write(int b) {
    mOut.write(b);
    mBytesWritten++;
  }

  @Override
  public int getBytesWritten() {
    return mBytesWritten;
  }

  @Override
  public void close() {
    mClosed = true;
  }

  @Override
  public void cancel() {
    mCanceled = true;
    close();
  }

  /**
   * @return whether this output stream is closed
   */
  public boolean isClosed() {
    return mClosed;
  }

  /**
   * @return whether this output stream is canceled
   */
  public boolean isCanceled() {
    return mCanceled;
  }

  /**
   * @return a newly created byte array for the output stream
   */
  public byte[] toByteArray() {
    return mOut.toByteArray();
  }
}
