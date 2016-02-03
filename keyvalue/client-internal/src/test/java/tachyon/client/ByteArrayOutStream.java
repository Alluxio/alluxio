/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client;

import java.io.ByteArrayOutputStream;

/**
 * A mock implementation of {@link OutStreamBase} backed by a byte stream. Supposed to
 * be only used for tests.
 */
public final class ByteArrayOutStream extends OutStreamBase {
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
    mBytesWritten ++;
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
