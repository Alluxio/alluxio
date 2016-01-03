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
 * A mock implementation of {@link AbstractCountingOutStream} backed by a byte stream. Supposed to
 * be only used for tests.
 *
 * TODO(binfan): move this to test jar if it turns out only used in test.
 */
public final class ByteArrayCountingOutStream extends AbstractCountingOutStream {
  private ByteArrayOutputStream mOut;

  public ByteArrayCountingOutStream() {
    /** set an initial size of underlying ByteArray, which will grow automatically */
    mOut = new ByteArrayOutputStream(1000);
  }

  public void write(int b) {
    mOut.write(b);
    mBytesWritten ++;
  }

  /**
   * @return the number of bytes written to this output stream
   */
  public int getBytesWritten() {
    return mBytesWritten;
  }

  /**
   * @return a newly created byte array for the output stream
   */
  public byte[] toByteArray() {
    return mOut.toByteArray();
  }
}
