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

package alluxio.client;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import alluxio.annotation.PublicApi;

/**
 * An abstraction of the output stream API in Alluxio to write data to a file or a block. In
 * addition to extending abstract class {@link OutputStream} as the basics, it also keeps counting
 * the number of bytes written to the output stream, and extends {@link Cancelable} to abort the
 * writes.
 */
@PublicApi
@NotThreadSafe
public abstract class OutStreamBase extends OutputStream implements Cancelable {
  // TODO(binfan): make mBytesWritten long so we could make offset > 2GB. This
  // requires checking the codebase for this assumption (using int to denote an offset).
  // See ALLUXIO-1536.
  /** The number of bytes written */
  protected int mBytesWritten = 0;

  /**
   * @return the number of bytes written to this stream
   */
  public int getBytesWritten() {
    return mBytesWritten;
  }

  /**
   * Aborts the output stream.
   *
   * @throws IOException if there is a failure to abort this output stream
   */
  @Override
  public void cancel() throws IOException {}
}
