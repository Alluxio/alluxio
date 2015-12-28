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

package tachyon.client.file;

import java.io.OutputStream;

/**
 * An abstraction of the output stream API in Tachyon to write data to a file or a block. In
 * addition to extending abstract class {@link OutputStream} as the basics, it also keeps
 * counting the number of bytes written to the output stream.
 */
public abstract class AbstractCountingOutStream extends OutputStream {
  // TODO(binfan): make mBytesWritten long.
  /** The number of bytes written */
  protected int mBytesWritten = 0;

  /**
   * @return the number of bytes written to this stream
   */
  public int getBytesWritten() {
    return mBytesWritten;
  }
}
