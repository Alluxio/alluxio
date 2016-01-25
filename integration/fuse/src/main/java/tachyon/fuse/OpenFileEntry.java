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

package tachyon.fuse;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import tachyon.client.file.FileInStream;
import tachyon.client.file.FileOutStream;

/**
 * Convenience class to encapsulate input/output streams
 * of open tachyon files.
 *
 * An open file can be either write-only or read-only, never both.
 * This means that one of getIn or getOut will be null, while the other will be non-null.
 * It is up to the user of this class (currently, only TachyonFuseFs) to check that.
 *
 * This mechanism is preferred over more complex subclassing to avoid useless casts
 * or type checks for every read/write call, which happen quite often.
 */
@NotThreadSafe
final class OpenFileEntry implements Closeable {
  private final FileInStream mIn;
  private final FileOutStream mOut;

  public OpenFileEntry(FileInStream in, FileOutStream out) {
    mIn = in;
    mOut = out;
  }

  /**
   * Gets the opened input stream for this open file entry. The value returned can be {@code null}
   * if the file is not open for reading.
   *
   * @return an opened input stream for the open tachyon file, or null
   */
  public FileInStream getIn() {
    return mIn;
  }

  /**
   * Gets the opened output stream for this open file entry. The value returned can be {@code null}
   * if the file is not open for writing.
   *
   * @return an opened input stream for the open tachyon file, or null
   */
  public FileOutStream getOut() {
    return mOut;
  }

  /**
   * Closes the underlying open streams.
   *
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (mIn != null) {
      mIn.close();
    }

    if (mOut != null) {
      mOut.close();
    }
  }
}
