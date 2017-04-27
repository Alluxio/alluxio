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

package alluxio.fuse;

import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Convenience class to encapsulate input/output streams of open alluxio files.
 *
 * An open file can be either write-only or read-only, never both. This means that one of getIn or
 * getOut will be null, while the other will be non-null. It is up to the user of this class
 * (currently, only {@link AlluxioFuseFileSystem}) to check that.
 *
 * This mechanism is preferred over more complex sub-classing to avoid useless casts or type checks
 * for every read/write call, which happen quite often.
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
   * @return an opened input stream for the open alluxio file, or null
   */
  public FileInStream getIn() {
    return mIn;
  }

  /**
   * Gets the opened output stream for this open file entry. The value returned can be {@code null}
   * if the file is not open for writing.
   *
   * @return an opened input stream for the open alluxio file, or null
   */
  public FileOutStream getOut() {
    return mOut;
  }

  /**
   * Closes the underlying open streams.
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
