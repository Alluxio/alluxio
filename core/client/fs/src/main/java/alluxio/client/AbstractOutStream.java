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

package alluxio.client;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * An abstraction of the output stream API in Alluxio to write data to a file or a block. In
 * addition to extending abstract class {@link OutputStream} as the basics, it also keeps counting
 * the number of bytes written to the output stream, and extends {@link Cancelable} to abort the
 * writes.
 */
@NotThreadSafe
public abstract class AbstractOutStream extends OutputStream implements Cancelable {
  /** The number of bytes written. */
  protected long mBytesWritten = 0;

  /**
   * @return the number of bytes written to this stream
   * @deprecated this method will not work if more than MAX_INT bytes are written; to get a count of
   *             bytes written, wrap this stream in a counting output stream such as
   *             org.apache.commons.io.output.CountingOutputStream
   */
  @Deprecated
  public int getBytesWritten() {
    // Cast to int to preserve backwards compatibility. In 2.0 we should change this to long so that
    // it can be correct.
    return (int) mBytesWritten;
  }

  /**
   * Aborts the output stream.
   */
  @Override
  public void cancel() throws IOException {}
}
