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

package alluxio.client.file.ufs;

import alluxio.client.file.FileOutStream;
import alluxio.exception.PreconditionMessage;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import java.io.IOException;
import java.io.OutputStream;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Supports writing to a UFS file directly.
 */
@ThreadSafe
public class UfsFileOutStream extends FileOutStream {
  /** Used to manage closeable resources. */
  private final Closer mCloser = Closer.create();
  private final OutputStream mUfsOutStream;

  /**
   * Creates a {@link UfsFileOutStream}.
   *
   * @param stream the embedded UFS output stream
   */
  public UfsFileOutStream(OutputStream stream) {
    mUfsOutStream = Preconditions.checkNotNull(stream);
    mCloser.register(mUfsOutStream);
  }

  @Override
  public synchronized long getBytesWritten() {
    return mBytesWritten;
  }

  @Override
  public synchronized void write(int b) throws IOException {
    mUfsOutStream.write(b);
    mBytesWritten++;
  }

  @Override
  public synchronized void write(byte[] b) throws IOException {
    Preconditions.checkArgument(b != null, PreconditionMessage.ERR_WRITE_BUFFER_NULL);
    mUfsOutStream.write(b);
    mBytesWritten += b.length;
  }

  @Override
  public synchronized void write(byte[] b, int off, int len) throws IOException {
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= b.length,
        PreconditionMessage.ERR_BUFFER_STATE.toString(), b.length, off, len);
    mUfsOutStream.write(b, off, len);
    mBytesWritten += len;
  }

  @Override
  public synchronized void cancel() {
    // no-op
  }

  @Override
  public synchronized void close() throws IOException {
    mCloser.close();
  }
}
