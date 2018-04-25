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

package alluxio.master.journal.ufs;

import java.io.IOException;
import java.io.OutputStream;

/** An output stream that outputs to nowhere. */
public final class NoopOutputStream extends OutputStream {
  public static NoopOutputStream INSTANCE = new NoopOutputStream();

  private NoopOutputStream() {}

  @Override
  public void write(int b) throws IOException {
    return;
  }

  @Override
  public void write(byte[] b) throws IOException {
    return;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    return;
  }

  @Override
  public void flush() throws IOException {
    return;
  }

  @Override
  public void close() throws IOException {
    return;
  }
}
