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

package alluxio.client.file;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Mock implementation of {@link FileOutStream} which delegates to a {@link ByteArrayOutputStream}.
 */
public final class MockFileOutStream extends FileOutStream {
  private final ByteArrayOutputStream mStream;

  /**
   * Creates a mock {@link FileOutputStream} which will store any bytes written to it for later
   * inspection during tests.
   */
  public MockFileOutStream(FileSystemContext fsContext) throws IOException {
    mStream = new ByteArrayOutputStream();
  }

  @Override
  public void cancel() {}

  @Override
  public void close() throws IOException {
    mStream.close();
  }

  @Override
  public void flush() throws IOException {
    mStream.flush();
  }

  @Override
  public void write(int b) {
    mStream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    mStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) {
    mStream.write(b, off, len);
  }

  /**
   * @return the bytes that have been written to the output stream
   */
  public byte[] toByteArray() {
    return mStream.toByteArray();
  }
}
