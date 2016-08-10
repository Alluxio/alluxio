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

import alluxio.client.file.options.InStreamOptions;
import alluxio.wire.FileInfo;

import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Mock implementation of {@link FileInStream} which delegates to a {@link ByteArrayInputStream}.
 */
public final class MockFileInStream extends FileInStream {
  private final ByteArrayInputStream mStream;

  /**
   * Creates a mock {@link FileInStream} which will supply the given bytes.
   *
   * @param context file system context
   * @param bytes the bytes to supply
   */
  public MockFileInStream(FileSystemContext context, byte[] bytes) {
    super(new URIStatus(new FileInfo()), InStreamOptions.defaults(), context);
    mStream = new ByteArrayInputStream(bytes);
  }

  @Override
  public int read() {
    return mStream.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return mStream.read(b);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return mStream.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    mStream.close();
  }

  @Override
  public long skip(long n) {
    return mStream.skip(n);
  }

  @Override
  public void seek(long n) {
    throw new UnsupportedOperationException("seek not implemented for mock FileInStream");
  }

  @Override
  public long remaining() {
    return mStream.available();
  }
}
