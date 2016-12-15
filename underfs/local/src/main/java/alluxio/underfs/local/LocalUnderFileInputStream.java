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

package alluxio.underfs.local;

import alluxio.Seekable;
import alluxio.exception.ExceptionMessage;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * HDFS implementation for {@link UnderFileInputStream}.
 */
@NotThreadSafe
public class LocalUnderFileInputStream extends InputStream implements Seekable {

  /** The underlying stream to read data from. */
  private FileInputStream mStream;

  /**
   * Creates a new instance of {@link LocalUnderFileInputStream}.
   *
   * @param stream the wrapped input stream
   */
  public LocalUnderFileInputStream(FileInputStream stream) {
    mStream = stream;
  }

  @Override
  public void close() throws IOException {
    mStream.close();
  }

  @Override
  public int read() throws IOException {
    return mStream.read();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return mStream.read(b, off, len);
  }

  @Override
  public void seek(long position) throws IOException {
    FileChannel channel = mStream.getChannel();
    if (position > channel.size()) {
      throw new IOException(ExceptionMessage.FAILED_SEEK.getMessage(position));
    }
    channel.position(position);
  }

  @Override
  public long skip(long n) throws IOException {
    return mStream.skip(n);
  }
}
