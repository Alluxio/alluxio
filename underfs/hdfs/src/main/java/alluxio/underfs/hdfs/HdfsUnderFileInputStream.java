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

package alluxio.underfs.hdfs;

import alluxio.underfs.UnderFileInputStream;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * HDFS implementation for {@link UnderFileInputStream}.
 */
@NotThreadSafe
public class HdfsUnderFileInputStream extends UnderFileInputStream {

  /** The underlying stream to read data from. */
  private FSDataInputStream mStream;

  /**
   * Create a new instance of {@link HdfsUnderFileInputStream}.
   *
   * @param stream the wrapped input stream
   */
  public HdfsUnderFileInputStream(FSDataInputStream stream) {
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
    mStream.seek(position);
  }

  @Override
  public long skip(long n) throws IOException {
    return mStream.skip(n);
  }
}
