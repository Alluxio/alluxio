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

import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Output stream implementation for {@link HdfsUnderFileSystem}. This class is just a wrapper on top
 * of an underlying {@link FSDataOutputStream}, except all calls to {@link #flush()} will be
 * converted to {@link FSDataOutputStream#sync()}. This is currently safe because all invocations of
 * flush intend the functionality to be sync.
 */
@NotThreadSafe
public class HdfsUnderFileOutputStream extends OutputStream {
  /** Underlying output stream. */
  private final FSDataOutputStream mOut;

  /**
   * Basic constructor.
   *
   * @param out underlying stream to wrap
   */
  public HdfsUnderFileOutputStream(FSDataOutputStream out) {
    mOut = out;
  }

  @Override
  public void close() throws IOException {
    mOut.close();
  }

  @Override
  public void flush() throws IOException {
    // TODO(calvin): This functionality should be restricted to select output streams.
    //#ifdef HADOOP1
    mOut.sync();
    //#else
    // Note that, hsync() flushes out the data in client's user buffer all the way to the disk
    // device which may result in much slower performance than sync().
    mOut.hsync();
    //#endif
  }

  @Override
  public void write(int b) throws IOException {
    mOut.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    mOut.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mOut.write(b, off, len);
  }
}
