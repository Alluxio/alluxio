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

import javax.annotation.concurrent.NotThreadSafe;
import java.io.FilterOutputStream;
import java.io.IOException;

/**
 * Output stream implementation for {@link HdfsUnderFileSystem}. This class is just a wrapper on top
 * of an underlying {@link FSDataOutputStream}, except all calls to {@link #flush()} will be
 * converted to {@link FSDataOutputStream#sync()}. This is currently safe because all invocations of
 * flush intend the functionality to be sync.
 */
@NotThreadSafe
public class HdfsUnderFileOutputStream extends FilterOutputStream {
  /** Underlying output stream. */
  private final FSDataOutputStream mOut;

  /**
   * Basic constructor.
   *
   * @param out underlying stream to wrap
   */
  public HdfsUnderFileOutputStream(FSDataOutputStream out) {
    super(out);
    mOut = out;
  }

  @Override
  public void flush() throws IOException {
    // TODO(calvin): This functionality should be restricted to select output streams.
    mOut.sync();
  }
}
