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

import alluxio.Seekable;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.FilterInputStream;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Input stream implementation for {@link HdfsUnderFileSystem}.
 */
@NotThreadSafe
public class HdfsUnderFileInputStream extends FilterInputStream implements Seekable {

  /** The underlying stream to read data from. */
  private FSDataInputStream mStream;

  /**
   * Creates a new instance of {@link HdfsUnderFileInputStream}.
   *
   * @param stream the wrapped input stream
   */
  public HdfsUnderFileInputStream(FSDataInputStream stream) {
    super(stream);
    mStream = stream;
  }

  @Override
  public void seek(long position) throws IOException {
    mStream.seek(position);
  }
}
