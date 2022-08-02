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

import static alluxio.underfs.hdfs.HdfsUnderFileSystem.tryHdfsCall;

import alluxio.underfs.SeekableUnderFileInputStream;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 * The input stream of HDFS as under filesystem. This input stream supports seeking and can be
 * cached for reuse.
 */
public class HdfsUnderFileInputStream extends SeekableUnderFileInputStream {

  HdfsUnderFileInputStream(FSDataInputStream in) {
    super(in);
  }

  @Override
  public void seek(long position) {
    tryHdfsCall(() -> {
      ((FSDataInputStream) in).seek(position);
      return null;
    });
  }

  @Override
  public long getPos() throws IOException {
    return ((FSDataInputStream) in).getPos();
  }

  @Override
  public int read(byte[] b, int off, int len) {
    return tryHdfsCall(() -> in.read(b, off, len));
  }
}
