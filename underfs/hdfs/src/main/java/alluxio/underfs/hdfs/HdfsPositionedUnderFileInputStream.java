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

import alluxio.underfs.SeekableUnderFileInputStream;
import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;

/**
 * The input stream of HDFS as under filesystem. This input stream supports seeking but internally
 * uses the positionedRead {@link FSDataInputStream} API. This stream can be cached for reuse.
 */
public class HdfsPositionedUnderFileInputStream extends SeekableUnderFileInputStream {
  private long mPos;

  HdfsPositionedUnderFileInputStream(FSDataInputStream in, long pos) {
    super(in);
    mPos = pos;
  }

  @Override
  public int available() throws IOException {
    if (mPos != ((FSDataInputStream) in).getPos()) {
      return 0;
    }
    return in.available();
  }

  @Override
  public long getPos() throws IOException {
    return mPos;
  }

  @Override
  public int read() throws IOException {
    byte[] buffer = new byte[1];
    int bytesRead = read(buffer);
    if (bytesRead > 0) {
      return BufferUtils.byteToInt(buffer[0]);
    }
    Preconditions.checkArgument(bytesRead != 0,
        "Expected a non-zero value if end of stream has not been reached");
    return bytesRead;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] buffer, int offset, int length) throws IOException {
    int bytesRead = ((FSDataInputStream) in).read(mPos, buffer, offset, length);
    if (bytesRead > 0) {
      mPos += bytesRead;
    }
    return bytesRead;
  }

  @Override
  public void seek(long position) throws IOException {
    mPos = position;
  }

  @Override
  public long skip(long n) throws IOException {
    mPos += n;
    return n;
  }

  @Override
  public boolean markSupported() {
    return false;
  }
}
