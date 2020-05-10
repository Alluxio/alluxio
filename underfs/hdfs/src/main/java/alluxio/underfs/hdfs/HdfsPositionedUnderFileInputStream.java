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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.IOException;

/**
 * The input stream of HDFS as under filesystem. This input stream has two mode of operations.
 * Under sequential mode, it uses the read api and can take advantage of underlying stream's
 * buffering. Under random read mode, it uses the positionedRead {@link FSDataInputStream} API.
 * This stream can be cached for reuse.
 */
public class HdfsPositionedUnderFileInputStream extends SeekableUnderFileInputStream {
  // TODO(david): make these parameters configurations and add diagnostic metrics.
  // After this many number of sequential reads (reads without large skips), it
  // will switch to sequential read mode.
  @VisibleForTesting
  static final int SEQUENTIAL_READ_LIMIT = 3;
  // This describes the number of bytes that we can move forward in a stream without
  // switching to random read mode.
  @VisibleForTesting
  static final int MOVEMENT_LIMIT = 512;

  private long mPos;
  // The heuristic is that if there are certain number of sequential reads in a row,
  // we switch to buffered read mode. In this mode, preads are serviced via pread calls,
  // and read calls are services by read calls. A sequential read is defined as a read that
  // is within a movement limit of the previous read. This is to guard against workloads
  // such as read, skip(2), read, skip(3) etc.
  private int mSequentialReadCount;

  HdfsPositionedUnderFileInputStream(FSDataInputStream in, long pos) {
    super(in);
    mPos = pos;
    mSequentialReadCount = 0;
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

  /**
   * If there has been a number of sequential reads in a row,
   * we move to regular buffered reads.
   */
  @Override
  public int read(byte[] buffer, int offset, int length) throws IOException {
    int bytesRead;
    if (isSequentialReadMode() && mPos != ((Seekable) in).getPos()) {
      ((Seekable) in).seek(mPos);
    }
    if (mPos == ((Seekable) in).getPos()) {
      // same position, use buffered reads as default
      bytesRead = in.read(buffer, offset, length);
    } else {
      bytesRead = ((PositionedReadable) in).read(mPos, buffer, offset, length);
    }
    if (bytesRead > 0) {
      mPos += bytesRead;
      mSequentialReadCount++;
    }
    return bytesRead;
  }

  private boolean isSequentialReadMode() {
    return mSequentialReadCount >= SEQUENTIAL_READ_LIMIT;
  }

  @Override
  public void seek(long position) throws IOException {
    if (position < mPos || position - mPos > MOVEMENT_LIMIT) {
      mSequentialReadCount = 0;
    }
    mPos = position;
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }
    if (n > MOVEMENT_LIMIT) {
      mSequentialReadCount = 0;
    }
    mPos += n;
    return n;
  }

  @Override
  public boolean markSupported() {
    return false;
  }
}
