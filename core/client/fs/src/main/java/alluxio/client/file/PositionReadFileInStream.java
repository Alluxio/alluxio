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

import alluxio.PositionReader;
import alluxio.client.file.FileInStream;
import alluxio.exception.PreconditionMessage;

import com.amazonaws.annotation.NotThreadSafe;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Implementation of {@link FileInStream} that reads from a dora cache if possible.
 */
@NotThreadSafe
public class PositionReadFileInStream extends FileInStream {
  private final long mLength;
  private long mPos = 0;
  private boolean mClosed;
  private final PositionReader mPositionReader;

  /**
   * Constructor.
   * @param reader
   * @param length
   */
  public PositionReadFileInStream(PositionReader reader,
      long length) {
    mPositionReader = reader;
    mLength = length;
  }

  @Override
  public long remaining() {
    return mLength - mPos;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Objects.requireNonNull(b, "Read buffer cannot be null");
    return read(ByteBuffer.wrap(b), off, len);
  }

  @Override
  public int read(ByteBuffer byteBuffer, int off, int len) throws IOException {
    int bytesRead = mPositionReader.read(mPos, byteBuffer, len);
    if (bytesRead <= 0) {
      return bytesRead;
    }
    mPos += bytesRead;
    return bytesRead;
  }

  @Override
  public int positionedRead(long position, byte[] buffer, int offset, int length)
      throws IOException {
    return mPositionReader.read(position, buffer, offset, length);
  }

  @Override
  public long getPos() throws IOException {
    return mPos;
  }

  @Override
  public void seek(long pos) throws IOException {
    Preconditions.checkState(!mClosed, "Cannot do operations on a closed BlockInStream");
    Preconditions.checkArgument(pos >= 0, PreconditionMessage.ERR_SEEK_NEGATIVE.toString(), pos);
    Preconditions.checkArgument(pos <= mLength,
        "Seek position past the end of the read region (block or file).");
    if (pos == mPos) {
      return;
    }
    mPos = pos;
  }

  @Override
  public long skip(long n) throws IOException {
    Preconditions.checkState(!mClosed, "Cannot do operations on a closed BlockInStream");
    if (n <= 0) {
      return 0;
    }
    long toSkip = Math.min(remaining(), n);
    seek(mPos + toSkip);
    return toSkip;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
    mPositionReader.close();
  }
}
