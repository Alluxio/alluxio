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

package alluxio.client.file.dora;

import alluxio.client.file.FileInStream;
import alluxio.client.file.dora.netty.PartialReadException;
import alluxio.exception.PreconditionMessage;
import alluxio.util.io.ChannelAdapters;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Implementation of {@link FileInStream} that reads from a dora cache if possible.
 */
public class DoraCacheFileInStream extends FileInStream {
  private final long mLength;
  private long mPos = 0;
  private boolean mClosed;
  private final DoraDataReader mDataReader;

  /**
   * Constructor.
   * @param reader
   * @param length
   */
  public DoraCacheFileInStream(DoraDataReader reader,
      long length) {
    mDataReader = reader;
    mLength = length;
  }

  @Override
  public long remaining() {
    return mLength - mPos;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    Objects.requireNonNull(b, "Read buffer cannot be null");
    return read(ByteBuffer.wrap(b, off, len));
  }

  @Override
  public int read(ByteBuffer byteBuffer) throws IOException {
    Preconditions.checkState(!mClosed, "stream closed");
    int len = byteBuffer.remaining();
    if (len == 0) {
      return 0;
    }
    if (mPos == mLength) {
      return -1;
    }
    int bytesRead = 0;
    try {
      bytesRead = mDataReader.read(mPos, ChannelAdapters.intoByteBuffer(byteBuffer), len);
    } catch (PartialReadException e) {
      bytesRead = e.getBytesRead();
      if (bytesRead == 0) {
        // we didn't make any progress, throw the exception so that the caller needs to handle that
        Throwables.propagateIfPossible(e.getCause(), IOException.class);
        throw new IOException(e.getCause());
      }
      // otherwise ignore the exception and let the caller decide whether to continue
    } finally {
      if (bytesRead > 0) { // -1 indicates EOF
        mPos += bytesRead;
      }
    }

    return bytesRead;
  }

  @Override
  public int positionedRead(long position, byte[] buffer, int offset, int length)
      throws IOException {
    if (length == 0) {
      return 0;
    }
    if (position < 0 || position >= mLength) {
      return -1;
    }
    try {
      mDataReader.readFully(position,
          ChannelAdapters.intoByteArray(buffer, offset, length), length);
    } catch (PartialReadException e) {
      if (e.getCauseType() == PartialReadException.CauseType.EARLY_EOF) {
        if (e.getBytesRead() > 0) {
          return e.getBytesRead();
        } else {
          return -1;
        }
      }
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new IOException(e.getCause());
    }
    return length;
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
    mDataReader.close();
  }
}
