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

import alluxio.client.block.stream.DataReader;
import alluxio.client.block.stream.GrpcDataReader;
import alluxio.client.file.FileInStream;
import alluxio.exception.PreconditionMessage;
import alluxio.exception.status.OutOfRangeException;
import alluxio.network.protocol.databuffer.DataBuffer;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Implementation of {@link FileInStream} that reads from a dora cache if possible.
 */
public class DoraCacheFileInStream extends FileInStream {

  private final GrpcDataReader.Factory mGrpcReaderFactory;
  private final long mLength;

  private long mPos = 0;
  private boolean mClosed;
  private DataReader mDataReader;
  private DataBuffer mCurrentChunk;

  /**
   * Constructor.
   * @param grpcReaderFactory
   * @param length
   */
  public DoraCacheFileInStream(GrpcDataReader.Factory grpcReaderFactory,
      long length) {
    mGrpcReaderFactory = grpcReaderFactory;
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
    Preconditions.checkArgument(off >= 0 && len >= 0 && len + off <= byteBuffer.capacity(),
        PreconditionMessage.ERR_BUFFER_STATE.toString(), byteBuffer.capacity(), off, len);
    Preconditions.checkState(!mClosed, "Cannot do operations on a closed BlockInStream");
    if (len == 0) {
      return 0;
    }
    if (mPos == mLength) {
      return -1;
    }
    readChunk();
    if (mCurrentChunk == null) {
      closeDataReader();
      if (mPos < mLength) {
        throw new OutOfRangeException(String.format("Block %s is expected to be %s bytes, "
                + "but only %s bytes are available in the UFS. "
                + "Please retry the read and on the next access, "
                + "Alluxio will sync with the UFS and fetch the updated file content.",
             mLength, mPos));
      }
      return -1;
    }
    int toRead = Math.min(len, mCurrentChunk.readableBytes());
    byteBuffer.position(off).limit(off + toRead);
    mCurrentChunk.readBytes(byteBuffer);
    mPos += toRead;
    if (mPos == mLength) {
      // a performance improvement introduced by https://github.com/Alluxio/alluxio/issues/14020
      closeDataReader();
    }
    return toRead;
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

    int totalBytesRead = 0;
    try (DataReader reader = mGrpcReaderFactory.create(position, length)) {
      while (totalBytesRead < length) {
        DataBuffer dataBuffer = null;
        try {
          dataBuffer = reader.readChunk();
          if (dataBuffer == null) {
            break;
          }
          int bytesRead = dataBuffer.readableBytes();
          dataBuffer.readBytes(buffer, offset, bytesRead);
          totalBytesRead += bytesRead;
          offset += bytesRead;
        } finally {
          if (dataBuffer != null) {
            dataBuffer.release();
          }
        }
      }
    }
    if (totalBytesRead == 0) {
      return -1;
    }
    return totalBytesRead;
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
    closeDataReader();
    mPos = pos;
  }

  @Override
  public long skip(long n) throws IOException {
    Preconditions.checkState(!mClosed, "Cannot do operations on a closed BlockInStream");
    if (n <= 0) {
      return 0;
    }
    long toSkip = Math.min(remaining(), n);
    mPos += toSkip;
    closeDataReader();
    return toSkip;
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      closeDataReader();
    } finally {
      mGrpcReaderFactory.close();
    }
    mClosed = true;
  }

  /**
   * Reads a new chunk from the channel if all of the current chunk is read.
   */
  private void readChunk() throws IOException {
    if (mDataReader == null) {
      mDataReader = mGrpcReaderFactory.create(mPos, mLength - mPos);
    }

    if (mCurrentChunk != null && mCurrentChunk.readableBytes() == 0) {
      mCurrentChunk.release();
      mCurrentChunk = null;
    }
    if (mCurrentChunk == null) {
      mCurrentChunk = mDataReader.readChunk();
    }
  }

  private void closeDataReader() throws IOException {
    if (mCurrentChunk != null) {
      mCurrentChunk.release();
      mCurrentChunk = null;
    }
    if (mDataReader != null) {
      mDataReader.close();
    }
    mDataReader = null;
  }
}
