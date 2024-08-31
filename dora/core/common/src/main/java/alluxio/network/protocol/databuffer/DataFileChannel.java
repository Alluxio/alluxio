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

package alluxio.network.protocol.databuffer;

import com.google.common.base.Preconditions;
import io.netty.channel.DefaultFileRegion;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A DataBuffer with the underlying data being a {@link FileChannel}.
 */
public final class DataFileChannel implements DataBuffer {
  public final RandomAccessFile mFile;
  private final long mOffset;
  private final long mLength;
  public final long mPageIndex;

  /**
   *
   * @param file The file
   * @param offset The offset into the FileChannel
   * @param length The length of the data to read
   */
  public DataFileChannel(long pageIndex, RandomAccessFile file, long offset, long length) {
    mFile = Preconditions.checkNotNull(file, "file");
    try {
      mFile.seek(offset);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    mOffset = offset;
    mLength = length;
    mPageIndex = pageIndex;
  }

  @Override
  public Object getNettyOutput() {
    return new DefaultFileRegion(mFile.getChannel(), mOffset, mLength);
  }

  @Override
  public long getLength() {
    return mLength;
  }

  @Override
  public ByteBuffer getReadOnlyByteBuffer() {
    throw new UnsupportedOperationException(
        "DataFileChannel#getReadOnlyByteBuffer is not implemented.");
  }

  @Override
  public void readBytes(byte[] dst, int dstIndex, int length) {
    throw new UnsupportedOperationException("DataFileChannel#readBytes is not implemented.");
  }

  @Override
  public void readBytes(OutputStream outputStream, int length) throws IOException {
    throw new UnsupportedOperationException("DataFileChannel#readBytes is not implemented.");
  }

  @Override
  public void readBytes(ByteBuffer outputBuf) {
    throw new UnsupportedOperationException("DataFileChannel#readBytes is not implemented.");
  }

  @Override
  public int readableBytes() {
    int lengthInt = (int) mLength;
    Preconditions.checkArgument(mLength == (long) lengthInt,
        "size of file %s is %s, cannot be cast to int", mFile, mLength);
    return lengthInt;
  }

  @Override
  public void release() {
    // Nothing we need to release explicitly, let GC take care of all objects.
  }
}
