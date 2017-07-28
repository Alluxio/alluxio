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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A DataBuffer with the underlying data being a {@link FileChannel}.
 */
public final class DataFileChannel implements DataBuffer {
  private final File mFile;
  private final long mOffset;
  private final long mLength;

  /**
   *
   * @param file The file
   * @param offset The offset into the FileChannel
   * @param length The length of the data to read
   */
  public DataFileChannel(File file, long offset, long length) {
    mFile = Preconditions.checkNotNull(file, "file");
    mOffset = offset;
    mLength = length;
  }

  @Override
  public Object getNettyOutput() {
    return new DefaultFileRegion(mFile, mOffset, mLength);
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
  public int readableBytes() {
    throw new UnsupportedOperationException("DataFileChannel#readableBytes is not implemented.");
  }

  @Override
  public void release() {
    // Nothing we need to release explicitly, let GC take care of all objects.
  }
}
