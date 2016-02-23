/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * A DataBuffer with the underlying data being a {@link FileChannel}.
 */
public final class DataFileChannel implements DataBuffer {
  private final FileChannel mFileChannel;
  private final long mOffset;
  private final long mLength;

  /**
   *
   * @param fileChannel The FileChannel representing the data
   * @param offset The offset into the FileChannel
   * @param length The length of the data to read
   */
  public DataFileChannel(FileChannel fileChannel, long offset, long length) {
    mFileChannel = Preconditions.checkNotNull(fileChannel);
    mOffset = offset;
    mLength = length;
  }

  @Override
  public Object getNettyOutput() {
    return new DefaultFileRegion(mFileChannel, mOffset, mLength);
  }

  @Override
  public long getLength() {
    return mLength;
  }

  @Override
  public ByteBuffer getReadOnlyByteBuffer() {
    ByteBuffer buffer = ByteBuffer.allocate((int) mLength);
    try {
      mFileChannel.position(mOffset);
      int bytesRead = 0;
      long bytesRemaining = mLength;
      while (bytesRemaining > 0 && (bytesRead = mFileChannel.read(buffer)) >= 0) {
        bytesRemaining -= bytesRead;
      }
    } catch (IOException e) {
      return null;
    }
    ByteBuffer readOnly = buffer.asReadOnlyBuffer();
    readOnly.position(0);
    return readOnly;
  }

  @Override
  public void release() {
    // Nothing we need to release explicitly, let GC take care of all objects.
  }
}
