/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.network.protocol.databuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.google.common.base.Preconditions;

import io.netty.channel.DefaultFileRegion;

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
