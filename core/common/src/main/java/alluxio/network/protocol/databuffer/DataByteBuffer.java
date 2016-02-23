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
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;

/**
 * A DataBuffer with the underlying data being a {@link ByteBuffer}.
 */
public final class DataByteBuffer implements DataBuffer {
  private final ByteBuffer mBuffer;
  private final long mLength;

  /**
   *
   * @param buffer The ByteBuffer representing the data
   * @param length The length of the ByteBuffer
   */
  public DataByteBuffer(ByteBuffer buffer, long length) {
    mBuffer = Preconditions.checkNotNull(buffer);
    mLength = length;
  }

  @Override
  public Object getNettyOutput() {
    return Unpooled.wrappedBuffer(mBuffer);
  }

  @Override
  public long getLength() {
    return mLength;
  }

  @Override
  public ByteBuffer getReadOnlyByteBuffer() {
    ByteBuffer buffer = mBuffer.asReadOnlyBuffer();
    buffer.position(0);
    return buffer;
  }

  @Override
  public void release() {
    // Nothing we need to release explicitly, let GC take care of all objects.
  }
}
