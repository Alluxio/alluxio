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

import java.nio.ByteBuffer;

import com.google.common.base.Preconditions;

import io.netty.buffer.Unpooled;

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
