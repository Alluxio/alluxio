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

package alluxio.network.protocol.databuffer;

import com.google.common.base.Preconditions;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * A DataBuffer with the underlying data being a {@link ByteBuf}.
 */
public final class DataNettyBuffer implements DataBuffer {
  private final ByteBuffer mBuffer;
  private final long mLength;
  private final ByteBuf mNettyBuf;

  /**
  * Constructor for creating a DataNettyBuffer, by passing a Netty ByteBuf.
  * This way we avoid one copy from ByteBuf to another ByteBuffer,
  * and making sure the buffer would not be recycled.
  * IMPORTANT: {@link #release()} must be called after
  * reading is finished. Otherwise the memory space for the ByteBuf might never be reclaimed.
  *
  * @param bytebuf The ByteBuf having the data
  * @param length The length of the underlying ByteBuffer data
  */
  public DataNettyBuffer(ByteBuf bytebuf, long length) {
    // throws exception if there are multiple nioBuffers, or reference count is not 1
    Preconditions.checkArgument(bytebuf.nioBufferCount() == 1,
        "Number of nioBuffers of this bytebuf is %s (1 expected).", bytebuf.nioBufferCount());
    Preconditions.checkArgument(bytebuf.refCnt() == 1,
        "Reference count of this bytebuf is %s (1 expected).", bytebuf.refCnt());

    // increase the bytebuf reference count so it would not be recycled by Netty
    bytebuf.retain();
    mNettyBuf = bytebuf;
    mBuffer = bytebuf.nioBuffer();
    mLength = length;
  }

  /**
   * We would not support this method in DataNettyBuffer because this class is only for
   * reading netty buffers. Throws an {@link UnsupportedOperationException} whenever called.
   * TODO(qifan): Investigate if using NettyDataBuffer for outgoing message is fine.
   *
   * @return {@link UnsupportedOperationException} whenever called
   */
  @Override
  public Object getNettyOutput() {
    throw new UnsupportedOperationException("DataNettyBuffer doesn't support getNettyOutput()");
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

  /**
   * Release the Netty ByteBuf.
   */
  @Override
  public void release() {
    Preconditions.checkState(mNettyBuf != null);
    Preconditions.checkState(mNettyBuf.refCnt() == 1,
        "Reference count of the netty buffer is %s (1 expected).", mNettyBuf.refCnt());
    Preconditions.checkState(mNettyBuf.release() == true, "Release Netty ByteBuf failed.");
  }
}
