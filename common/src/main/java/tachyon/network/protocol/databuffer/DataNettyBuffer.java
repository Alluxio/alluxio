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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * A DataBuffer with the underlying data being a {@link ByteBuf}.
 */
public class DataNettyBuffer extends DataBuffer {
  private final ByteBuffer mBuffer;
  private final long mLength;
  private final ByteBuf mNettyBuf;

  /**
  * Constructor for creating a DataNettyBuffer, by passing a Netty ByteBuf.
  * This way we avoid one copy from ByteBuf to another ByteBuffer,
  * and making sure the buffer would not be recycled.
  * IMPORTANT: {@link #releaseBuffer()} must be called after
  * reading is finished. Otherwise the memory space for the ByteBuf might never be reclaimed.
  *
  * @param bytebuf The ByteBuf having the data
  * @param length The length of the underlying ByteBuffer data
  */
  public DataNettyBuffer(ByteBuf bytebuf, long length) {
    // throws exception if there are multiple nioBuffers, or reference count is not 1
    // we probably want to fail instead of catching these exceptions for now
    assert (bytebuf.nioBufferCount() == 1);
    assert (bytebuf.refCnt() == 1);

    // increase the bytebuf reference count so it would not be recycled by Netty
    bytebuf.retain();
    mNettyBuf = bytebuf;
    mBuffer = bytebuf.nioBuffer();
    mLength = length;
  }

  /**
   * We would not support this method in DataNettyBuffer because this class is only for
   * reading netty buffers.
   * TODO: investigate if using NettyDataBuffer for outgoing message is fine
   *
   * @throws UnsupportedOperationException whenever called
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
  * Release the Netty ByteBuf if we used ByteBuf to construct this DataByteBuffer.
  *
  * @return True if the netty ByteBuf is released.
  *         As the Netty channel is responsible for performing another {@link ByteBuf#release()},
  *         this method can return false in unit tests.
  */
  @Override
  public boolean release() {
    if (mNettyBuf != null) {
      return mNettyBuf.release();
    }
    return true;
  }
}
