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

  @Override
  public void readBytes(byte[] dst, int dstIndex, int length) {
    throw new UnsupportedOperationException("DataNettyBuffer#readBytes is not implemented.");
  }

  @Override
  public int readableBytes() {
    throw new UnsupportedOperationException("DataNettyBuffer#readBytes is not implemented.");
  }

  /**
   * Release the Netty ByteBuf.
   */
  @Override
  public void release() {
    Preconditions.checkState(mNettyBuf != null);
    Preconditions.checkState(mNettyBuf.refCnt() == 1,
        "Reference count of the netty buffer is %s (1 expected).", mNettyBuf.refCnt());
    Preconditions.checkState(mNettyBuf.release(), "Release Netty ByteBuf failed.");
  }
}
