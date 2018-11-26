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
 * A DataBuffer with the underlying data being a {@link ByteBuf}. This is a new version of
 * {@link DataNettyBuffer}.
 */
 // TODO(peis): Deprecate {@link DataNettyBuffer}.
public final class DataNettyBufferV2 implements DataBuffer {
  private final ByteBuf mNettyBuf;

  /**
  * Constructor for creating a DataNettyBuffer, by passing a Netty ByteBuf.
  *
  * @param bytebuf The ByteBuf having the data
  */
  public DataNettyBufferV2(ByteBuf bytebuf) {
    Preconditions.checkNotNull(bytebuf, "bytebuf");
    mNettyBuf = bytebuf;
  }

  /**
   * @return the netty buffer
   */
  @Override
  public Object getNettyOutput() {
    return mNettyBuf;
  }

  @Override
  public long getLength() {
    return mNettyBuf.readableBytes();
  }

  @Override
  public ByteBuffer getReadOnlyByteBuffer() {
    throw new RuntimeException("DataNettyBufferV2::getReadOnlyByteBuffer is not supported.");
  }

  @Override
  public void readBytes(byte[] dst, int dstIndex, int length) {
    mNettyBuf.readBytes(dst, dstIndex, length);
  }

  @Override
  public int readableBytes() {
    return mNettyBuf.readableBytes();
  }

  /**
   * Release the Netty ByteBuf.
   */
  @Override
  public void release() {
    mNettyBuf.release();
  }
}
