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

import alluxio.util.io.BufferUtils;

import com.google.common.base.Preconditions;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * A DataBuffer with the underlying data being a {@link ByteBuffer}. If the backing ByteBuffer is
 * a direct ByteBuffer, {@link #release()} should only be called when all references to the
 * object are finished with it. For this reason, it is not recommended to share objects of this
 * class unless reference counting is implemented by the users of this class.
 */
public final class NioDataBuffer implements DataBuffer {
  private final ByteBuffer mBuffer;
  private final long mLength;

  /**
   * @param buffer The ByteBuffer representing the data
   * @param length The length of the ByteBuffer
   */
  public NioDataBuffer(ByteBuffer buffer, long length) {
    mBuffer = Preconditions.checkNotNull(buffer, "buffer");
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
  public void readBytes(byte[] dst, int dstIndex, int length) {
    mBuffer.get(dst, dstIndex, length);
  }

  @Override
  public void readBytes(OutputStream outputStream, int length) throws IOException {
    Unpooled.wrappedBuffer(mBuffer).readBytes(outputStream, length).release();
  }

  @Override
  public void readBytes(ByteBuffer outputBuf) {
    if (mBuffer.remaining() <= outputBuf.remaining()) {
      outputBuf.put(mBuffer);
    } else {
      int oldLimit = mBuffer.limit();
      mBuffer.limit(mBuffer.position() + outputBuf.remaining());
      outputBuf.put(mBuffer);
      mBuffer.limit(oldLimit);
    }
  }

  @Override
  public int readableBytes() {
    return mBuffer.remaining();
  }

  @Override
  public void release() {
    if (mBuffer.isDirect()) {
      BufferUtils.cleanDirectBuffer(mBuffer);
    }
  }
}
