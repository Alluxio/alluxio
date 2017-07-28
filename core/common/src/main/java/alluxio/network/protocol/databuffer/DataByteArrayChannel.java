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
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;

/**
 * A DataBuffer backed by a byte[].
 */
public final class DataByteArrayChannel implements DataBuffer {
  private final byte[] mByteArray;
  private final long mOffset;
  private final long mLength;

  /**
   *
   * @param byteArray the array representing the data
   * @param offset the offset into the byteArray
   * @param length the length of the data
   */
  public DataByteArrayChannel(byte[] byteArray, long offset, long length) {
    mByteArray = Preconditions.checkNotNull(byteArray, "byteArray");
    mOffset = offset;
    mLength = length;
  }

  @Override
  public Object getNettyOutput() {
    return Unpooled.wrappedBuffer(mByteArray, (int) mOffset, (int) mLength);
  }

  @Override
  public long getLength() {
    return mLength;
  }

  @Override
  public ByteBuffer getReadOnlyByteBuffer() {
    return ByteBuffer.wrap(mByteArray, (int) mOffset, (int) mLength).asReadOnlyBuffer();
  }

  @Override
  public void readBytes(byte[] dst, int dstIndex, int length) {
    throw new UnsupportedOperationException("DataByteArrayChannel#readBytes is not implemented.");
  }

  @Override
  public int readableBytes() {
    throw new UnsupportedOperationException(
        "DataByteArrayChannel#readableBytes is not implemented.");
  }

  @Override
  public void release() {
    // Nothing we need to release explicitly, let GC take care of all objects.
  }
}
