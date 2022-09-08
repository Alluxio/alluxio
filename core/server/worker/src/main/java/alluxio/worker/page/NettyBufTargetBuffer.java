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

package alluxio.worker.page;

import alluxio.client.file.cache.store.PageReadTargetBuffer;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * Netty Buf backed target buffer for zero-copy read from page store.
 */
public class NettyBufTargetBuffer implements PageReadTargetBuffer {
  private final ByteBuf mTarget;
  private long mOffset = 0;

  /**
   * @param target target buffer
   */
  public NettyBufTargetBuffer(ByteBuf target) {
    mTarget = target;
  }

  @Override
  public boolean hasByteArray() {
    return false;
  }

  @Override
  public byte[] byteArray() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasByteBuffer() {
    return false;
  }

  @Override
  public ByteBuffer byteBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long offset() {
    return mOffset;
  }

  @Override
  public WritableByteChannel byteChannel() {
    return new WritableByteChannel() {
      @Override
      public int write(ByteBuffer src) throws IOException {
        int readableBytes = src.remaining();
        mTarget.writeBytes(src);
        return readableBytes - src.remaining();
      }

      @Override
      public boolean isOpen() {
        return true;
      }

      @Override
      public void close() throws IOException {
      }
    };
  }

  @Override
  public long remaining() {
    return mTarget.writableBytes();
  }

  @Override
  public void writeBytes(byte[] srcArray, int srcOffset, int dstOffset, int length) {
    throw new UnsupportedOperationException();
  }
}
