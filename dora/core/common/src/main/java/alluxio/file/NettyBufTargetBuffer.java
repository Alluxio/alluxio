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

package alluxio.file;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Netty Buf backed target buffer for zero-copy read from page store.
 */
public class NettyBufTargetBuffer implements ReadTargetBuffer {
  private final ByteBuf mTarget;

  /**
   * @param target target buffer
   */
  public NettyBufTargetBuffer(ByteBuf target) {
    mTarget = target;
  }

  @Override
  public byte[] byteArray() {
    return mTarget.array();
  }

  @Override
  public ByteBuffer byteBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int offset() {
    return mTarget.writerIndex();
  }

  @Override
  public void offset(int newOffset) {
    mTarget.writerIndex(newOffset);
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
  public void writeBytes(byte[] srcArray, int srcOffset, int length) {
    mTarget.writeBytes(srcArray, srcOffset, length);
  }

  @Override
  public void writeBytes(ByteBuf buf) {
    mTarget.writeBytes(buf);
  }

  @Override
  public int readFromFile(RandomAccessFile file, int length) throws IOException {
    try (FileChannel channel = file.getChannel()) {
      return mTarget.writeBytes(channel, length);
    }
  }

  @Override
  public int readFromInputStream(InputStream is, int length) throws IOException {
    int bytesToRead = Math.min(length, mTarget.writableBytes());
    ReadableByteChannel source = Channels.newChannel(is);
    ByteBuffer slice = mTarget.nioBuffer(mTarget.writerIndex(), bytesToRead);
    int bytesRead = source.read(slice);
    if (bytesRead > 0) {
      mTarget.writerIndex(mTarget.writerIndex() + bytesRead);
    }
    return bytesRead;
  }

  /**
   * Get the internal Bytebuf object.
   * @return the internal Bytebuf object
   */
  public ByteBuf getTargetBuffer() {
    return mTarget;
  }
}
