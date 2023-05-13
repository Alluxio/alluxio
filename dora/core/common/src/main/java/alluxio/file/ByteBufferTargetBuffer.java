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

import alluxio.util.io.ChannelAdapters;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Target buffer backed by nio ByteBuffer for zero-copy read from page store.
 */
public class ByteBufferTargetBuffer implements ReadTargetBuffer {
  private final ByteBuffer mTarget;

  /**
   * Constructor.
   * @param target
   */
  public ByteBufferTargetBuffer(ByteBuffer target) {
    mTarget = target;
  }

  @Override
  public byte[] byteArray() {
    if (mTarget.hasArray()) {
      return mTarget.array();
    }
    throw new UnsupportedOperationException("ByteBuffer is not backed by an array");
  }

  @Override
  public ByteBuffer byteBuffer() {
    return mTarget;
  }

  @Override
  public int offset() {
    return mTarget.position();
  }

  @Override
  public void offset(int newOffset) {
    mTarget.position(newOffset);
  }

  @Override
  public WritableByteChannel byteChannel() {
    return ChannelAdapters.intoByteBuffer(mTarget);
  }

  @Override
  public long remaining() {
    return mTarget.remaining();
  }

  @Override
  public void writeBytes(byte[] srcArray, int srcOffset, int length) {
    mTarget.put(srcArray, srcOffset, length);
  }

  @Override
  public void writeBytes(ByteBuf buf) {
    if (mTarget.remaining() <= buf.readableBytes()) {
      buf.readBytes(mTarget);
      return;
    }
    int oldLimit = mTarget.limit();
    mTarget.limit(mTarget.position() + buf.readableBytes());
    buf.readBytes(mTarget);
    mTarget.limit(oldLimit);
  }

  @Override
  public int readFromFile(RandomAccessFile file, int length) throws IOException {
    int bytesToRead = Math.min(length, mTarget.remaining());
    ByteBuffer slice = mTarget.slice();
    slice.limit(bytesToRead);
    int bytesRead = file.getChannel().read(slice);
    if (bytesRead > 0) {
      mTarget.position(mTarget.position() + bytesRead);
    }
    return bytesRead;
  }

  @Override
  public int readFromInputStream(InputStream is, int length) throws IOException {
    int bytesToRead = Math.min(length, mTarget.remaining());
    ReadableByteChannel source = Channels.newChannel(is);
    ByteBuffer slice = mTarget.slice();
    slice.limit(bytesToRead);
    int bytesRead = source.read(slice);
    if (bytesRead > 0) {
      mTarget.position(mTarget.position() + bytesRead);
    }
    return bytesRead;
  }
}
