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

package alluxio.grpc;

import alluxio.network.protocol.databuffer.DataBuffer;

import io.grpc.internal.ReadableBuffer;

import java.nio.ByteBuffer;

public class ReadableDataBuffer implements DataBuffer {
  private final ReadableBuffer mBuffer;

  public ReadableDataBuffer(ReadableBuffer buffer) {
    mBuffer = buffer;
  }

  @Override
  public Object getNettyOutput() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLength() {
    return mBuffer.readableBytes();
  }

  @Override
  public ByteBuffer getReadOnlyByteBuffer() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readBytes(byte[] dst, int dstIndex, int length) {
    mBuffer.readBytes(dst, dstIndex, length);
  }

  @Override
  public int readableBytes() {
    return mBuffer.readableBytes();
  }

  @Override
  public void release() {
    mBuffer.close();
  }
}
