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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Class for making wrapping {@link ReadableBuffer} as {@link DataBuffer}.
 */
public class ReadableDataBuffer implements DataBuffer {
  private final ReadableBuffer mBuffer;

  /**
   * Creates {@link DataBuffer} for reading.
   * @param buffer internal buffer
   */
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
  public void readBytes(ByteBuffer outputBuf) {
    mBuffer.readBytes(outputBuf);
  }

  @Override
  public void readBytes(byte[] dst, int dstIndex, int length) {
    mBuffer.readBytes(dst, dstIndex, length);
  }

  @Override
  public void readBytes(OutputStream outputStream, int length) throws IOException {
    mBuffer.readBytes(outputStream, length);
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
