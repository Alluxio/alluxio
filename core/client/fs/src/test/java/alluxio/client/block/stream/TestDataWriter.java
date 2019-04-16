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

package alluxio.client.block.stream;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link DataWriter} which writes data to a bytebuffer.
 */
public class TestDataWriter implements DataWriter {
  private final ByteBuffer mBuffer;

  public TestDataWriter(ByteBuffer buffer) {
    mBuffer = buffer;
  }

  @Override
  public void writeChunk(ByteBuf chunk) throws IOException {
    mBuffer.limit(mBuffer.position() + chunk.readableBytes());
    chunk.readBytes(mBuffer);
  }

  @Override
  public void cancel() {
    return;
  }

  @Override
  public void flush() {
    return;
  }

  @Override
  public int chunkSize() {
    return 128;
  }

  @Override
  public long pos() {
    return mBuffer.position();
  }

  @Override
  public void close() {
    return;
  }
}
