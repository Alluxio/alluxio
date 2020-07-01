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

package alluxio.worker.block.io;

import alluxio.network.protocol.databuffer.DataBuffer;

import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * A simple {@link BlockWriter} to use for testing purposes.
 */
public final class MockBlockWriter extends BlockWriter {
  private final ByteArrayOutputStream mOutputStream;
  private long mPosition;

  /**
   * Constructs a mock block writer which will remember all bytes written to it.
   */
  public MockBlockWriter() {
    mOutputStream = new ByteArrayOutputStream();
  }

  @Override
  public void close() throws IOException {
    mOutputStream.close();
    mPosition = -1;
  }

  @Override
  public long append(ByteBuffer inputBuf) throws IOException {
    byte[] bytes = new byte[inputBuf.remaining()];
    inputBuf.get(bytes);
    mOutputStream.write(bytes);
    mPosition += bytes.length;
    return bytes.length;
  }

  @Override
  public long append(ByteBuf buf) throws IOException {
    long bytesWritten = buf.readBytes(getChannel(), buf.readableBytes());
    mPosition += bytesWritten;
    return bytesWritten;
  }

  @Override
  public long append(DataBuffer buffer) throws IOException {
    byte[] bytes = new byte[buffer.readableBytes()];
    buffer.readBytes(bytes, 0, bytes.length);
    mOutputStream.write(bytes);
    mPosition += bytes.length;
    return bytes.length;
  }

  @Override
  public GatheringByteChannel getChannel() {
    return new GatheringByteChannel() {
      WritableByteChannel mChannel = Channels.newChannel(mOutputStream);

      @Override
      public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        long b = 0;
        for (int i = offset; i < offset + length; i++) {
          b += mChannel.write(srcs[i]);
        }
        return b;
      }

      @Override
      public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
      }

      @Override
      public int write(ByteBuffer src) throws IOException {
        return mChannel.write(src);
      }

      @Override
      public boolean isOpen() {
        return mChannel.isOpen();
      }

      @Override
      public void close() throws IOException {
        mChannel.close();
      }
    };
  }

  @Override
  public long getPosition() {
    return mPosition;
  }

  /**
   * @return the bytes written to this writer
   */
  public byte[] getBytes() {
    return mOutputStream.toByteArray();
  }
}
