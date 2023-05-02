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

package alluxio.util.io;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Channel adapters.
 * <br>
 * <p><b>Thread Safety</b></p>
 * The channel implementations in this class are thread safe, as per the general requirement
 * of the {@link java.nio.channels.Channel} interface. However, in case a channel is based on
 * an underlying buffer, cautions must be taken to preserve thread safety:
 * <ul>
 *   <li>Two channels should not share the same underlying buffer and be operated on
 *   <i>concurrently</i>.</li>
 *   <li>A buffer should not be manipulated directly while there is an outstanding channel that
 *   uses it as the underlying buffer.</li>
 * </ul>
 */
public class ChannelAdapters {
  private ChannelAdapters() { } // utility class

  /**
   * Adapts a ByteBuffer to be the destination of a {@link WritableByteChannel}.
   * <br>
   * The returned channel, when its {@link WritableByteChannel#write(ByteBuffer)} method is called,
   * will copy {@code p} bytes from the source buffer to the output buffer,
   * where {@code p = min(src.remaining(), output.remaining())}. After the write is successful,
   * the positions of both the source and output buffer will have increased by {@code p}.
   * If the output buffer has no space remaining, while the source
   * buffer has available bytes, {@link ClosedChannelException} is thrown.
   *
   * @param output the output byte buffer
   * @return channel
   */
  public static WritableByteChannel intoByteBuffer(ByteBuffer output) {
    return new ByteBufferWritableChannel(output);
  }

  /**
   * Adapts a byte array to be the destination of a {@link WritableByteChannel}.
   * Bytes will be written from the beginning of the array and to the end.
   *
   * @param output output buffer
   * @return channel
   */
  public static WritableByteChannel intoByteArray(byte[] output) {
    return intoByteBuffer(ByteBuffer.wrap(output));
  }

  /**
   * Adapts a byte array to be the destination of a {@link WritableByteChannel}.
   * Bytes will be written from index {@code offset} to {@code offset + length}.
   *
   * @param output output buffer
   * @param offset offset within the buffer
   * @param length length of the writable region within the buffer
   * @return channel
   */
  public static WritableByteChannel intoByteArray(byte[] output, int offset, int length) {
    return intoByteBuffer(ByteBuffer.wrap(output, offset, length));
  }

  /**
   * todo(bowen): implement.
   * @param output output
   * @return channel
   */
  public static WritableByteChannel intoByteBuf(ByteBuf output) {
    throw new UnsupportedOperationException("todo");
  }

  @ThreadSafe
  static class ByteBufferWritableChannel implements WritableByteChannel {
    @GuardedBy("this")
    private final ByteBuffer mOutput;
    private boolean mClosed = false;

    ByteBufferWritableChannel(ByteBuffer byteBuffer) {
      mOutput = byteBuffer;
    }

    /**
     * Copies bytes from the source buffer into the channel.
     *
     * @param src the buffer from which bytes are to be retrieved
     * @return number of bytes written, 0 when the source buffer has no bytes remaining
     * @throws ClosedChannelException if the channel has been explicitly closed,
     *         or when the output buffer does not have remaining space,
     *         but the source buffer has remaining readable bytes
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
      if (mClosed) {
        throw new ClosedChannelException();
      }
      synchronized (this) {
        int srcRemaining = src.remaining();
        int outputRemaining = mOutput.remaining();
        if (outputRemaining == 0 && srcRemaining > 0) {
          throw new ClosedChannelException();
        }
        int bytesToCopy = Math.min(srcRemaining, outputRemaining);
        ByteBuffer slice = src.slice();
        slice.limit(bytesToCopy);
        mOutput.put(slice);
        src.position(src.position() + bytesToCopy);
        return bytesToCopy;
      }
    }

    @Override
    public boolean isOpen() {
      return !mClosed;
    }

    @Override
    public void close() throws IOException {
      if (!mClosed) {
        mClosed = true;
      }
    }
  }
}
