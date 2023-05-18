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

package alluxio;

import alluxio.file.ByteArrayTargetBuffer;
import alluxio.file.ByteBufferTargetBuffer;
import alluxio.file.NettyBufTargetBuffer;
import alluxio.file.ReadTargetBuffer;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Position read interface. All implementer should be thread-safe.
 */
@ThreadSafe
public interface PositionReader extends Closeable {
  /**
   * @param position position of the file to start reading data
   * @param buffer target byte array
   * @return bytes read, or -1 none of data is read
   */
  default int read(long position, byte[] buffer)
      throws IOException {
    return read(position, new ByteArrayTargetBuffer(buffer));
  }

  /**
   * @param position position of the file to start reading data
   * @param buffer target byte array
   * @param offset the offset of the buffer
   * @param length bytes to read
   * @return bytes read, or -1 none of data is read
   */
  default int read(long position, byte[] buffer, int offset, int length)
      throws IOException {
    return read(position, new ByteArrayTargetBuffer(buffer, offset, length));
  }

  /**
   * @param position position of the file to start reading data
   * @param buffer target byte buffer
   * @return bytes read, or -1 none of data is read
   */
  default int read(long position, ByteBuffer buffer) throws IOException {
    return read(position, new ByteBufferTargetBuffer(buffer));
  }

  /**
   * @param position position of the file to start reading data
   * @param buffer target byte buf
   * @return bytes read, or -1 none of data is read
   */
  default int read(long position, ByteBuf buffer) throws IOException {
    return read(position, new NettyBufTargetBuffer(buffer));
  }

  /**
   * @param position position of the file to start reading data
   * @param buffer target byte buffer
   * @return bytes read, or -1 none of data is read
   */
  default int read(long position, ReadTargetBuffer buffer)
      throws IOException {
    Preconditions.checkArgument(position >= 0, "position should be non-negative");
    if (buffer.remaining() == 0) {
      return 0;
    }
    return readInternal(position, buffer);
  }

  /**
   * @param position position of the file to start reading data
   * @param buffer target byte buffer
   * @return bytes read, or -1 none of data is read
   */
  int readInternal(long position, ReadTargetBuffer buffer)
      throws IOException;

  /**
   * Closes the positon reader and do cleanup job if any.
   */
  default void close() throws IOException {}
}
