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

import alluxio.client.file.cache.store.ByteArrayTargetBuffer;
import alluxio.client.file.cache.store.ByteBufferTargetBuffer;
import alluxio.client.file.cache.store.NettyBufTargetBuffer;
import alluxio.client.file.cache.store.PageReadTargetBuffer;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Position read interface. All implementer should be thread-safe.
 */
public interface PositionReader {
  /**
   * @param position position of the file to start reading data
   * @param buffer target byte array
   * @param offset the offset of the buffer
   * @param length bytes to read
   * @return bytes read, or -1 if end of file
   */
  default int positionRead(long position, byte[] buffer, int offset, int length)
      throws IOException {
    return positionRead(position, new ByteArrayTargetBuffer(buffer, offset), length);
  }

  /**
   * @param position position of the file to start reading data
   * @param buffer target byte buffer
   * @param length bytes to read
   * @return bytes read, or -1 if end of file
   */
  default int positionRead(long position, ByteBuffer buffer, int length) throws IOException {
    return positionRead(position, new ByteBufferTargetBuffer(buffer), length);
  }

  /**
   * @param position position of the file to start reading data
   * @param buffer target byte buf
   * @param length bytes to read
   * @return bytes read, or -1 if end of file
   */
  default int positionRead(long position, ByteBuf buffer, int length) throws IOException {
    return positionRead(position, new NettyBufTargetBuffer(buffer), length);
  }

  /**
   * @param position position of the file to start reading data
   * @param buffer target byte buffer
   * @param length bytes to read
   * @return bytes read, or -1 if end of file
   */
  int positionRead(long position, PageReadTargetBuffer buffer, int length) throws IOException;
}
