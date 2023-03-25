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

package alluxio.client;

import alluxio.client.file.cache.store.ByteArrayTargetBuffer;
import alluxio.client.file.cache.store.ByteBufferTargetBuffer;
import alluxio.client.file.cache.store.NettyBufTargetBuffer;
import alluxio.client.file.cache.store.PageReadTargetBuffer;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * Position read interface. All implementer should be thread-safe.
 */
public interface PositionRead {
  /**
   * @param fileId the file identifier
   * @param position position of the file to start reading data
   * @param buffer target byte array
   * @param offset the offset of the buffer
   * @param size bytes to read
   * @return bytes read, or -1 if end of file
   */
  default  int read(PathIdentifier fileId, long position, byte[] buffer, int offset, int size) {
    return read(fileId, position, new ByteArrayTargetBuffer(buffer, offset), size);
  }

  /**
   * @param fileId the file identifier
   * @param position position of the file to start reading data
   * @param buffer target byte buffer
   * @param size bytes to read
   * @return bytes read, or -1 if end of file
   */
  default  int read(PathIdentifier fileId, long position, ByteBuffer buffer, int size) {
    return read(fileId, position, new ByteBufferTargetBuffer(buffer), size);
  }

  /**
   * @param fileId the file identifier
   * @param position position of the file to start reading data
   * @param buffer target byte buf
   * @param size bytes to read
   * @return bytes read, or -1 if end of file
   */
  default  int read(PathIdentifier fileId, long position, ByteBuf buffer, int size) {
    return read(fileId, position, new NettyBufTargetBuffer(buffer), size);
  }

  /**
   * @param fileId the file identifier
   * @param position position of the file to start reading data
   * @param buffer target byte buffer
   * @param size bytes to read
   * @return bytes read, or -1 if end of file
   */
  int read(PathIdentifier fileId, long position, PageReadTargetBuffer buffer, int size);
}
