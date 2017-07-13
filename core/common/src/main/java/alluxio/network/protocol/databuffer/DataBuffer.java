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

package alluxio.network.protocol.databuffer;

import java.nio.ByteBuffer;

/**
 * This interface is a simple wrapper for the optional data that an RPCMessage may have. This allows
 * subclasses to support different representations of the data.
 */
public interface DataBuffer {
  /**
   * Returns an object for writing to a netty channel.
   *
   * @return the object to output to Netty. Must be ByteBuf or FileRegion
   */
  Object getNettyOutput();

  /**
   * Returns the length of the data.
   *
   * @return the length of the data in bytes
   */
  long getLength();

  /**
   * Returns a {@link ByteBuffer} for read-only access to the data.
   *
   * @return a read-only ByteBuffer representing the data
   */
  ByteBuffer getReadOnlyByteBuffer();

  /**
   * Transfers this buffer's data to the specified destination starting at
   * the current {@code readerIndex} and increases the {@code readerIndex}
   * by the number of the transferred bytes (= {@code length}).
   *
   * @param dst the destination
   * @param dstIndex the first index of the destination
   * @param length the number of bytes to transfer
   */
  void readBytes(byte[] dst, int dstIndex, int length);

  /**
   * @return the number of readable bytes remaining
   */
  int readableBytes();

  /**
   * Releases the underlying buffer of this DataBuffer if no longer needed.
   */
  void release();
}
