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

import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * A reader interface to access the data of a block stored in managed storage.
 * <p>
 * This class does not provide thread-safety.
 */
public interface BlockReader extends Closeable {

  /**
   * Reads data from the block.
   *
   * @param offset the offset from starting of the block file in bytes
   * @param length the length of data to read in bytes, -1 for the rest of the block
   * @return {@link ByteBuffer} the data that was read
   */
  ByteBuffer read(long offset, long length) throws IOException;

  /**
   * Gets the length of the block in bytes.
   *
   * @return the length of the block in bytes
   */
  long getLength();

  /**
   * Returns a readable byte channel of the block.
   *
   * @return channel
   */
  ReadableByteChannel getChannel();

  /**
   * Transfers data (up to buf.writableBytes()) from this reader to the buffer.
   * This is only called by the netty data server.
   *
   * @param buf the byte buffer
   * @return the number of bytes transferred, -1 if the end of the block is reached
   */
  int transferTo(ByteBuf buf) throws IOException;

  /**
   * @return true if this reader is closed
   */
  boolean isClosed();
}
