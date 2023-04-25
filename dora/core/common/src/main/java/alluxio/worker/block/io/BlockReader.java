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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

/**
 * An abstract reader class to access the data of a block stored in managed storage.
 * <p>
 * This class does not provide thread-safety.
 */
public abstract class BlockReader extends BlockClient {

  /**
   * Default constructor for the abstract reader implementations.
   */
  public BlockReader() {
    super(Type.READER);
  }

  /**
   * Reads data from the block.
   *
   * <h2>Implementation Note</h2>
   * Since this method needs to allocate a buffer inside its implementation, it must not choose a
   * ByteBuffer that needs any kind of explicit de-allocation, e.g. a pooled buffer, because the
   * caller has no means of knowing how and when to properly deallocate the returned buffer.
   * <br>
   * If explicit de-allocation is desired, use an externally allocated {@link ByteBuf} and call
   * {@link #transferTo(ByteBuf)} instead. It is then the caller's responsibility to de-allocate
   * the buffer.
   *
   * @param offset the offset from starting of the block file in bytes
   * @param length the length of data to read in bytes. If offset + length exceeds the block size,
   *               the part of data from offset to the end of block is returned.
   * @return {@link ByteBuffer} the data that was read, empty buffer if {@code offset} is at
   * the end of block
   */
  public abstract ByteBuffer read(long offset, long length) throws IOException;

  /**
   * Gets the length of the block in bytes.
   *
   * @return the length of the block in bytes
   */
  public abstract long getLength();

  /**
   * Returns a readable byte channel of the block.
   *
   * @return channel
   */
  public abstract ReadableByteChannel getChannel();

  /**
   * Transfers data (up to buf.writableBytes()) from this reader to the buffer.
   *
   * @param buf the byte buffer
   * @return the number of bytes transferred, -1 if the end of the block is reached
   */
  public abstract int transferTo(ByteBuf buf) throws IOException;

  /**
   * @return true if this reader is closed
   */
  public abstract boolean isClosed();

  /**
   * @return an informational string of the location the reader is reading from
   */
  public abstract String getLocation();
}
