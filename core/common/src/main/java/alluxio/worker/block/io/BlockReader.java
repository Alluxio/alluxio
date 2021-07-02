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
   * @param offset the offset from starting of the block file in bytes
   * @param length the length of data to read in bytes
   * @return {@link ByteBuffer} the data that was read
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
