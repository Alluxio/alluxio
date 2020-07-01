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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * An abstract writer class to write or update a block stored in managed storage.
 * <p>
 * This class does not provide thread-safety.
 */
public abstract class BlockWriter extends BlockClient {

  /**
   * Default constructor for the abstract writer implementations.
   */
  public BlockWriter() {
    super(Type.WRITER);
  }

  /**
   * Appends data to the end of a block from an input {@link ByteBuffer}.
   *
   * @param inputBuf {@link ByteBuffer} that input data is stored in
   * @return the size of data that was appended in bytes
   */
  public abstract long append(ByteBuffer inputBuf) throws IOException;

  /**
   * Appends buf.readableBytes() bytes to the end of this block writer from the given buf.
   *
   * @param buf the byte buffer that holds the data
   * @return the size of data that was appended in bytes
   */
  public abstract long append(ByteBuf buf) throws IOException;

  /**
   * Appends buffer.readableBytes() bytes to the end of this block writer from the given buffer.
   *
   * @param buffer the byte buffer that holds the data
   * @return the size of data that was appended in bytes
   */
  public abstract long append(DataBuffer buffer) throws IOException;

  /**
   * @return the current write position (same as the number of bytes written)
   */
  public abstract long getPosition();

  /**
   * @return a writeable byte channel of the block
   */
  public abstract WritableByteChannel getChannel();
}
