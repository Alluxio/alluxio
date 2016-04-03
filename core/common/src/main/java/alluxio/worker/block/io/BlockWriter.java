/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.block.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A writer interface to write or update a block stored in managed storage.
 * <p>
 * This class does not provide thread-safety.
 */
public interface BlockWriter extends Closeable {
  /**
   * Appends data to the end of a block from an input {@link ByteBuffer}.
   *
   * @param inputBuf {@link ByteBuffer} that input data is stored in
   * @return the size of data that was appended in bytes
   * @throws IOException if the operation fails
   */
  long append(ByteBuffer inputBuf) throws IOException;

  /**
   * Returns writable byte channel to write to this block.
   *
   * @return channel
   */
  WritableByteChannel getChannel();
}
