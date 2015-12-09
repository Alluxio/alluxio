/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker.block.io;

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
   * @throws IOException if the operation fails
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
}
