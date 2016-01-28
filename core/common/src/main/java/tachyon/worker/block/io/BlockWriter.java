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
