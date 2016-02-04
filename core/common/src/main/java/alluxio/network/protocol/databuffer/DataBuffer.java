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
   * Release the underlying buffer of this DataBuffer if no longer needed.
   */
  void release();
}
