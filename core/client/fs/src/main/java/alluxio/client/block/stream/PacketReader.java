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

package alluxio.client.block.stream;

import alluxio.network.protocol.databuffer.DataBuffer;

import java.io.Closeable;
import java.io.IOException;

/**
 * The interface to read packets.
 */
public interface PacketReader extends Closeable {

  /**
   * Reads a packet. The caller needs to release the packet.
   *
   * @return the data buffer or null if EOF is reached
   */
  DataBuffer readPacket() throws IOException;

  /**
   * @return the current stream position
   */
  long pos();

  /**
   * The factory interface to create {@link PacketReader}s.
   */
  interface Factory extends Closeable {
    /**
     * Creates an instance of {@link PacketReader}.
     *
     * @param offset the stream offset
     * @param len the length of the stream
     * @return the created object
     */
    PacketReader create(long offset, long len) throws IOException;

    /**
     * @return whether this factory generates packet readers which perform short-circuit reads
     */
    boolean isShortCircuit();
  }
}
