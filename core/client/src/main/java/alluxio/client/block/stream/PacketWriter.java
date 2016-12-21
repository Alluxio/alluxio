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

import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;

/**
 * The interface to write packets.
 */
public interface PacketWriter extends Closeable {
  /**
   * Writes a packet. This method takes the ownership of this packet.
   *
   * @param packet the packet
   * @throws IOException if it fails to write the packet
   */
  void writePacket(ByteBuf packet) throws IOException;

  /**
   *  Flushes all the pending packets.
   *
   * @throws IOException if it fails to flush
   */
  void flush() throws IOException;

  /**
   * Cancels the packet writes.
   *
   * @throws IOException if it fails to cancel
   */
  void cancel() throws IOException;

  /**
   * @return the packet size in bytes used
   */
  int packetSize();

  /**
   * @return the current pos which is the same as the totally number of bytes written so far
   */
  long pos();
}
