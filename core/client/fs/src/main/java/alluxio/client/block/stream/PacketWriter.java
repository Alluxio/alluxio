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

import alluxio.client.Cancelable;

import io.netty.buffer.ByteBuf;

import java.io.Closeable;
import java.io.IOException;

/**
 * The interface to write packets.
 */
public interface PacketWriter extends Closeable, Cancelable {
  /**
   * Writes a packet. This method takes the ownership of this packet even if it fails to write
   * the packet.
   *
   * @param packet the packet
   */
  void writePacket(ByteBuf packet) throws IOException;

  /**
   *  Flushes all the pending packets.
   */
  void flush() throws IOException;

  /**
   * @return the packet size in bytes used
   */
  int packetSize();

  /**
   * @return the current pos which is the same as the totally number of bytes written so far
   */
  long pos();
}
