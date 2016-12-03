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

import alluxio.Constants;
import alluxio.worker.block.io.LocalFileBlockReader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A netty block reader that streams a block region from a netty data server.
 *
 * Protocol:
 * 1. The client sends a read request (blockId, offset, length).
 * 2. Once the server receives the request, it streams packets the client. The streaming pauses
 *    if the server's buffer is full and resumes if the buffer is not full.
 * 3. The client reads packets from the stream. Reading pauses if the client buffer is full and
 *    resumes if the buffer is not full. If the client can keep up with network speed, the buffer
 *    should have at most one packet.
 * 4. The client stops reading if it receives an empty packe which signifies the end of the block
 *    streaming.
 * 5. The client can cancel the read request at anytime. The cancel request is ignored by the
 *    server if everything has been sent to channel.
 * 6. In order to reuse the channel, the client must read all the packets in the channel before
 *    releasing the channel to the channel pool.
 * 7. To make it simple to handle errors, the channel is closed if any error occurs.
 */
@NotThreadSafe
public final class LocalPacketReader implements PacketReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final long LOCAL_READ_PACKET_SIZE = 64 * 1024;

  /** The file reader to read a local block. */
  private final LocalFileBlockReader mReader;

  private long mPos;
  private final long mEnd;

  /**
   * Creates an instance of {@link LocalPacketReader}.
   *
   * @param offset the offset
   * @param len the length to read
   * @throws IOException if it fails to create the object
   */
  private LocalPacketReader(LocalFileBlockReader reader, long offset, int len) throws IOException {
    mReader = reader;
    mPos = offset;
    mEnd = offset + len;
  }

  @Override
  public ByteBuf readPacket() throws IOException {
    if (mPos >= mEnd) {
      return null;
    }
    ByteBuf buf =
        Unpooled.wrappedBuffer(mReader.read(mPos, Math.min(LOCAL_READ_PACKET_SIZE, mEnd - mPos)));
    mPos += buf.readableBytes();
    return buf;
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public void close() {}
}

