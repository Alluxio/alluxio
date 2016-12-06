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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.client.block.BlockWorkerClient;
import alluxio.worker.block.io.LocalFileBlockWriter;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A netty block writer that streams a full block to a netty data server.
 *
 * Protocol:
 * 1. The client streams packets (start from pos 0) to the server. The client pauses if the client
 *    buffer is full, resumes if the buffer is not full.
 * 2. The server reads packets from the channel and writes them to the block worker. See the server
 *    side implementation for details.
 * 3. When all the packets are sent, the client closes the reader by sending an empty packet to
 *    the server to signify the end of the block. The client must wait the response from the server
 *    to make sure everything has been written to the block worker.
 * 4. To make it simple to handle errors, the channel is closed if any error occurs.
 */
@NotThreadSafe
public class LocalPacketWriter implements PacketWriter {
  // TODO(peis): Use a separate configuration.
  private static final long PACKET_SIZE =
      Configuration.getBytes(PropertyKey.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES);
  private static final long FILE_BUFFER_BYTES =
      Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES);

  private long mPos = 0;
  private long mPosReserved = 0;
  private final long mBlockId;
  private final LocalFileBlockWriter mWriter;
  private final BlockWorkerClient mBlockWorkerClient;

  /**
   * Creates an instance of {@link LocalPacketWriter}.
   */
  public LocalPacketWriter(BlockWorkerClient blockWorkerClient, long blockId) throws IOException {
    String blockPath = blockWorkerClient.requestBlockLocation(blockId, FILE_BUFFER_BYTES);
    mWriter = new LocalFileBlockWriter(blockPath);
    mPosReserved += FILE_BUFFER_BYTES;
    mBlockId = blockId;
    mBlockWorkerClient = blockWorkerClient;
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public int packetSize() {
    return (int) PACKET_SIZE;
  }

  @Override
  public void writePacket(final ByteBuf buf) throws IOException {
    reserve(mPos + buf.readableBytes());
    try {
      buf.readBytes(mWriter.getChannel(), buf.readableBytes());
      mPos += buf.readableBytes();
    } finally {
      ReferenceCountUtil.release(buf);
    }
  }

  void reserve(long pos) throws IOException {
    if (pos <= mPosReserved) {
      return;
    }
    mBlockWorkerClient.requestSpace(mBlockId, Math.max(pos - mPosReserved, FILE_BUFFER_BYTES));
  }

  @Override
  public void cancel() {}

  @Override
  public void flush() {}

  @Override
  public void close() throws IOException {
    mWriter.close();
  }
}

