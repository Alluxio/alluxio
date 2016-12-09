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

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A local packet writer that simply writes packets to a local file.
 */
@NotThreadSafe
public final class LocalPacketWriter implements PacketWriter {
  private static final long PACKET_SIZE =
      Configuration.getBytes(PropertyKey.USER_LOCAL_WRITER_PACKET_SIZE_BYTES);
  private static final long FILE_BUFFER_BYTES =
      Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES);

  private long mPos = 0;
  private long mPosReserved = 0;
  private final long mBlockId;
  private final LocalFileBlockWriter mWriter;
  private final BlockWorkerClient mBlockWorkerClient;
  private boolean mClosed = false;

  /**
   * Creates an instance of {@link LocalPacketWriter}.
   *
   * @param blockWorkerClient the block worker client, not owned by this class
   * @param blockId the block ID
   * @throws IOException if it fails to create the packet writer
   * @return the {@link LocalPacketWriter} created
   */
  public static LocalPacketWriter createLocalPacketWriter(BlockWorkerClient blockWorkerClient,
      long blockId) throws IOException {
    return new LocalPacketWriter(blockWorkerClient, blockId);
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
    Preconditions.checkState(!mClosed, "PacketWriter is closed while writing packets.");
    reserve(mPos + buf.readableBytes());
    try {
      mPos += buf.readableBytes();
      buf.readBytes(mWriter.getChannel(), buf.readableBytes());
    } finally {
      buf.release();
    }
  }

  @Override
  public void cancel() {}

  @Override
  public void flush() {}

  @Override
  public void close() throws IOException {
    mWriter.close();
    mClosed = true;
  }

  /**
   * Creates an instance of {@link LocalPacketWriter}.
   *
   * @param blockWorkerClient the block worker client, not owned by this class
   * @param blockId the block ID
   * @throws IOException if it fails to create the packet writer
   */
  private LocalPacketWriter(BlockWorkerClient blockWorkerClient, long blockId) throws IOException {
    String blockPath = blockWorkerClient.requestBlockLocation(blockId, FILE_BUFFER_BYTES);
    mWriter = new LocalFileBlockWriter(blockPath);
    mPosReserved += FILE_BUFFER_BYTES;
    mBlockId = blockId;
    mBlockWorkerClient = blockWorkerClient;
  }

  /**
   * Reserves enough space in the block worker.
   *
   * @param pos the pos of the file/block to reserve to
   * @throws IOException if it fails to reserve the space
   */
  private void reserve(long pos) throws IOException {
    if (pos <= mPosReserved) {
      return;
    }
    mBlockWorkerClient.requestSpace(mBlockId, Math.max(pos - mPosReserved, FILE_BUFFER_BYTES));
  }
}

