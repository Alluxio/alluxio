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
public final class LocalFilePacketWriter implements PacketWriter {
  private static final long PACKET_SIZE =
      Configuration.getBytes(PropertyKey.USER_LOCAL_WRITER_PACKET_SIZE_BYTES);
  private static final long FILE_BUFFER_BYTES =
      Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES);

  /** The position to write the next byte at. */
  private long mPos = 0;
  /** The number of bytes reserved on the block worker to hold the block. */
  private long mPosReserved = 0;
  private final long mBlockId;
  private final LocalFileBlockWriter mWriter;
  private final BlockWorkerClient mBlockWorkerClient;
  private boolean mClosed = false;

  /**
   * Creates an instance of {@link LocalFilePacketWriter}. This requires the block to be locked
   * beforehand.
   *
   * @param blockWorkerClient the block worker client, not owned by this class
   * @param blockId the block ID
   * @throws IOException if it fails to create the packet writer
   * @return the {@link LocalFilePacketWriter} created
   */
  public static LocalFilePacketWriter create(BlockWorkerClient blockWorkerClient,
      long blockId) throws IOException {
    return new LocalFilePacketWriter(blockWorkerClient, blockId);
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
    try {
      Preconditions.checkState(!mClosed, "PacketWriter is closed while writing packets.");
      int sz = buf.readableBytes();
      ensureReserved(mPos + sz);
      mPos += sz;
      Preconditions.checkState(buf.readBytes(mWriter.getChannel(), sz) == sz);
    } finally {
      buf.release();
    }
  }

  @Override
  public void cancel() throws IOException {
    close();
  }

  @Override
  public void flush() {}

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    try {
      mWriter.close();
    } finally {
      mClosed = true;
    }
  }

  /**
   * Creates an instance of {@link LocalFilePacketWriter}.
   *
   * @param blockWorkerClient the block worker client, not owned by this class
   * @param blockId the block ID
   * @throws IOException if it fails to create the packet writer
   */
  private LocalFilePacketWriter(BlockWorkerClient blockWorkerClient, long blockId)
      throws IOException {
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
  private void ensureReserved(long pos) throws IOException {
    if (pos <= mPosReserved) {
      return;
    }
    long toReserve = Math.max(pos - mPosReserved, FILE_BUFFER_BYTES);
    mBlockWorkerClient.requestSpace(mBlockId, toReserve);
    mPosReserved += toReserve;
  }
}

