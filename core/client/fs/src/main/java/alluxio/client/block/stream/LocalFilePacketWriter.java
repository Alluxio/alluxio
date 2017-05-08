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
import alluxio.client.file.FileSystemContext;
import alluxio.client.netty.NettyRPC;
import alluxio.client.netty.NettyRPCContext;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockWriter;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A local packet writer that simply writes packets to a local file.
 */
@NotThreadSafe
public final class LocalFilePacketWriter implements PacketWriter {
  private static final long FILE_BUFFER_BYTES =
      Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES);
  private static final long WRITE_TIMEOUT_MS =
      Configuration.getLong(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  private final Channel mChannel;
  private final LocalFileBlockWriter mWriter;
  private final long mBlockId;
  private final long mPacketSize;
  private final ProtoMessage mCreateRequest;
  private final NettyRPCContext mNettyRPCContext;

  /** The position to write the next byte at. */
  private long mPos;
  /** The number of bytes reserved on the block worker to hold the block. */
  private long mPosReserved;

  private boolean mClosed = false;
  /**
   * Creates an instance of {@link LocalFilePacketWriter}. This requires the block to be locked
   * beforehand.
   *
   * @param blockId the block ID
   * @param tier the target tier
   * @param packetSize the packet size
   * @return the {@link LocalFilePacketWriter} created
   */
  public static LocalFilePacketWriter create(FileSystemContext context, WorkerNetAddress address,
      long sessionId, long blockId, int tier, long packetSize) {
    return new LocalFilePacketWriter(context, address, sessionId, blockId, tier, packetSize);
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public int packetSize() {
    return (int) mPacketSize;
  }

  @Override
  public void writePacket(final ByteBuf buf) {
    try {
      Preconditions.checkState(!mClosed, "PacketWriter is closed while writing packets.");
      int sz = buf.readableBytes();
      ensureReserved(mPos + sz);
      mPos += sz;
      Preconditions.checkState(buf.readBytes(mWriter.getChannel(), sz) == sz);
    } catch (IOException e) {
      throw AlluxioStatusException.fromIOException(e);
    } finally {
      buf.release();
    }
  }

  @Override
  public void cancel() {
    if (mClosed) {
      return;
    }
    mClosed = true;

    Closer closer = Closer.create();
    closer.register(mWriter);
    closer.register(new Closeable() {
      @Override
      public void close() throws IOException {
        NettyRPC.call(mNettyRPCContext, new ProtoMessage(
            Protocol.LocalBlockCompleteRequest.newBuilder().setBlockId(mBlockId).setCancel(true)
                .build()));
      }
    });
    CommonUtils.close(closer);
  }

  @Override
  public void flush() {}

  @Override
  public void close() {
    if (mClosed) {
      return;
    }
    mClosed = true;

    Closer closer = Closer.create();
    closer.register(mWriter);
    closer.register(new Closeable() {
      @Override
      public void close() throws IOException {
        NettyRPC.call(mNettyRPCContext, new ProtoMessage(
            Protocol.LocalBlockCompleteRequest.newBuilder().setBlockId(mBlockId).build()
        ));
      }
    });
    CommonUtils.close(closer);
  }

  /**
   * Creates an instance of {@link LocalFilePacketWriter}.
   *
   * @param blockId the block ID
   * @param tier the target tier
   * @param packetSize the packet size
   */
  private LocalFilePacketWriter(FileSystemContext context, WorkerNetAddress address, long sessionId,
      long blockId, int tier, long packetSize) {
    mChannel = context.acquireNettyChannel(address);
    mCreateRequest = new ProtoMessage(
        Protocol.LocalBlockCreateRequest.newBuilder().setBlockId(blockId).setSessionId(sessionId)
            .setTier(tier).setSpaceToReserve(FILE_BUFFER_BYTES).build());
    mNettyRPCContext = NettyRPCContext.defaults().setChannel(mChannel).setTimeout(WRITE_TIMEOUT_MS);

    ProtoMessage message = NettyRPC.call(mNettyRPCContext, mCreateRequest);
    Preconditions.checkState(message.isLocalBlockCreateResponse());
    mWriter = new LocalFileBlockWriter(message.asLocalBlockCreateResponse().getPath());
    mPosReserved += FILE_BUFFER_BYTES;
    mBlockId = blockId;
    mPacketSize = packetSize;
  }

  /**
   * Reserves enough space in the block worker.
   *
   * @param pos the pos of the file/block to reserve to
   */
  private void ensureReserved(long pos) {
    if (pos <= mPosReserved) {
      return;
    }
    long toReserve = Math.max(pos - mPosReserved, FILE_BUFFER_BYTES);
    NettyRPC.call(mNettyRPCContext, new ProtoMessage(
        mCreateRequest.asLocalBlockCreateRequest().toBuilder().setSpaceToReserve(toReserve)
            .build()));
    mPosReserved += toReserve;
  }
}

