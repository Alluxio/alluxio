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
import alluxio.client.WriteType;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.network.netty.NettyRPC;
import alluxio.network.netty.NettyRPCContext;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.io.LocalFileBlockWriter;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A local packet writer that simply writes packets to a local file.
 */
@NotThreadSafe
public final class LocalFilePacketWriter implements PacketWriter {
  private static final Logger LOG = LoggerFactory.getLogger(LocalFilePacketWriter.class);
  private static final long FILE_BUFFER_BYTES =
      Configuration.getBytes(PropertyKey.USER_FILE_BUFFER_BYTES);
  private static final long WRITE_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  private final Channel mChannel;
  private final LocalFileBlockWriter mWriter;
  private final long mBlockId;
  private final long mPacketSize;
  private final ProtoMessage mCreateRequest;
  private final NettyRPCContext mNettyRPCContext;
  private final OutStreamOptions mOptions;
  private final Closer mCloser;

  /** The position to write the next byte at. */
  private long mPos;
  /** The number of bytes reserved on the block worker to hold the block. */
  private long mPosReserved;

  private boolean mClosed = false;

  /**
   * Creates an instance of {@link LocalFilePacketWriter}. This requires the block to be locked
   * beforehand.
   *
   * @param context the file system context
   * @param address the worker network address
   * @param blockId the block ID
   * @param options the output stream options
   * @return the {@link LocalFilePacketWriter} created
   */
  public static LocalFilePacketWriter create(final FileSystemContext context,
      final WorkerNetAddress address,
      long blockId, OutStreamOptions options) throws IOException {
    long packetSize = Configuration.getBytes(PropertyKey.USER_LOCAL_WRITER_PACKET_SIZE_BYTES);

    Closer closer = Closer.create();
    try {
      final Channel channel = context.acquireNettyChannel(address);
      closer.register(new Closeable() {
        @Override
        public void close() throws IOException {
          context.releaseNettyChannel(address, channel);
        }
      });
      Protocol.LocalBlockCreateRequest.Builder builder =
          Protocol.LocalBlockCreateRequest.newBuilder().setBlockId(blockId)
              .setTier(options.getWriteTier()).setSpaceToReserve(FILE_BUFFER_BYTES);
      if (options.getWriteType() == WriteType.ASYNC_THROUGH
          && Configuration.getBoolean(PropertyKey.USER_FILE_UFS_TIER_ENABLED)) {
        builder.setCleanupOnFailure(false);
      }
      ProtoMessage createRequest = new ProtoMessage(builder.build());
      NettyRPCContext nettyRPCContext =
          NettyRPCContext.defaults().setChannel(channel).setTimeout(WRITE_TIMEOUT_MS);
      ProtoMessage message = NettyRPC.call(nettyRPCContext, createRequest);
      Preconditions.checkState(message.isLocalBlockCreateResponse());
      LocalFileBlockWriter writer =
          closer.register(new LocalFileBlockWriter(message.asLocalBlockCreateResponse().getPath()));
      return new LocalFilePacketWriter(blockId, packetSize, options, channel,
          writer, createRequest, nettyRPCContext, closer);
    } catch (Exception e) {
      throw CommonUtils.closeAndRethrow(closer, e);
    }
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
  public void writePacket(final ByteBuf buf) throws IOException {
    try {
      Preconditions.checkState(!mClosed, "PacketWriter is closed while writing packets.");
      int sz = buf.readableBytes();
      ensureReserved(mPos + sz);
      mPos += sz;
      Preconditions.checkState(mWriter.append(buf) == sz);
    } finally {
      buf.release();
    }
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;

    try {
      NettyRPC.call(mNettyRPCContext, new ProtoMessage(
          Protocol.LocalBlockCompleteRequest.newBuilder().setBlockId(mBlockId).setCancel(true)
              .build()));
    } catch (Exception e) {
      throw mCloser.rethrow(e);
    } finally {
      mCloser.close();
    }
  }

  @Override
  public void flush() {}

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;

    mCloser.register(new Closeable() {
      @Override
      public void close() throws IOException {
        Protocol.LocalBlockCompleteRequest request =
            Protocol.LocalBlockCompleteRequest.newBuilder().setBlockId(mBlockId).build();
        NettyRPC.call(mNettyRPCContext, new ProtoMessage(request));
      }
    });
    mCloser.close();
  }

  /**
   * Creates an instance of {@link LocalFilePacketWriter}.
   *
   * @param blockId the block ID
   * @param packetSize the packet size
   * @param options the output stream options
   */
  private LocalFilePacketWriter(long blockId, long packetSize, OutStreamOptions options,
      Channel channel, LocalFileBlockWriter writer,
      ProtoMessage createRequest, NettyRPCContext nettyRPCContext, Closer closer) {
    mChannel = channel;
    mCloser = closer;
    mOptions = options;
    mWriter = writer;
    mCreateRequest = createRequest;
    mNettyRPCContext = nettyRPCContext;
    mPosReserved += FILE_BUFFER_BYTES;
    mBlockId = blockId;
    mPacketSize = packetSize;
  }

  /**
   * Reserves enough space in the block worker.
   *
   * @param pos the pos of the file/block to reserve to
   */
  private void ensureReserved(long pos) throws IOException {
    if (pos <= mPosReserved) {
      return;
    }
    long toReserve = Math.max(pos - mPosReserved, FILE_BUFFER_BYTES);
    NettyRPC.call(mNettyRPCContext, new ProtoMessage(
        mCreateRequest.asLocalBlockCreateRequest().toBuilder().setSpaceToReserve(toReserve)
            .setOnlyReserveSpace(true).build()));
    mPosReserved += toReserve;
  }
}

