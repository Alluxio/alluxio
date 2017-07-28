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
import alluxio.client.file.options.InStreamOptions;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.CanceledException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.status.Status.PStatus;
import alluxio.util.CommonUtils;
import alluxio.util.network.NettyUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A netty packet reader that streams a region from a netty data server.
 *
 * Protocol:
 * 1. The client sends a read request (id, offset, length).
 * 2. Once the server receives the request, it streams packets to the client. The streaming pauses
 *    if the server's buffer is full and resumes if the buffer is not full.
 * 3. The client reads packets from the stream. Reading pauses if the client buffer is full and
 *    resumes if the buffer is not full. If the client can keep up with network speed, the buffer
 *    should have at most one packet.
 * 4. The client stops reading if it receives an empty packet which signifies the end of the stream.
 * 5. The client can cancel the read request at anytime. The cancel request is ignored by the
 *    server if everything has been sent to channel.
 * 6. If the client wants to reuse the channel, the client must read all the packets in the channel
 *    before releasing the channel to the channel pool.
 * 7. To make it simple to handle errors, the channel is closed if any error occurs.
 */
@NotThreadSafe
public final class NettyPacketReader implements PacketReader {
  private static final Logger LOG = LoggerFactory.getLogger(NettyPacketReader.class);

  private static final int MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS);
  private static final long READ_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  /** Special packet that indicates an exception is caught. */
  private static final ByteBuf THROWABLE = Unpooled.buffer(0);
  /** Special packet that indicates this is a UFS read heartbeat sent by the server. */
  private static final ByteBuf UFS_READ_HEARTBEAT = Unpooled.buffer(0);
  /** Special packet that indicates the EOF is reached or the stream is cancelled. */
  private static final ByteBuf EOF_OR_CANCELLED = Unpooled.buffer(0);

  private final FileSystemContext mContext;
  private final Channel mChannel;
  private final Protocol.ReadRequest mReadRequest;
  private final WorkerNetAddress mAddress;

  /**
   * This queue contains buffers read from netty. Its length is bounded by MAX_PACKETS_IN_FLIGHT.
   * Only the netty I/O thread can push to the queue. Only the client thread can poll from the
   * queue.
   */
  private final BlockingQueue<ByteBuf> mPackets = new LinkedBlockingQueue<>();
  /**
   * The exception caught when reading packets from the netty channel. This is only updated
   * by the netty I/O thread. The client thread only reads it after THROWABLE is found in
   * mPackets queue.
   */
  private volatile Throwable mPacketReaderException;

  /**
   * The next pos to read. This is only updated by the client thread (not touched by the netty
   * I/O thread).
   */
  private long mPosToRead;
  /**
   * This is true only when an empty packet (EOF or CANCELLED) is received. This is only updated
   * by the client thread (not touched by the netty I/O thread).
   */
  private boolean mDone = false;

  private boolean mClosed = false;

  /**
   * Creates an instance of {@link NettyPacketReader}. If this is used to read a block remotely, it
   * requires the block to be locked beforehand and the lock ID is passed to this class.
   *
   * @param context the file system context
   * @param address the netty data server address
   * @param readRequest the read request
   * @param options the in stream options
   */
  private NettyPacketReader(FileSystemContext context, WorkerNetAddress address,
      Protocol.ReadRequest readRequest, InStreamOptions options) throws IOException {
    mContext = context;
    mAddress = address;
    mPosToRead = readRequest.getOffset();
    mReadRequest = readRequest;

    mChannel = mContext.acquireNettyChannel(address);
    mChannel.pipeline().addLast(new PacketReadHandler());
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(mReadRequest)))
        .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  @Override
  public long pos() {
    return mPosToRead;
  }

  @Override
  public DataBuffer readPacket() throws IOException {
    Preconditions.checkState(!mClosed, "PacketReader is closed while reading packets.");
    // TODO(peis): Have a better criteria to resume so that we can have fewer state changes.
    if (!tooManyPacketsPending()) {
      NettyUtils.enableAutoRead(mChannel);
    }
    ByteBuf buf;
    try {
      while ((buf = mPackets.poll(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)) == UFS_READ_HEARTBEAT) {
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CanceledException(e);
    }
    if (buf == null) {
      throw new DeadlineExceededException(String
          .format("Timeout to read %d from %s.", mReadRequest.getBlockId(), mChannel.toString()));
    }
    if (buf == THROWABLE) {
      Preconditions.checkNotNull(mPacketReaderException, "mPacketReaderException");
      Throwables.propagateIfPossible(mPacketReaderException, IOException.class);
      throw AlluxioStatusException.fromCheckedException(mPacketReaderException);
    }
    if (buf == EOF_OR_CANCELLED) {
      mDone = true;
      return null;
    }
    mPosToRead += buf.readableBytes();
    Preconditions.checkState(mPosToRead - mReadRequest.getOffset() <= mReadRequest.getLength());
    return new DataNettyBufferV2(buf);
  }

  @Override
  public void close() {
    if (mClosed) {
      return;
    }
    try {
      if (mDone) {
        return;
      }
      if (!mChannel.isOpen()) {
        return;
      }
      if (remaining() > 0) {
        Protocol.ReadRequest cancelRequest = mReadRequest.toBuilder().setCancel(true).build();
        mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(cancelRequest)))
            .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
      }

      try {
        readAndDiscardAll();
      } catch (IOException e) {
        LOG.warn("Failed to close the NettyBlockReader (block: {}, address: {}) with exception {}.",
            mReadRequest.getBlockId(), mAddress, e.getMessage());
        CommonUtils.closeChannel(mChannel);
        return;
      }
    } finally {
      if (mChannel.isOpen()) {
        mChannel.pipeline().removeLast();

        // Make sure "autoread" is on before releasing the channel.
        NettyUtils.enableAutoRead(mChannel);
      }
      mContext.releaseNettyChannel(mAddress, mChannel);
      mClosed = true;
    }
  }

  /**
   * Reads and discards everything read from the channel until it reaches end of the stream.
   */
  private void readAndDiscardAll() throws IOException {
    DataBuffer buf;
    do {
      buf = readPacket();
      if (buf != null) {
        buf.release();
      }
      // A null packet indicates the end of the stream.
    } while (buf != null);
  }

  /**
   * @return bytes remaining
   */
  private long remaining() {
    return mReadRequest.getOffset() + mReadRequest.getLength() - mPosToRead;
  }

  /**
   * @return true if there are too many packets pending
   */
  private boolean tooManyPacketsPending() {
    return mPackets.size() >= MAX_PACKETS_IN_FLIGHT;
  }

  /**
   * The netty handler that reads packets from the channel.
   */
  private class PacketReadHandler extends ChannelInboundHandlerAdapter {
    /**
     * Default constructor.
     */
    PacketReadHandler() {}

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
      // Precondition check is not used here to avoid calling msg.getClass().getCanonicalName()
      // all the time.
      if (!(msg instanceof RPCProtoMessage)) {
        throw new IllegalStateException(String
            .format("Incorrect response type %s, %s.", msg.getClass().getCanonicalName(), msg));
      }

      ByteBuf buf;
      RPCProtoMessage response = (RPCProtoMessage) msg;
      ProtoMessage message = response.getMessage();
      if (message.isReadResponse()) {
        Preconditions.checkState(
            message.asReadResponse().getType() == Protocol.ReadResponse.Type.UFS_READ_HEARTBEAT);
        buf = UFS_READ_HEARTBEAT;
      } else if (message.isResponse()) {
        // Canceled is considered a valid status and handled in the reader. We avoid creating a
        // CanceledException as an optimization.
        if (message.asResponse().getStatus() != PStatus.CANCELED) {
          CommonUtils.unwrapResponse(response.getMessage().asResponse());
        }

        DataBuffer dataBuffer = response.getPayloadDataBuffer();
        if (dataBuffer == null) {
          buf = EOF_OR_CANCELLED;
        } else {
          Preconditions.checkState(dataBuffer.getNettyOutput() instanceof ByteBuf);
          buf = (ByteBuf) dataBuffer.getNettyOutput();
        }
      } else {
        throw new IllegalStateException(
            String.format("Incorrect response type %s.", message.toString()));
      }

      if (tooManyPacketsPending()) {
        NettyUtils.disableAutoRead(ctx.channel());
      }
      mPackets.offer(buf);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Exception caught while reading from {}.", mReadRequest.getBlockId(), cause);

      // NOTE: The netty I/O thread associated with mChannel is the only thread that can update
      // mPacketReaderException and push to mPackets. So it is safe to do the following without
      // synchronization.
      // Make sure to set mPacketReaderException before pushing THROWABLE to mPackets.
      if (mPacketReaderException == null) {
        mPacketReaderException = cause;
        mPackets.offer(THROWABLE);
      }
      ctx.close();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
      LOG.warn("Channel {} is closed while reading from {}.", mChannel, mReadRequest.getBlockId());

      // NOTE: The netty I/O thread associated with mChannel is the only thread that can update
      // mPacketReaderException and push to mPackets. So it is safe to do the following without
      // synchronization.
      // Make sure to set mPacketReaderException before pushing THROWABLE to mPackets.
      if (mPacketReaderException == null) {
        mPacketReaderException =
            new IOException(String.format("Channel %s is closed.", mChannel.toString()));
        mPackets.offer(THROWABLE);
      }
      ctx.fireChannelUnregistered();
    }
  }

  /**
   * Factory class to create {@link NettyPacketReader}s.
   */
  public static class Factory implements PacketReader.Factory {
    private final FileSystemContext mContext;
    private final WorkerNetAddress mAddress;
    private final Protocol.ReadRequest mReadRequestPartial;
    private final InStreamOptions mOptions;

    /**
     * Creates an instance of {@link NettyPacketReader.Factory} for block reads.
     *
     * @param context the file system context
     * @param address the worker address
     * @param readRequestPartial the partial read request
     * @param options the in stream options
     */
    public Factory(FileSystemContext context, WorkerNetAddress address,
        Protocol.ReadRequest readRequestPartial, InStreamOptions options) {
      mContext = context;
      mAddress = address;
      mReadRequestPartial = readRequestPartial;
      mOptions = options;
    }

    @Override
    public PacketReader create(long offset, long len) throws IOException {
      return new NettyPacketReader(mContext, mAddress,
          mReadRequestPartial.toBuilder().setOffset(offset).setLength(len).build(), mOptions);
    }

    @Override
    public boolean isShortCircuit() {
      return false;
    }

    @Override
    public void close() throws IOException {}
  }
}

