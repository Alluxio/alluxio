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
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.Status;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.network.NettyUtils;
import alluxio.util.proto.ProtoMessage;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
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
      Configuration.getLong(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  /** Special packet that indicates an exception is caught. */
  private static final ByteBuf THROWABLE = Unpooled.buffer(0);
  /** Special packet that indicates the EOF is reached or the stream is cancelled. */
  private static final ByteBuf EOF_OR_CANCELLED = Unpooled.buffer(0);

  private final FileSystemContext mContext;
  private final Channel mChannel;
  private final Protocol.RequestType mRequestType;
  private final InetSocketAddress mAddress;
  private final long mId;
  private final long mStart;
  private final long mBytesToRead;
  private final boolean mNoCache;

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
   * @param address the netty data server network address
   * @param id the block ID or UFS file ID
   * @param offset the offset
   * @param len the length to read
   * @param lockId the lock ID
   * @param sessionId the session ID
   * @param noCache do not cache the block to the Alluxio worker if read from UFS when this is set
   * @param type the request type (block or UFS file)
   * @throws IOException if it fails to acquire a netty channel
   */
  private NettyPacketReader(FileSystemContext context, InetSocketAddress address, long id,
      long offset, long len, long lockId, long sessionId, boolean noCache,
      Protocol.RequestType type) throws IOException {
    Preconditions.checkArgument(offset >= 0 && len > 0);

    mContext = context;
    mAddress = address;
    mId = id;
    mStart = offset;
    mPosToRead = offset;
    mBytesToRead = len;
    mRequestType = type;
    mNoCache = noCache;

    mChannel = mContext.acquireNettyChannel(address);

    mChannel.pipeline().addLast(new PacketReadHandler());

    Protocol.ReadRequest readRequest =
        Protocol.ReadRequest.newBuilder().setId(id).setOffset(offset).setLength(len)
            .setLockId(lockId).setSessionId(sessionId).setType(type).setNoCache(noCache).build();
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(readRequest)))
        .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  @Override
  public long pos() {
    return mPosToRead;
  }

  @Override
  public DataBuffer readPacket() throws IOException {
    Preconditions.checkState(!mClosed, "PacketReader is closed while reading packets.");
    ByteBuf buf;

    // TODO(peis): Have a better criteria to resume so that we can have fewer state changes.
    if (!tooManyPacketsPending()) {
      NettyUtils.enableAutoRead(mChannel);
    }
    try {
      buf = mPackets.poll(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    if (buf == null) {
      throw new IOException(String.format("Timeout to read %d from %s.", mId, mChannel.toString()));
    }
    if (buf == THROWABLE) {
      throw CommonUtils.castToIOException(Preconditions.checkNotNull(mPacketReaderException));
    }
    if (buf == EOF_OR_CANCELLED) {
      mDone = true;
      return null;
    }
    mPosToRead += buf.readableBytes();
    Preconditions.checkState(mPosToRead - mStart <= mBytesToRead);
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
        Protocol.ReadRequest cancelRequest =
            Protocol.ReadRequest.newBuilder().setId(mId).setCancel(true).setType(mRequestType)
                .setNoCache(mNoCache).build();
        mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(cancelRequest)))
            .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
      }

      try {
        readAndDiscardAll();
      } catch (IOException e) {
        LOG.warn("Failed to close the NettyBlockReader (block: {}, address: {}) with exception {}.",
            mId, mAddress, e.getMessage());
        mChannel.close();
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
   *
   * @throws IOException if any I/O related errors occur
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
    return mStart + mBytesToRead - mPosToRead;
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
      if (!acceptMessage(msg)) {
        throw new IllegalStateException(String
            .format("Incorrect response type %s, %s.", msg.getClass().getCanonicalName(), msg));
      }

      RPCProtoMessage response = (RPCProtoMessage) msg;
      Protocol.Status status = response.getMessage().<Protocol.Response>getMessage().getStatus();
      if (!Status.isOk(status) && !Status.isCancelled(status)) {
        throw new IOException(String
            .format("Failed to read block %d from %s with status %s.", mId, mAddress,
                status.toString()));
      }

      DataBuffer dataBuffer = response.getPayloadDataBuffer();
      ByteBuf buf;
      if (dataBuffer == null) {
        buf = EOF_OR_CANCELLED;
      } else {
        Preconditions.checkState(
            dataBuffer.getLength() > 0 && (dataBuffer.getNettyOutput() instanceof ByteBuf));
        buf = (ByteBuf) dataBuffer.getNettyOutput();
      }

      if (tooManyPacketsPending()) {
        NettyUtils.disableAutoRead(ctx.channel());
      }
      mPackets.offer(buf);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Exception caught while reading from {}.", mId, cause);

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
      LOG.warn("Channel {} is closed while reading from {}.", mChannel, mId);

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

    /**
     * @param msg the message received
     * @return true if this message should be processed
     */
    private boolean acceptMessage(Object msg) {
      if (msg instanceof RPCProtoMessage) {
        return ((RPCProtoMessage) msg).getMessage().getType() == ProtoMessage.Type.RESPONSE;
      }
      return false;
    }
  }

  /**
   * Factory class to create {@link NettyPacketReader}s.
   */
  public static class Factory implements PacketReader.Factory {
    private final FileSystemContext mContext;
    private final InetSocketAddress mAddress;
    private final long mId;
    private final long mLockId;
    private final long mSessionId;
    private final boolean mNoCache;
    private final Protocol.RequestType mRequestType;

    /**
     * Creates an instance of {@link NettyPacketReader.Factory} for block reads.
     *
     * @param context the file system context
     * @param address the worker address
     * @param id the block ID or UFS ID
     * @param lockId the lock ID
     * @param sessionId the session ID
     * @param noCache if set, the block won't be cached in Alluxio if the block is a UFS block
     * @param type the request type
     */
    public Factory(FileSystemContext context, InetSocketAddress address, long id, long lockId,
        long sessionId, boolean noCache, Protocol.RequestType type) {
      mContext = context;
      mAddress = address;
      mId = id;
      mLockId = lockId;
      mSessionId = sessionId;
      mNoCache = noCache;
      mRequestType = type;
    }

    @Override
    public PacketReader create(long offset, long len) throws IOException {
      return new NettyPacketReader(mContext, mAddress, mId, offset, len, mLockId, mSessionId,
          mNoCache, mRequestType);
    }

    @Override
    public boolean isShortCircuit() {
      return false;
    }
  }
}

