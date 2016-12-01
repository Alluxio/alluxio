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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.block.BlockStoreContext;
import alluxio.network.protocol.RPCBlockReadRequest;
import alluxio.network.protocol.RPCBlockReadResponse;
import alluxio.network.protocol.RPCResponse;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
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
public class NettyBlockReader implements BlockReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final int MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS);
  private static final long READ_TIMEOUT_MS =
      Configuration.getLong(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  private final Channel mChannel;
  private final InetSocketAddress mAddress;
  private final long mBlockId;
  private final long mStart;
  private final long mBytesToRead;
  private final Handler mHandler = new Handler();

  private ReentrantLock mLock = new ReentrantLock();
  @GuardedBy("mLock")
  private Queue<ByteBuf> mPackets = new LinkedList<>();
  @GuardedBy("mLock")
  private Throwable mPacketReaderException = null;
  private Condition mNotEmptyOrFail = mLock.newCondition();

  /** The next pos to read. */
  private long mPosToRead;
  /** This is true only when an empty packet is received. */
  private boolean mDone = false;

  /**
   * Creates an instance of {@link NettyBlockReader}.
   *
   * @param address the netty data server network address
   * @param blockId the block ID
   * @param offset the offset
   * @param len the length to read
   * @param lockId the lock ID
   * @param sessionId the session ID
   * @throws IOException if it fails to create the object
   */
  public NettyBlockReader(final InetSocketAddress address, long blockId, long offset, int len,
      long lockId, long sessionId) throws IOException {
    mAddress = address;
    mBlockId = blockId;
    mStart = offset;
    mPosToRead = offset;
    mBytesToRead = len;
    Preconditions.checkState(offset >= 0 && len > 0);

    mChannel = BlockStoreContext.acquireNettyChannel(address);
    mChannel.pipeline().addLast(mHandler);

    mChannel.writeAndFlush(new RPCBlockReadRequest(blockId, offset, len, lockId, sessionId))
        .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  @Override
  public long pos() {
    return mPosToRead;
  }

  @Override
  public ByteBuf readPacket() throws IOException {
    while (true) {
      mLock.lock();
      try {
        if (mPacketReaderException != null) {
          throw new IOException(mPacketReaderException);
        }
        ByteBuf buf = mPackets.poll();
        if (mPackets.size() < MAX_PACKETS_IN_FLIGHT) {
          mChannel.config().setAutoRead(true);
          mChannel.read();
        }
        if (buf == null) {
          try {
            if (!mNotEmptyOrFail.await(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
              throw new IOException(String
                  .format("Timeout while reading packet from block %d @ %s.", mBlockId, mAddress));
            }
          } catch (InterruptedException e) {
            throw Throwables.propagate(e);
          }
        }
        if (buf.readableBytes() == 0) {
          mDone = true;
          return null;
        }
        mPosToRead += buf.readableBytes();
        Preconditions.checkState(mPosToRead - mStart <= mBytesToRead);
        return buf;
      } finally {
        mLock.unlock();
      }
    }
 }

  @Override
  public void close() {
    try {
      if (mDone) {
        return;
      }
      if (!mChannel.isOpen()) {
        return;
      }
      try {
        ChannelFuture channelFuture =
            mChannel.writeAndFlush(RPCBlockReadRequest.createCancelRequest(mBlockId));
        channelFuture.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        channelFuture.sync();
      } catch (InterruptedException e) {
        mChannel.close();
        throw Throwables.propagate(e);
      }

      while (true) {
        try {
          ByteBuf buf = readPacket();
          if (buf == null) {
            return;
          }
          ReferenceCountUtil.release(buf);
        } catch (IOException e) {
          LOG.warn("Failed to close the NettyBlockReader (block: {}, address: {}).",
              mBlockId, mAddress, e);
          mChannel.close();
          return;
        }
      }
    } finally {
      if (mChannel.isOpen()) {
        Preconditions.checkState(mChannel.pipeline().last() instanceof Handler);
        mChannel.pipeline().removeLast();
      }
      BlockStoreContext.releaseNettyChannel(mAddress, mChannel);
    }
  }

  /**
   * The netty handler that reads packets from the channel.
   */
  public class Handler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
      Preconditions.checkState(msg instanceof RPCBlockReadResponse, "Incorrect response type.");
      RPCBlockReadResponse response = (RPCBlockReadResponse) msg;
      if (response.getStatus() != RPCResponse.Status.SUCCESS) {
        throw new IOException(String
            .format("Failed to read block %d from %s with status %s.", mBlockId, mAddress,
                response.getStatus().getMessage()));
      }
      mLock.lock();
      try {
        Preconditions.checkState(mPacketReaderException == null);
        ByteBuf buf = response.getPayloadData();
        Preconditions.checkState(mPackets.offer(buf));
        mNotEmptyOrFail.signal();

        if (mPackets.size() >= MAX_PACKETS_IN_FLIGHT) {
          ctx.channel().config().setAutoRead(false);
        }
      } finally {
        mLock.unlock();
        ReferenceCountUtil.release(response.getPayloadData());
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Exception caught while reading response from netty channel {}.",
          cause.getMessage());
      mLock.lock();
      try {
        mPacketReaderException = cause;
        mNotEmptyOrFail.signal();
      } finally {
        mLock.unlock();
      }
      ctx.close();
    }
  }
}

