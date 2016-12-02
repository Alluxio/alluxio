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
import alluxio.network.protocol.RPCBlockWriteRequest;
import alluxio.network.protocol.RPCBlockWriteResponse;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.databuffer.DataNettyBuffer;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
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
public class NettyBlockWriter implements BlockWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final long PACKET_SIZE =
      Configuration.getBytes(PropertyKey.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES);
  private static final int MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS);
  private static final long WRITE_TIMEOUT_MS =
      Configuration.getLong(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  private final Channel mChannel;
  private final InetSocketAddress mAddress;
  private final long mBlockId;
  private final long mSessionId;

  private ReentrantLock mLock = new ReentrantLock();
  /** The next pos to write to the channel. */
  @GuardedBy("mLock")
  private long mPosToWrite = 0;
  /** The next pos to queue to the netty buffer. */
  @GuardedBy("mLock")
  private long mPosToQueue = 0;
  @GuardedBy("mLock")
  private Throwable mThrowable = null;
  @GuardedBy("mLock")
  private boolean mDone = false;
  /** This condition meets if mThrowable != null or mDone = true. */
  private Condition mDoneOrFail = mLock.newCondition();
  /** This condition meets if mThrowable != null or the buffer is not full. */
  private Condition mBufferNotFullOrFail = mLock.newCondition();

  /**
   * Creates an instance of {@link NettyBlockWriter}.
   *
   * @param address the data server network address
   * @param blockId the block ID
   * @param sessionId the session ID
   * @throws IOException it fails to create the object because it fails to acquire a netty channel
   */
  public NettyBlockWriter(final InetSocketAddress address, long blockId, long sessionId)
      throws IOException {
    mAddress = address;
    mSessionId = sessionId;
    mBlockId = blockId;

    mChannel = BlockStoreContext.acquireNettyChannel(address);
    mChannel.pipeline().addLast(new Handler());
  }

  @Override
  public long pos() {
    mLock.lock();
    try {
      return mPosToQueue;
    } finally {
      mLock.unlock();
    }
  }

  @Override
  public void writePacket(final ByteBuf buf) throws IOException {
    Preconditions.checkArgument(buf.readableBytes() <= PACKET_SIZE);
    final long len;
    final long offset;
    try {
      mLock.lock();
      while (true) {
        if (mThrowable != null) {
          throw new IOException(mThrowable);
        }
        if (!tooManyPacketsInFlight()) {
          offset = mPosToQueue;
          mPosToQueue += buf.readableBytes();
          len = buf.readableBytes();
          break;
        }
        try {
          if (!mBufferNotFullOrFail.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            throw new IOException(
                String.format("Timeout to write packet to block %d @ %s.", mBlockId, mAddress));
          }
        } catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      }
    } finally {
      mLock.unlock();
    }

    mChannel.eventLoop().submit(new Runnable() {
      @Override
      public void run() {
        mChannel.write(new RPCBlockWriteRequest(mSessionId, mBlockId, offset, len,
            new DataNettyBuffer(buf, len))).addListener(new WriteListener(offset + len));
      }
    });
  }

  @Override
  public void close() {
    mLock.lock();
    try {
      // Write the last packet.
      mChannel.eventLoop().submit(new Runnable() {
        @Override
        public void run() {
          mChannel.write(new RPCBlockWriteRequest(mSessionId, mBlockId, mPosToQueue, 0, null))
              .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        }
      });

      while (true) {
        if (mDone) {
          return;
        }
        if (mThrowable != null) {
          Preconditions.checkState(!mChannel.isOpen());
          return;
        }
        try {
          if (!mDoneOrFail.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            mChannel.close();
            LOG.warn("Timeout to close the NettyBlockWriter (block: {}, address: {}).", mBlockId,
                mAddress);
            return;
          }
        } catch (InterruptedException e) {
          mChannel.close();
          throw Throwables.propagate(e);
        }
      }
    } finally {
      mLock.unlock();
      if (mChannel.isOpen()) {
        Preconditions.checkState(mChannel.pipeline().last() instanceof Handler);
        mChannel.pipeline().removeLast();
      }
      BlockStoreContext.releaseNettyChannel(mAddress, mChannel);
    }
  }

  /**
   * @return true if there are too many bytes in flight
   */
  private boolean tooManyPacketsInFlight() {
    return mPosToQueue - mPosToWrite >= MAX_PACKETS_IN_FLIGHT * PACKET_SIZE;
  }

  /**
   * The netty handler that handles {@link RPCBlockWriteResponse}.
   */
  private final class Handler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
      Preconditions.checkState(msg instanceof RPCBlockWriteResponse, "Incorrect response type.");
      RPCBlockWriteResponse response = (RPCBlockWriteResponse) msg;
      if (response.getStatus() != RPCResponse.Status.SUCCESS) {
        throw new IOException(String
            .format("Failed to write block %d from %s with status %s.", mBlockId, mAddress,
                response.getStatus().getMessage()));
      }
      mLock.lock();
      try {
        mDone = true;
        mDoneOrFail.notify();
      } finally {
        mLock.unlock();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Exception caught while reading response from netty channel {}.",
          cause.getMessage());
      try {
        mLock.lock();
        mThrowable = cause;
        mBufferNotFullOrFail.notify();
        mDoneOrFail.notify();
      } finally {
        mLock.unlock();
      }

      ctx.close();
    }
  }

  /**
   * The netty channel future listener that is called when packet write is flushed.
   */
  private final class WriteListener implements ChannelFutureListener {
    private final long mPosToWriteUncommitted;

    public WriteListener(long pos) {
      mPosToWriteUncommitted = pos;
    }
    @Override
    public void operationComplete(ChannelFuture future) {
      if (!future.isSuccess()) {
        future.channel().close();
      }

      mLock.lock();
      try {
        Preconditions.checkState(mPosToWriteUncommitted - mPosToWrite <= PACKET_SIZE,
            "Some packet is not acked.");
        mPosToWrite = mPosToWriteUncommitted;

        if (future.cause() != null) {
          mThrowable = future.cause();
          mDoneOrFail.notify();
          mBufferNotFullOrFail.notify();
          return;
        }

        if (mPosToQueue - mPosToWriteUncommitted < MAX_PACKETS_IN_FLIGHT * PACKET_SIZE) {
          mBufferNotFullOrFail.notify();
        }
      } finally {
        mLock.unlock();
      }
    }
  }
}

