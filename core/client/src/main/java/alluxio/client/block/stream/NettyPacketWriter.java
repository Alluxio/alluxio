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
import alluxio.network.protocol.RPCMessageDecoder;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.Status;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.protobuf.MessageLite;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A netty packet writer that streams a full block or file to a netty data server.
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
public class NettyPacketWriter implements PacketWriter {
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
  private final Protocol.RequestType mRequestType;

  private boolean mClosed = false;

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
  /** This condition meets if there is nothing in the netty buffer. */
  private Condition mBufferEmptyOrFail = mLock.newCondition();

  /**
   * Creates an instance of {@link NettyPacketWriter}.
   *
   * @param address the data server network address
   * @param blockId the block ID
   * @param sessionId the session ID
   * @param type the request type (block or UFS file)
   * @throws IOException it fails to create the object because it fails to acquire a netty channel
   */
  public NettyPacketWriter(final InetSocketAddress address, long blockId, long sessionId,
      Protocol.RequestType type) throws IOException {
    mAddress = address;
    mSessionId = sessionId;
    mBlockId = blockId;
    mRequestType = type;

    mChannel = BlockStoreContext.acquireNettyChannel(address);
    addHandler();
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
    final long len;
    final long offset;
    mLock.lock();
    try {
      Preconditions.checkState(!mClosed);
      Preconditions.checkArgument(buf.readableBytes() <= PACKET_SIZE);
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
    } catch (Throwable e) {
      buf.release();
      throw e;
    } finally {
      mLock.unlock();
    }

    mChannel.eventLoop().submit(new Runnable() {
      @Override
      public void run() {
        Protocol.WriteRequest writeRequest =
            Protocol.WriteRequest.newBuilder().setId(mBlockId).setOffset(offset)
                .setSessionId(mSessionId).setType(mRequestType).build();
        DataBuffer dataBuffer = new DataNettyBufferV2(buf);
        mChannel.writeAndFlush(new RPCProtoMessage(writeRequest, dataBuffer))
            .addListener(new WriteListener(offset + len));
      }
    });
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }

    mLock.lock();
    try {
      mThrowable = new IOException("PacketWriter is cancelled.");
      mBufferEmptyOrFail.signal();
      mBufferNotFullOrFail.signal();
      mDoneOrFail.signal();

      // TODO(peis): Better support cancel so that we do not need to close the channel.
      ChannelFuture future = mChannel.close().sync();
      if (future.cause() != null) {
        throw new IOException(future.cause());
      }
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    } finally {
      mLock.unlock();
      mClosed = true;
    }
  }

  @Override
  public void flush() throws IOException {
    mChannel.flush();

    mLock.lock();
    try {
      while (true) {
        if (mPosToWrite == mPosToQueue) {
          return;
        }
        if (mThrowable != null) {
          throw new IOException(mThrowable);
        }
        if (!mBufferEmptyOrFail.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          throw new IOException(
              String.format("Timeout to flush packets to block %d @ %s.", mBlockId, mAddress));
        }
      }
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    } finally {
      mLock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mLock.lock();
    try {
      // Write the last packet.
      mChannel.eventLoop().submit(new Runnable() {
        @Override
        public void run() {
          Protocol.WriteRequest writeRequest =
              Protocol.WriteRequest.newBuilder().setId(mBlockId).setOffset(mPosToQueue)
                  .setSessionId(mSessionId).setType(mRequestType).build();
          mChannel.writeAndFlush(new RPCProtoMessage(writeRequest, null))
              .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
          LOG.info("last packet written");
        }
      });

      while (true) {
        if (mDone) {
          return;
        }
        try {
          if (mThrowable != null) {
            mChannel.close().sync();
            throw new IOException(mThrowable);
          }
          if (!mDoneOrFail.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            mChannel.close().sync();
            throw new IOException(String.format(
                "Timeout to close the NettyPacketWriter (block: %d, address: %s).", mBlockId,
                mAddress));
          }
        } catch (InterruptedException e) {
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
      mClosed = true;
    }
  }

  /**
   * Add {@link Handler} to the channel pipeline.
   */
  private void addHandler() {
    ChannelPipeline pipeline = mChannel.pipeline();
    if (!(pipeline.last() instanceof RPCMessageDecoder)) {
      throw new RuntimeException(String.format("Channel pipeline has unexpected handlers %s.",
          pipeline.last().getClass().getCanonicalName()));
    }
    mChannel.pipeline().addLast(new Handler());
  }

  /**
   * @return true if there are too many bytes in flight
   */
  private boolean tooManyPacketsInFlight() {
    return mPosToQueue - mPosToWrite >= MAX_PACKETS_IN_FLIGHT * PACKET_SIZE;
  }

  @Override
  public int packetSize() {
    return (int) PACKET_SIZE;
  }

  /**
   * The netty handler that handles netty write response.
   */
  private final class Handler extends ChannelInboundHandlerAdapter {
    /**
     * Default constructor.
     */
    public Handler() {}

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
      Preconditions.checkState(acceptMessage(msg), "Incorrect response type.");
      RPCProtoMessage response = (RPCProtoMessage) msg;
      Protocol.Status status = ((Protocol.Response) response.getMessage()).getStatus();

      if (!Status.isOk(status)) {
        throw new IOException(String
            .format("Failed to write block %d from %s with status %s.", mBlockId, mAddress,
                status.toString()));
      }
      mLock.lock();
      try {
        mDone = true;
        mDoneOrFail.signal();
      } finally {
        mLock.unlock();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Exception caught while reading response from netty channel {}.",
          cause);
      mLock.lock();
      try {
        mThrowable = cause;
        mBufferNotFullOrFail.signal();
        mDoneOrFail.signal();
        mBufferEmptyOrFail.signal();
      } finally {
        mLock.unlock();
      }

      ctx.close();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
      mLock.lock();
      try {
        if (mThrowable == null) {
          mThrowable = new IOException("Channel closed.");
        }
        mBufferNotFullOrFail.signal();
        mDoneOrFail.signal();
        mBufferEmptyOrFail.signal();
      } finally {
        mLock.unlock();
      }
      ctx.fireChannelUnregistered();
    }

    /**
     * @param msg the message received
     * @return true if this message should be processed
     */
    private boolean acceptMessage(Object msg) {
      if (msg instanceof RPCProtoMessage) {
        MessageLite header = ((RPCProtoMessage) msg).getMessage();
        return header instanceof Protocol.Response;
      }
      return false;
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
          mDoneOrFail.signal();
          mBufferNotFullOrFail.signal();
          mBufferEmptyOrFail.signal();
          return;
        }
        if (mPosToWrite == mPosToQueue) {
          mBufferEmptyOrFail.signal();
        }
        if (!tooManyPacketsInFlight()) {
          mBufferNotFullOrFail.signal();
        }
      } finally {
        mLock.unlock();
      }
    }
  }
}

