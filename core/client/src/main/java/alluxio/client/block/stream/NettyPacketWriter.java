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
import alluxio.util.proto.ProtoMessage;

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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A netty packet writer that streams a full block or a UFS file to a netty data server.
 *
 * Protocol:
 * 1. The client streams packets (start from pos 0) to the server. The client pauses if the client
 *    buffer is full, resumes if the buffer is not full.
 * 2. The server reads packets from the channel and writes them to the block worker. See the server
 *    side implementation for details.
 * 3. The client can either send an EOF packet or a CANCEL packet to end the write request. The
 *    client has to wait for the response from the data server for the EOF or CANCEL packet to make
 *    sure that the server has cleaned its states.
 * 4. To make it simple to handle errors, the channel is closed if any error occurs.
 *
 * NOTE: this class is NOT threadsafe. Do not call cancel/close while some other threads are
 * writing.
 */
@NotThreadSafe
public final class NettyPacketWriter implements PacketWriter {
  private static final Logger LOG = LoggerFactory.getLogger(NettyPacketWriter.class);

  private static final long PACKET_SIZE =
      Configuration.getBytes(PropertyKey.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES);
  private static final int MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS);
  private static final long WRITE_TIMEOUT_MS =
      Configuration.getLong(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  private final FileSystemContext mContext;
  private final Channel mChannel;
  private final InetSocketAddress mAddress;
  private final long mId;
  private final long mSessionId;
  private final int mTier;
  private final Protocol.RequestType mRequestType;
  private final long mLength;

  private boolean mClosed = false;

  private ReentrantLock mLock = new ReentrantLock();
  /** The next pos to write to the channel. */
  @GuardedBy("mLock")
  private long mPosToWrite = 0;
  /**
   * The next pos to queue to the netty buffer. mPosToQueue - mPosToWrite is the data sitting
   * in the netty buffer.
   */
  @GuardedBy("mLock")
  private long mPosToQueue = 0;
  @GuardedBy("mLock")
  private Throwable mPacketWriteException = null;
  @GuardedBy("mLock")
  private boolean mDone = false;
  @GuardedBy("mLock")
  private boolean mEOFSent = false;
  @GuardedBy("mLock")
  private boolean mCancelSent = false;
  /** This condition is met if mPacketWriteException != null or mDone = true. */
  private Condition mDoneOrFailed = mLock.newCondition();
  /** This condition is met if mPacketWriteException != null or the buffer is not full. */
  private Condition mBufferNotFullOrFailed = mLock.newCondition();
  /** This condition is met if there is nothing in the netty buffer. */
  private Condition mBufferEmptyOrFailed = mLock.newCondition();

  /**
   * Creates an instance of {@link NettyPacketWriter}.
   *
   * @param context the file system context
   * @param address the data server network address
   * @param id the block ID or UFS file ID
   * @param length the length of the block or file to write, set to Long.MAX_VALUE if unknown
   * @param sessionId the session ID
   * @param tier the target tier
   * @param type the request type (block or UFS file)
   * @throws IOException it fails to acquire a netty channel
   */
  public NettyPacketWriter(FileSystemContext context, final InetSocketAddress address, long id,
      long length, long sessionId, int tier, Protocol.RequestType type) throws IOException {
    mContext = context;
    mAddress = address;
    mSessionId = sessionId;
    mId = id;
    mRequestType = type;
    mLength = length;
    mTier = tier;
    mChannel = mContext.acquireNettyChannel(address);
    mChannel.pipeline().addLast(new PacketWriteHandler());
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
      Preconditions.checkState(!mClosed && !mEOFSent && !mCancelSent);
      Preconditions.checkArgument(buf.readableBytes() <= PACKET_SIZE);
      while (true) {
        if (mPacketWriteException != null) {
          throw new IOException(mPacketWriteException);
        }
        if (!tooManyPacketsInFlight()) {
          offset = mPosToQueue;
          mPosToQueue += buf.readableBytes();
          len = buf.readableBytes();
          break;
        }
        try {
          if (!mBufferNotFullOrFailed.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            throw new IOException(
                String.format("Timeout to write packet to %d @ %s.", mId, mAddress));
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

    Protocol.WriteRequest writeRequest =
        Protocol.WriteRequest.newBuilder().setId(mId).setOffset(offset).setSessionId(mSessionId)
            .setTier(mTier).setType(mRequestType).build();
    DataBuffer dataBuffer = new DataNettyBufferV2(buf);
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(writeRequest), dataBuffer))
        .addListener(new WriteListener(offset + len));
  }

  @Override
  public void cancel() throws IOException {
    if (mClosed) {
      return;
    }
    sendCancel();
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
        if (mPacketWriteException != null) {
          throw new IOException(mPacketWriteException);
        }
        if (!mBufferEmptyOrFailed.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          throw new IOException(
              String.format("Timeout to flush packets to %d @ %s.", mId, mAddress));
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

    sendEof();
    mLock.lock();
    try {
      while (true) {
        if (mDone) {
          return;
        }
        try {
          if (mPacketWriteException != null) {
            mChannel.close().sync();
            throw new IOException(mPacketWriteException);
          }
          if (!mDoneOrFailed.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            mChannel.close().sync();
            throw new IOException(String.format(
                "Timeout to close the NettyPacketWriter (block: %d, address: %s).", mId,
                mAddress));
          }
        } catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      }
    } finally {
      mLock.unlock();
      if (mChannel.isOpen()) {
        mChannel.pipeline().removeLast();
      }
      mContext.releaseNettyChannel(mAddress, mChannel);
      mClosed = true;
    }
  }

  /**
   * @return true if there are too many bytes in flight
   */
  private boolean tooManyPacketsInFlight() {
    return mPosToQueue - mPosToWrite >= MAX_PACKETS_IN_FLIGHT * PACKET_SIZE;
  }

  /**
   * Sends an EOF packet to end the write request if the stream.
   */
  private void sendEof() {
    final long pos;
    mLock.lock();
    try {
      if (mEOFSent || mCancelSent) {
        return;
      }
      mEOFSent = true;
      pos = mPosToQueue;
    } finally {
      mLock.unlock();
    }
    // Write the EOF packet.
    Protocol.WriteRequest writeRequest =
        Protocol.WriteRequest.newBuilder().setId(mId).setOffset(pos).setSessionId(mSessionId)
            .setTier(mTier).setType(mRequestType).setEof(true).build();
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(writeRequest), null))
        .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  /**
   * Sends a CANCEL packet to end the write request if the stream.
   */
  private void sendCancel() {
    final long pos;
    mLock.lock();
    try {
      if (mEOFSent || mCancelSent) {
        return;
      }
      mCancelSent = true;
      pos = mPosToQueue;
    } finally {
      mLock.unlock();
    }
    // Write the EOF packet.
    Protocol.WriteRequest writeRequest =
        Protocol.WriteRequest.newBuilder().setId(mId).setOffset(pos).setSessionId(mSessionId)
            .setTier(mTier).setType(mRequestType).setCancel(true).build();
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(writeRequest), null))
        .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  @Override
  public int packetSize() {
    return (int) PACKET_SIZE;
  }

  /**
   * The netty handler that handles netty write response.
   */
  private final class PacketWriteHandler extends ChannelInboundHandlerAdapter {
    /**
     * Default constructor.
     */
    PacketWriteHandler() {}

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
      Preconditions.checkState(acceptMessage(msg), "Incorrect response type.");
      RPCProtoMessage response = (RPCProtoMessage) msg;
      Protocol.Status status = response.getMessage().<Protocol.Response>getMessage().getStatus();

      if (!Status.isOk(status) && !Status.isCancelled(status)) {
        throw new IOException(String
            .format("Failed to write block %d from %s with status %s.", mId, mAddress,
                status.toString()));
      }
      mLock.lock();
      try {
        mDone = true;
        mDoneOrFailed.signal();
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
        mPacketWriteException = cause;
        mBufferNotFullOrFailed.signal();
        mDoneOrFailed.signal();
        mBufferEmptyOrFailed.signal();
      } finally {
        mLock.unlock();
      }

      ctx.close();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
      mLock.lock();
      try {
        if (mPacketWriteException == null) {
          mPacketWriteException = new IOException("Channel closed.");
        }
        mBufferNotFullOrFailed.signal();
        mDoneOrFailed.signal();
        mBufferEmptyOrFailed.signal();
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
        return ((RPCProtoMessage) msg).getMessage().getType() == ProtoMessage.Type.RESPONSE;
      }
      return false;
    }
  }

  /**
   * The netty channel future listener that is called when packet write is flushed.
   */
  private final class WriteListener implements ChannelFutureListener {
    private final long mPosToWriteUncommitted;

    /**
     * @param posToWriteUncommitted the pos to commit (i.e. update mPosToWrite)
     */
    WriteListener(long posToWriteUncommitted) {
      mPosToWriteUncommitted = posToWriteUncommitted;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
      if (!future.isSuccess()) {
        future.channel().close();
      }
      boolean shouldSendEOF = false;
      mLock.lock();
      try {
        Preconditions.checkState(mPosToWriteUncommitted - mPosToWrite <= PACKET_SIZE,
            "Some packet is not acked.");
        Preconditions.checkState(mPosToWriteUncommitted <= mLength);
        mPosToWrite = mPosToWriteUncommitted;

        if (future.cause() != null) {
          mPacketWriteException = future.cause();
          mDoneOrFailed.signal();
          mBufferNotFullOrFailed.signal();
          mBufferEmptyOrFailed.signal();
          return;
        }
        if (mPosToWrite == mPosToQueue) {
          mBufferEmptyOrFailed.signal();
        }
        if (!tooManyPacketsInFlight()) {
          mBufferNotFullOrFailed.signal();
        }
        if (mPosToWrite == mLength) {
          shouldSendEOF = true;
        }
      } finally {
        mLock.unlock();
      }
      if (shouldSendEOF) {
        sendEof();
      }
    }
  }
}

