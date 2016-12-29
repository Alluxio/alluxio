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
import alluxio.client.file.FileSystemContext;
import alluxio.network.protocol.RPCMessageDecoder;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.Status;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.protobuf.MessageLite;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
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
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static final boolean CANCEL_ENABLED =
      Configuration.getBoolean(PropertyKey.USER_NETWORK_NETTY_READER_CANCEL_ENABLED);
  private static final int MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS);
  private static final long READ_TIMEOUT_MS =
      Configuration.getLong(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  private final FileSystemContext mContext;
  private final Channel mChannel;
  private final Protocol.RequestType mRequestType;
  private final InetSocketAddress mAddress;
  private final long mId;
  private final long mStart;
  private final long mBytesToRead;

  // TODO(peis): Investigate whether we can remove this lock. The main reason to keep this lock
  // is to protect mPacketReaderException.
  private final ReentrantLock mLock = new ReentrantLock();
  @GuardedBy("mLock")
  private final Queue<ByteBuf> mPackets = new LinkedList<>();
  @GuardedBy("mLock")
  private Throwable mPacketReaderException = null;
  /** The condition is met when mPackets.size() > 0 or mPacketReaderException != null. */
  private final Condition mNotEmptyOrFailed = mLock.newCondition();

  /** The next pos to read. */
  private long mPosToRead;
  /** This is true only when an empty packet is received. */
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
   * @param type the request type (block or UFS file)
   * @throws IOException if it fails to acquire a netty channel
   */
  private NettyPacketReader(FileSystemContext context, InetSocketAddress address, long id,
      long offset, long len, long lockId, long sessionId, Protocol.RequestType type)
      throws IOException {
    Preconditions.checkArgument(offset >= 0 && len > 0);

    mContext = context;
    mAddress = address;
    mId = id;
    mStart = offset;
    mPosToRead = offset;
    mBytesToRead = len;
    mRequestType = type;

    mChannel = context.acquireNettyChannel(address);

    ChannelPipeline pipeline = mChannel.pipeline();
    if (!(pipeline.last() instanceof RPCMessageDecoder)) {
      throw new RuntimeException(String.format("Channel pipeline has unexpected handlers %s.",
          pipeline.last().getClass().getCanonicalName()));
    }
    mChannel.pipeline().addLast(new PacketReadHandler());

    Protocol.ReadRequest readRequest =
        Protocol.ReadRequest.newBuilder().setId(id).setOffset(offset).setLength(len)
            .setLockId(lockId).setSessionId(sessionId).setType(type).build();
    mChannel.writeAndFlush(new RPCProtoMessage(readRequest))
        .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  @Override
  public long pos() {
    return mPosToRead;
  }

  @Override
  public ByteBuf readPacket() throws IOException {
    Preconditions.checkState(!mClosed, "PacketReader is closed while reading packets.");
    ByteBuf buf = null;
    mLock.lock();
    try {
      while (true) {
        if (mDone) {
          return null;
        }
        if (mPacketReaderException != null) {
          throw new IOException(mPacketReaderException);
        }
        buf = mPackets.poll();

        // TODO(peis): Have a better criteria to resume so that we can have fewer state changes.
        if (!tooManyPacketsPending()) {
          resume();
        }
        // Queue is empty.
        if (buf == null) {
          try {
            if (!mNotEmptyOrFailed.await(READ_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
              throw new IOException(
                  String.format("Timeout while reading packet from block %d @ %s.", mId, mAddress));
            }
          } catch (InterruptedException e) {
            throw Throwables.propagate(e);
          }
        } else {
          if (buf.readableBytes() == 0) {
            buf.release();
            mDone = true;
            return null;
          }
          mPosToRead += buf.readableBytes();
          Preconditions.checkState(mPosToRead - mStart <= mBytesToRead);
          return buf;
        }
      }
    } catch (Throwable e) {
      if (buf != null) {
        buf.release();
      }
      throw e;
    } finally {
      mLock.unlock();
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
        if (!CANCEL_ENABLED) {
          mChannel.close().sync();
          return;
        }
        if (remaining() > 0) {
          Protocol.ReadRequest cancelRequest =
              Protocol.ReadRequest.newBuilder().setId(mId).setCancel(true).setType(mRequestType)
                  .build();
          mChannel.writeAndFlush(new RPCProtoMessage(cancelRequest))
              .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        }
      } catch (InterruptedException e) {
        mChannel.close();
        throw Throwables.propagate(e);
      }

      while (true) {
        try {
          ByteBuf buf = readPacket();
          // A null packet indicates the end of the stream.
          if (buf == null) {
            return;
          }
          buf.release();
        } catch (IOException e) {
          LOG.warn("Failed to close the NettyBlockReader (block: {}, address: {}).",
              mId, mAddress, e);
          try {
            mChannel.close().sync();
          } catch (InterruptedException ee) {
            throw Throwables.propagate(ee);
          }
          return;
        }
      }
    } finally {
      if (mChannel.isOpen()) {
        Preconditions.checkState(mChannel.pipeline().last() instanceof PacketReadHandler);
        mChannel.pipeline().removeLast();

        // Make sure "autoread" is on before realsing the channel.
        resume();
      }
      mContext.releaseNettyChannel(mAddress, mChannel);
      mClosed = true;
    }
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
    public PacketReadHandler() {}

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      Preconditions.checkState(acceptMessage(msg), "Incorrect response type %s, %s.",
          msg.getClass().getCanonicalName(), msg);

      RPCProtoMessage response = (RPCProtoMessage) msg;
      Protocol.Status status = ((Protocol.Response) response.getMessage()).getStatus();
      if (!Status.isOk(status)) {
        ctx.fireExceptionCaught(new IOException(String
            .format("Failed to read block %d from %s with status %s.", mId, mAddress,
                status.toString())));
      }
      mLock.lock();
      try {
        Preconditions.checkState(mPacketReaderException == null);
        DataBuffer dataBuffer = response.getPayloadDataBuffer();
        ByteBuf buf;
        if (dataBuffer == null) {
          buf = ctx.alloc().buffer(0, 0);
        } else {
          Preconditions.checkState(dataBuffer.getLength() > 0);
          assert dataBuffer.getNettyOutput() instanceof ByteBuf;
          buf = (ByteBuf) dataBuffer.getNettyOutput();
        }
        mPackets.offer(buf);
        mNotEmptyOrFailed.signal();

        if (tooManyPacketsPending()) {
          pause();
        }
      } finally {
        mLock.unlock();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Exception caught while reading response from netty channel.", cause);
      mLock.lock();
      try {
        mPacketReaderException = cause;
        mNotEmptyOrFailed.signal();
      } finally {
        mLock.unlock();
      }
      ctx.close();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
      mLock.lock();
      try {
        if (mPacketReaderException == null) {
          mPacketReaderException = new IOException("ChannelClosed");
        }
        mNotEmptyOrFailed.signal();
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
   * Pauses the underlying packet reader.
   */
  private void pause() {
    mChannel.config().setAutoRead(false);
  }

  /**
   * Resumes the underlying packet reader.
   */
  private void resume() {
    mChannel.config().setAutoRead(true);
    mChannel.read();
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
    private final Protocol.RequestType mRequestType;

    /**
     * Creates an instance of {@link NettyPacketReader.Factory} for block reads.
     *
     * @param context the file system context
     * @param address the worker address
     * @param id the block ID or UFS ID
     * @param lockId the lock ID
     * @param sessionId the session ID
     * @param type the request type
     */
    public Factory(FileSystemContext context, InetSocketAddress address, long id, long lockId,
        long sessionId, Protocol.RequestType type) {
      mContext = context;
      mAddress = address;
      mId = id;
      mLockId = lockId;
      mSessionId = sessionId;
      mRequestType = type;
    }

    @Override
    public PacketReader create(long offset, long len) throws IOException {
      return new NettyPacketReader(mContext, mAddress, mId, offset, len, mLockId, mSessionId,
          mRequestType);
    }
  }
}

