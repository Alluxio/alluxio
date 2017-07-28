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
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.CanceledException;
import alluxio.exception.status.DeadlineExceededException;
import alluxio.exception.status.UnavailableException;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataNettyBufferV2;
import alluxio.proto.dataserver.Protocol;
import alluxio.proto.status.Status.PStatus;
import alluxio.resource.LockResource;
import alluxio.util.CommonUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

  private static final int MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS);
  private static final long WRITE_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  private final FileSystemContext mContext;
  private final Channel mChannel;
  private final WorkerNetAddress mAddress;
  private final long mLength;
  private final Protocol.WriteRequest mPartialRequest;
  private final long mPacketSize;

  private boolean mClosed;

  private ReentrantLock mLock = new ReentrantLock();
  /** The next pos to write to the channel. */
  @GuardedBy("mLock")
  private long mPosToWrite;
  /**
   * The next pos to queue to the netty buffer. mPosToQueue - mPosToWrite is the data sitting
   * in the netty buffer.
   */
  @GuardedBy("mLock")
  private long mPosToQueue;
  @GuardedBy("mLock")
  private Throwable mPacketWriteException;
  @GuardedBy("mLock")
  private boolean mDone;
  @GuardedBy("mLock")
  private boolean mEOFSent;
  @GuardedBy("mLock")
  private boolean mCancelSent;
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
   * @param address the data server address
   * @param id the block ID or UFS file ID
   * @param length the length of the block or file to write, set to Long.MAX_VALUE if unknown
   * @param tier the target tier
   * @param type the request type (block or UFS file)
   * @param packetSize the packet size
   */
  public NettyPacketWriter(FileSystemContext context, final WorkerNetAddress address, long id,
      long length, int tier, Protocol.RequestType type, long packetSize) throws IOException {
    this(context, address, length, Protocol.WriteRequest.newBuilder().setId(id).setTier(tier)
        .setType(type).buildPartial(), packetSize);
  }

  /**
   * Creates an instance of {@link NettyPacketWriter}.
   *
   * @param context the file system context
   * @param address the data server address
   * @param length the length of the block or file to write, set to Long.MAX_VALUE if unknown
   * @param partialRequest details of the write request which are constant for all requests
   * @param packetSize the packet size
   */
  public NettyPacketWriter(FileSystemContext context, final WorkerNetAddress address, long length,
      Protocol.WriteRequest partialRequest, long packetSize) throws IOException {
    mContext = context;
    mAddress = address;
    mLength = length;
    mPartialRequest = partialRequest;
    mPacketSize = packetSize;
    mChannel = mContext.acquireNettyChannel(address);
    mChannel.pipeline().addLast(new PacketWriteHandler());
  }

  @Override
  public long pos() {
    try (LockResource lr = new LockResource(mLock)) {
      return mPosToQueue;
    }
  }

  @Override
  public void writePacket(final ByteBuf buf) throws IOException {
    final long len;
    final long offset;
    try (LockResource lr = new LockResource(mLock)) {
      Preconditions.checkState(!mClosed && !mEOFSent && !mCancelSent);
      Preconditions.checkArgument(buf.readableBytes() <= mPacketSize);
      while (true) {
        if (mPacketWriteException != null) {
          Throwables.propagateIfPossible(mPacketWriteException, IOException.class);
          throw AlluxioStatusException.fromCheckedException(mPacketWriteException);
        }
        if (!tooManyPacketsInFlight()) {
          offset = mPosToQueue;
          mPosToQueue += buf.readableBytes();
          len = buf.readableBytes();
          break;
        }
        try {
          if (!mBufferNotFullOrFailed.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            throw new DeadlineExceededException(
                String.format("Timeout writing to %s for request %s.", mAddress, mPartialRequest));
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new CanceledException(e);
        }
      }
    } catch (IOException e) {
      buf.release();
      throw e;
    }

    Protocol.WriteRequest writeRequest = mPartialRequest.toBuilder().setOffset(offset).build();
    DataBuffer dataBuffer = new DataNettyBufferV2(buf);
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(writeRequest), dataBuffer))
        .addListener(new WriteListener(offset + len));
  }

  @Override
  public void cancel() {
    if (mClosed) {
      return;
    }
    sendCancel();
  }

  @Override
  public void flush() throws IOException {
    mChannel.flush();

    try (LockResource lr = new LockResource(mLock)) {
      while (true) {
        if (mPosToWrite == mPosToQueue) {
          return;
        }
        if (mPacketWriteException != null) {
          Throwables.propagateIfPossible(mPacketWriteException, IOException.class);
          throw AlluxioStatusException.fromCheckedException(mPacketWriteException);
        }
        if (!mBufferEmptyOrFailed.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          throw new DeadlineExceededException(
              String.format("Timeout flushing to %s for request %s.", mAddress, mPartialRequest));
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CanceledException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }

    sendEof();
    Future<?> closeFuture = null;
    mLock.lock();
    try {
      while (true) {
        if (mDone) {
          return;
        }
        try {
          if (mPacketWriteException != null) {
            closeFuture = mChannel.eventLoop().submit(new Runnable() {
              @Override
              public void run() {
                mChannel.close();
              }
            });
            throw new UnavailableException(mPacketWriteException);
          }
          if (!mDoneOrFailed
              .await(Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_WRITER_CLOSE_TIMEOUT_MS),
                  TimeUnit.MILLISECONDS)) {
            closeFuture = mChannel.eventLoop().submit(new Runnable() {
              @Override
              public void run() {
                mChannel.close();
              }
            });
            throw new DeadlineExceededException(String.format(
                "Timeout closing PacketWriter to %s for request %s.", mAddress, mPartialRequest));
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new CanceledException(e);
        }
      }
    } finally {
      mLock.unlock();
      if (closeFuture != null) {
        try {
          closeFuture.sync();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new CanceledException(e);
        }
      }
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
    return mPosToQueue - mPosToWrite >= MAX_PACKETS_IN_FLIGHT * mPacketSize;
  }

  /**
   * Sends an EOF packet to end the write request if the stream.
   */
  private void sendEof() {
    final long pos;
    try (LockResource lr = new LockResource(mLock)) {
      if (mEOFSent || mCancelSent) {
        return;
      }
      mEOFSent = true;
      pos = mPosToQueue;
    }
    // Write the EOF packet.
    Protocol.WriteRequest writeRequest =
        mPartialRequest.toBuilder().setOffset(pos).setEof(true).build();
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(writeRequest), null))
        .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  /**
   * Sends a CANCEL packet to end the write request if the stream.
   */
  private void sendCancel() {
    final long pos;
    try (LockResource lr = new LockResource(mLock)) {
      if (mEOFSent || mCancelSent) {
        return;
      }
      mCancelSent = true;
      pos = mPosToQueue;
    }
    // Write the EOF packet.
    Protocol.WriteRequest writeRequest =
        mPartialRequest.toBuilder().setOffset(pos).setCancel(true).build();
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(writeRequest), null))
        .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  @Override
  public int packetSize() {
    return (int) mPacketSize;
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
      Preconditions.checkState(acceptMessage(msg),
          String.format("Incorrect response type %s.", msg.toString()));
      RPCProtoMessage response = (RPCProtoMessage) msg;
      // Canceled is considered a valid status and handled in the writer. We avoid creating a
      // CanceledException as an optimization.
      if (response.getMessage().asResponse().getStatus() != PStatus.CANCELED) {
        CommonUtils.unwrapResponse(response.getMessage().asResponse());
      }

      try (LockResource lr = new LockResource(mLock)) {
        mDone = true;
        mDoneOrFailed.signal();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Exception caught while reading response from netty channel {}.", cause);
      try (LockResource lr = new LockResource(mLock)) {
        mPacketWriteException = cause;
        mBufferNotFullOrFailed.signal();
        mDoneOrFailed.signal();
        mBufferEmptyOrFailed.signal();
      }

      ctx.close();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
      try (LockResource lr = new LockResource(mLock)) {
        if (mPacketWriteException == null) {
          mPacketWriteException = new IOException("Channel closed.");
        }
        mBufferNotFullOrFailed.signal();
        mDoneOrFailed.signal();
        mBufferEmptyOrFailed.signal();
      }
      ctx.fireChannelUnregistered();
    }

    /**
     * @param msg the message received
     * @return true if this message should be processed
     */
    private boolean acceptMessage(Object msg) {
      if (msg instanceof RPCProtoMessage) {
        return ((RPCProtoMessage) msg).getMessage().isResponse();
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
      try (LockResource lr = new LockResource(mLock)) {
        Preconditions.checkState(mPosToWriteUncommitted - mPosToWrite <= mPacketSize,
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
      }
      if (shouldSendEOF) {
        sendEof();
      }
    }
  }
}

