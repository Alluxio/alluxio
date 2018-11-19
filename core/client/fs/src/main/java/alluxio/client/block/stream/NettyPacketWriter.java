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
import alluxio.client.WriteType;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.options.OutStreamOptions;
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
import alluxio.util.proto.ProtoUtils;
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
  private static final long CLOSE_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_WRITER_CLOSE_TIMEOUT_MS);
  /** Uses a long flush timeout since flush in S3 streaming upload may take a long time. */
  private static final long FLUSH_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.USER_NETWORK_NETTY_WRITER_FLUSH_TIMEOUT);

  private final FileSystemContext mContext;
  private final Channel mChannel;
  private final WorkerNetAddress mAddress;
  private final long mLength;
  private final Protocol.WriteRequest mPartialRequest;
  private final long mPacketSize;

  private boolean mClosed;

  /**
   * Uses to gurantee the operation ordering.
   *
   * NOTE: {@link Channel#writeAndFlush(Object)} is async.
   * Netty I/O thread executes the {@link ChannelFutureListener#operationComplete(Future)}
   * before writing any new message to the wire, which may introduce another layer of ordering.
   */
  private final ReentrantLock mLock = new ReentrantLock();

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
  private final Condition mDoneOrFailed = mLock.newCondition();
  /** This condition is met if mPacketWriteException != null or the buffer is not full. */
  private final Condition mBufferNotFullOrFailed = mLock.newCondition();
  /** This condition is met if there is nothing in the netty buffer. */
  private final Condition mBufferEmptyOrFailed = mLock.newCondition();
  /** This condition is met if mPacketWriteException != null or flush is completed. */
  private final Condition mFlushedOrFailed = mLock.newCondition();

  /**
   * @param context the file system context
   * @param address the data server address
   * @param id the block or UFS ID
   * @param length the length of the block or file to write, set to Long.MAX_VALUE if unknown
   * @param type type of the write request
   * @param options the options of the output stream
   * @return an instance of {@link NettyPacketWriter}
   */
  public static NettyPacketWriter create(FileSystemContext context, WorkerNetAddress address,
      long id, long length, Protocol.RequestType type, OutStreamOptions options)
      throws IOException {
    long packetSize =
        Configuration.getBytes(PropertyKey.USER_NETWORK_NETTY_WRITER_PACKET_SIZE_BYTES);
    Channel nettyChannel = context.acquireNettyChannel(address);
    return new NettyPacketWriter(context, address, id, length, packetSize, type, options,
        nettyChannel);
  }

  /**
   * Creates an instance of {@link NettyPacketWriter}.
   *
   * @param context the file system context
   * @param address the data server address
   * @param id the block or UFS file Id
   * @param length the length of the block or file to write, set to Long.MAX_VALUE if unknown
   * @param packetSize the packet size
   * @param type type of the write request
   * @param options details of the write request which are constant for all requests
   * @param channel netty channel
   */
  private NettyPacketWriter(FileSystemContext context, final WorkerNetAddress address, long id,
      long length, long packetSize, Protocol.RequestType type, OutStreamOptions options,
      Channel channel) {
    mContext = context;
    mAddress = address;
    mLength = length;
    Protocol.WriteRequest.Builder builder =
        Protocol.WriteRequest.newBuilder().setId(id).setTier(options.getWriteTier()).setType(type);
    if (type == Protocol.RequestType.UFS_FILE) {
      Protocol.CreateUfsFileOptions ufsFileOptions =
          Protocol.CreateUfsFileOptions.newBuilder().setUfsPath(options.getUfsPath())
              .setOwner(options.getOwner()).setGroup(options.getGroup())
              .setMode(options.getMode().toShort()).setMountId(options.getMountId())
              .setAcl(ProtoUtils.toProto(options.getAcl()))
              .build();
      builder.setCreateUfsFileOptions(ufsFileOptions);
    }
    // two cases to use UFS_FALLBACK_BLOCK endpoint:
    // (1) this writer is created by the fallback of a short-circuit writer, or
    boolean alreadyFallback = type == Protocol.RequestType.UFS_FALLBACK_BLOCK;
    // (2) the write type is async when UFS tier is enabled.
    boolean possibleToFallback = type == Protocol.RequestType.ALLUXIO_BLOCK
        && options.getWriteType() == WriteType.ASYNC_THROUGH
        && Configuration.getBoolean(PropertyKey.USER_FILE_UFS_TIER_ENABLED);
    if (alreadyFallback || possibleToFallback) {
      // Overwrite to use the fallback-enabled endpoint in case (2)
      builder.setType(Protocol.RequestType.UFS_FALLBACK_BLOCK);
      Protocol.CreateUfsBlockOptions ufsBlockOptions =
          Protocol.CreateUfsBlockOptions.newBuilder().setMountId(options.getMountId())
          .setFallback(alreadyFallback).build();
      builder.setCreateUfsBlockOptions(ufsBlockOptions);
    }
    mPartialRequest = builder.buildPartial();
    mPacketSize = packetSize;
    mChannel = channel;
    mChannel.pipeline().addLast(new PacketWriteResponseHandler());
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
                String.format("Timeout writing to %s for request %s after %dms.",
                    mAddress, mPartialRequest, WRITE_TIMEOUT_MS));
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

  /**
   * Notifies the server UFS fallback endpoint to start writing a new block by resuming the given
   * number of bytes from block store.
   *
   * @param pos number of bytes already written to block store
   */
  public void writeFallbackInitPacket(long pos) {
    Preconditions.checkState(mPartialRequest.getType()
        == Protocol.RequestType.UFS_FALLBACK_BLOCK);
    Protocol.CreateUfsBlockOptions ufsBlockOptions = mPartialRequest.getCreateUfsBlockOptions()
        .toBuilder().setBytesInBlockStore(pos).build();
    Protocol.WriteRequest writeRequest = mPartialRequest.toBuilder().setOffset(0)
        .setCreateUfsBlockOptions(ufsBlockOptions).build();
    try (LockResource lr = new LockResource(mLock)) {
      mPosToQueue = pos;
    }
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(writeRequest), null))
        .addListener(new WriteListener(pos, true));
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
      if (mEOFSent || mCancelSent || mPosToQueue == 0) {
        return;
      }
      while (mPosToWrite != mPosToQueue) {
        if (mPacketWriteException != null) {
          Throwables.propagateIfPossible(mPacketWriteException, IOException.class);
          throw AlluxioStatusException.fromCheckedException(mPacketWriteException);
        }
        if (!mBufferEmptyOrFailed.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          throw new DeadlineExceededException(
              String.format("Timeout flushing to %s for request %s after %dms.",
                  mAddress, mPartialRequest, WRITE_TIMEOUT_MS));
        }
      }
      if (mEOFSent || mCancelSent) {
        return;
      }
      Protocol.WriteRequest writeRequest =
          mPartialRequest.toBuilder().setOffset(mPosToQueue).setFlush(true).build();
      mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(writeRequest), null))
          .addListener(new FlushOrEofOrCancelListener());
      if (!mFlushedOrFailed.await(FLUSH_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        throw new DeadlineExceededException(
            String.format("Timeout flush to %s for request %s after %dms.",
                mAddress, mPartialRequest, FLUSH_TIMEOUT_MS));
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
            throw new UnavailableException(
                "Failed to write data packet due to " + mPacketWriteException.getMessage(),
                mPacketWriteException);
          }
          if (!mDoneOrFailed.await(CLOSE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            closeFuture = mChannel.eventLoop().submit(new Runnable() {
              @Override
              public void run() {
                mChannel.close();
              }
            });
            throw new DeadlineExceededException(String.format(
                "Timeout closing PacketWriter to %s for request %s after %dms.",
                mAddress, mPartialRequest, CLOSE_TIMEOUT_MS));
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
   * Sends an EOF packet to end the write request of the stream.
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
        .addListener(new FlushOrEofOrCancelListener());
  }

  /**
   * Sends a CANCEL packet to end the write request of the stream.
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
    // Write the CANCEL packet.
    Protocol.WriteRequest writeRequest =
        mPartialRequest.toBuilder().setOffset(pos).setCancel(true).build();
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(writeRequest), null))
        .addListener(new FlushOrEofOrCancelListener());
  }

  @Override
  public int packetSize() {
    return (int) mPacketSize;
  }

  /**
   * Updates the channel exception to be the given exception e, or adds e to suppressed exceptions.
   *
   * @param e Exception received
   */
  @GuardedBy("mLock")
  private void updateException(Throwable e) {
    if (mPacketWriteException == null || mPacketWriteException == e) {
      mPacketWriteException = e;
    } else {
      mPacketWriteException.addSuppressed(e);
    }
  }

  /**
   * The netty handler that handles netty write response.
   */
  private final class PacketWriteResponseHandler extends ChannelInboundHandlerAdapter {
    /**
     * Default constructor.
     */
    PacketWriteResponseHandler() {}

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
      Preconditions.checkState(acceptMessage(msg), "Incorrect response type %s.", msg);
      RPCProtoMessage message = (RPCProtoMessage) msg;
      Protocol.Response response = message.getMessage().asResponse();
      // Canceled is considered a valid status and handled in the writer. We avoid creating a
      // CanceledException as an optimization.
      if (response.getStatus() != PStatus.CANCELED) {
        CommonUtils.unwrapResponseFrom(response, ctx.channel());
      }
      try (LockResource lr = new LockResource(mLock)) {
        if (response.getMessage().equals(Constants.FLUSHED_SIGNAL)) {
          mFlushedOrFailed.signal();
        } else {
          mDone = true;
          mDoneOrFailed.signal();
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Exception is caught when writing block {} to channel {}:",
          mPartialRequest.getId(), ctx.channel(), cause);
      try (LockResource lr = new LockResource(mLock)) {
        updateException(cause);
        mBufferNotFullOrFailed.signal();
        mDoneOrFailed.signal();
        mBufferEmptyOrFailed.signal();
        mFlushedOrFailed.signal();
      }
      ctx.close();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
      LOG.warn("Channel {} is closed.", ctx.channel());
      try (LockResource lr = new LockResource(mLock)) {
        if (!mDone) {
          updateException(new IOException(String
              .format("Channel %s is closed when writing block %d.", ctx.channel(),
                  mPartialRequest.getId())));
          mBufferNotFullOrFailed.signal();
          mDoneOrFailed.signal();
          mBufferEmptyOrFailed.signal();
          mFlushedOrFailed.signal();
        }
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
   * The netty channel future listener that is called when a packet write is complete.
   */
  private final class WriteListener implements ChannelFutureListener {
    private final long mPosToWriteUncommitted;
    private final boolean mIsUfsInit;

    WriteListener(long posToWriteUncommitted, boolean isUfsInit) {
      mPosToWriteUncommitted = posToWriteUncommitted;
      mIsUfsInit = isUfsInit;
    }

    /**
     * @param posToWriteUncommitted the pos to commit (i.e. update mPosToWrite)
     */
    WriteListener(long posToWriteUncommitted) {
      mPosToWriteUncommitted = posToWriteUncommitted;
      mIsUfsInit = false;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
      if (!future.isSuccess()) {
        future.channel().close();
      }
      try (LockResource lr = new LockResource(mLock)) {
        Preconditions.checkState(
            mIsUfsInit || mPosToWriteUncommitted - mPosToWrite <= mPacketSize,
            "Some packet is not acked.");
        Preconditions.checkState(mPosToWriteUncommitted <= mLength);
        Preconditions.checkState(mPosToWriteUncommitted <= mLength);
        mPosToWrite = mPosToWriteUncommitted;

        if (future.cause() != null) {
          updateException(future.cause());
          mDoneOrFailed.signal();
          mBufferNotFullOrFailed.signal();
          mBufferEmptyOrFailed.signal();
          mFlushedOrFailed.signal();
          return;
        }
        if (mPosToWrite == mPosToQueue) {
          mBufferEmptyOrFailed.signal();
        }
        if (!tooManyPacketsInFlight()) {
          mBufferNotFullOrFailed.signal();
        }
        if (mPosToWrite == mLength) {
          sendEof();
        }
      }
    }
  }

  /**
   * The netty channel future listener that is called when a FLUSH or EOF or CANCEL is complete.
   */
  private final class FlushOrEofOrCancelListener implements ChannelFutureListener {
    /**
     * Constructor.
     */
    FlushOrEofOrCancelListener() {}

    @Override
    public void operationComplete(ChannelFuture future) {
      if (!future.isSuccess()) {
        future.channel().close();
        try (LockResource lr = new LockResource(mLock)) {
          updateException(future.cause());
          mDoneOrFailed.signal();
          mBufferNotFullOrFailed.signal();
          mBufferEmptyOrFailed.signal();
          mFlushedOrFailed.signal();
        }
      }
    }
  }
}

