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

package alluxio.client.file.dora;

import alluxio.client.file.FileSystemContext;
import alluxio.client.file.dora.PartialReadException.CauseType;
import alluxio.client.file.dora.event.ResponseEvent;
import alluxio.client.file.dora.event.ResponseEventContext;
import alluxio.client.file.dora.event.ResponseEventFactory;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.CancelledException;
import alluxio.exception.status.UnavailableException;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.CommonUtils;
import alluxio.util.network.NettyUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.util.proto.ProtoUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import io.grpc.Status;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Positioned Netty data reader.
 */
public class NettyDataReader implements DoraDataReader {
  private static final Logger LOG = LoggerFactory.getLogger(NettyDataReader.class);
  private final int mMaxPacketsInFlight;
  private final long mReadTimeoutMs;
  private final FileSystemContext mContext;
  private final WorkerNetAddress mAddress;
  private final Supplier<Protocol.ReadRequest.Builder> mRequestBuilder;
  private boolean mClosed = false;

  NettyDataReader(FileSystemContext context, WorkerNetAddress address,
                  Protocol.ReadRequest.Builder requestBuilder) {
    mContext = context;
    AlluxioConfiguration conf = context.getClusterConf();
    mReadTimeoutMs = conf.getMs(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);
    mMaxPacketsInFlight = conf.getInt(PropertyKey.USER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS);
    mAddress = address;
    // clone the builder so that the initial values does not get overridden
    mRequestBuilder = requestBuilder::clone;
  }

  @Override
  public int read(long offset, WritableByteChannel outChannel, int length)
      throws PartialReadException {
    Preconditions.checkState(!mClosed, "Reader is closed");
    final Channel channel;
    try {
      channel = mContext.acquireNettyChannel(mAddress);
    } catch (IOException ioe) {
      throw new PartialReadException(length, 0, CauseType.TRANSPORT_ERROR, ioe);
    }
    BlockingQueue<ResponseEvent> queue = new LinkedBlockingQueue<>();
    channel.pipeline().addLast(new PacketReadHandler(queue, mMaxPacketsInFlight));

    boolean needsCleanup = false;
    try {
      Protocol.ReadRequest readRequest = mRequestBuilder
          .get()
          .setOffset(offset)
          .setLength(length)
          .clearCancel()
          .build();
      channel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(readRequest)))
          .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
      int bytesRead = readInternal(length, channel, queue, outChannel);
      // if we reach here, the read was successful, so no clean up is needed
      needsCleanup = false;
      return bytesRead;
    } catch (PartialReadException e) {
      LOG.debug("Read was incomplete: {} bytes requested, {} bytes actually read",
          length, e.getBytesRead(), e);
      switch (e.getCauseType()) {
        // need to send a cancel if the error originates from client side
        case INTERRUPT:
        case TIMEOUT:
        case OUTPUT:
        case TRANSPORT_ERROR:
          needsCleanup = true;
          break;
        default:
          // otherwise, the error comes from the server side, we assume
          // the server has taken care of itself and cancelled its processing of the ongoing request
      }
      throw e;
    } catch (RuntimeException e) {
      // runtime exceptions are client side programming errors, and should do clean up
      needsCleanup = true;
      throw e;
    } finally {
      if (needsCleanup && channel.isOpen()) {
        Protocol.ReadRequest cancelRequest = mRequestBuilder.get().setCancel(true).build();
        channel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(cancelRequest)))
            .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        try {
          // discard any remaining messages
          readInternal(length, channel, queue, new DiscardingWritableChannel());
        } catch (PartialReadException e) {
          LOG.warn("Failed to close the NettyDataReader (address: {}) with exception {}.",
              mAddress, e.getCause().getMessage());
          CommonUtils.closeChannel(channel);
        }
      }
      if (channel.isOpen()) {
        channel.pipeline().removeLast();
        // Make sure "autoread" is on before releasing the channel.
        NettyUtils.enableAutoRead(channel);
      }
      mContext.releaseNettyChannel(mAddress, channel);
    }
  }

  @Override
  public void readFully(long offset, WritableByteChannel outChannel, int length)
      throws PartialReadException {
    int totalBytesRead = 0;
    while (totalBytesRead < length) {
      int bytesToRead = length - totalBytesRead;
      try {
        // todo(bowen): adjust timeout on each retry to account for the total expected timeout
        int bytesRead = read(offset + totalBytesRead, outChannel, bytesToRead);
        if (bytesRead < 0) { // eof
          break;
        }
        offset += bytesRead;
        totalBytesRead += bytesRead;
      } catch (PartialReadException e) {
        int bytesRead = e.getBytesRead();
        offset += bytesRead;
        totalBytesRead += bytesRead;
        if (bytesRead == 0) {
          // the last attempt did not make any progress, giving up
          LOG.warn("Giving up read due to no progress can be made: {} ({}), "
                  + "{} bytes requested, {} bytes read so far",
              e.getCauseType(), e.getCause().getMessage(), length, totalBytesRead);
          throw new PartialReadException(length, totalBytesRead, e.getCauseType(), e.getCause());
        }
        // decide if the error is retryable
        switch (e.getCauseType()) {
          // error cases that cannot be retried
          case SERVER_ERROR:
          case OUTPUT:
          case CANCELLED:
            LOG.warn("Giving up read due to unretryable error: {} ({}), "
                    + "{} bytes requested, {} bytes read so far",
                e.getCauseType(), e.getCause().getMessage(), length, totalBytesRead);
            throw new PartialReadException(length, totalBytesRead, e.getCauseType(), e.getCause());
          default:
            LOG.debug("Retrying read on exception {}, current progress: {} read / {} requested",
                e.getCauseType(), totalBytesRead, length, e.getCause());
        }
      }
    }
    if (totalBytesRead < length) {
      throw new PartialReadException(length, totalBytesRead, CauseType.EARLY_EOF,
          new EOFException(String.format("Unexpected EOF: %d bytes wanted, "
              + "%d bytes available", length, totalBytesRead)));
    }
  }

  private int readInternal(int length, Channel channel,
                           BlockingQueue<ResponseEvent> responseEventQueue,
                           WritableByteChannel outChannel)
      throws PartialReadException {
    ResponseEventContext responseEventContext =
        new ResponseEventContext(length, 0, outChannel);

    ResponseEvent responseEvent;
    while (responseEventContext.getBytesRead() < responseEventContext.getLengthToRead()) {
      if (!tooManyPacketsPending(responseEventQueue, mMaxPacketsInFlight)) {
        NettyUtils.enableAutoRead(channel);
      }
      try {
        responseEvent = responseEventQueue.poll(mReadTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PartialReadException(
            length, responseEventContext.getBytesRead(), CauseType.INTERRUPT, e);
      }
      if (responseEvent == null) {
        throw new PartialReadException(
            length, responseEventContext.getBytesRead(), CauseType.TIMEOUT,
            new TimeoutException(String.format("Timeout to read from %s.", channel)));
      }
      responseEvent.postProcess(responseEventContext);
    }
    if (responseEventContext.getBytesRead() == 0) {
      return -1;
    }
    return responseEventContext.getBytesRead();
  }

  @Override
  public void close() throws IOException {
    if (mClosed) {
      return;
    }
    mClosed = true;
  }

  /**
   * @return true if there are too many packets pending
   */
  private static boolean tooManyPacketsPending(
      BlockingQueue<ResponseEvent> queue, int maxPacketsInFlight) {
    return queue.size() >= maxPacketsInFlight;
  }

  /**
   * The netty handler that reads packets from the channel.
   */
  private static class PacketReadHandler extends ChannelInboundHandlerAdapter {

    private final ResponseEventFactory mResponseEventFactory =
        ResponseEventFactory.getResponseEventFactory();

    private final BlockingQueue<ResponseEvent> mResponseEventQueue;

    private final int mMaxPacketsInFlight;

    PacketReadHandler(BlockingQueue<ResponseEvent> queue, int maxPacketsInFlight) {
      mResponseEventQueue = queue;
      mMaxPacketsInFlight = maxPacketsInFlight;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
      // Precondition check is not used here to avoid calling msg.getClass().getCanonicalName()
      // all the time.
      if (!(msg instanceof RPCProtoMessage)) {
        throw new IllegalStateException(String
            .format("Incorrect response type %s, %s.", msg.getClass().getCanonicalName(), msg));
      }

      ResponseEvent responseEvent;
      RPCProtoMessage rpcProtoMessage = (RPCProtoMessage) msg;
      ProtoMessage message = rpcProtoMessage.getMessage();
      if (message.isReadResponse()) {
        Preconditions.checkState(
            message.asReadResponse().getType()
                == Protocol.ReadResponse.Type.UFS_READ_HEARTBEAT);
        responseEvent = mResponseEventFactory.createUfsReadHeartBeatResponseEvent();
      } else if (message.isResponse()) {
        Protocol.Response response = message.asResponse();
        // Canceled is considered a valid status and handled in the reader. We avoid creating a
        // CanceledException as an optimization.
        switch (response.getStatus()) {
          case CANCELLED:
            responseEvent =
                mResponseEventFactory.createCancelledResponseEvent(
                    new CancelledException("Server canceled: " + response.getMessage()));
            break;
          case OK:
            DataBuffer dataBuffer = rpcProtoMessage.getPayloadDataBuffer();
            if (dataBuffer != null) {
              responseEvent = mResponseEventFactory.createDataResponseEvent(dataBuffer);
            } else {
              // an empty response indicates the worker has done sending data
              responseEvent = mResponseEventFactory.createEofResponseEvent();
            }
            break;
          default:
            Status status = ProtoUtils.fromProto(response.getStatus());
            AlluxioStatusException error = AlluxioStatusException.from(
                status.withDescription(String.format("Error from server %s: %s",
                    ctx.channel().remoteAddress(), response.getMessage())));
            responseEvent = mResponseEventFactory.createServerErrorResponseEvent(error);
        }
      } else {
        throw new IllegalStateException(
            String.format("Incorrect response type %s.", message));
      }

      if (tooManyPacketsPending(mResponseEventQueue, mMaxPacketsInFlight)) {
        NettyUtils.disableAutoRead(ctx.channel());
      }
      mResponseEventQueue.offer(responseEvent);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Exception is caught while reading data from channel {}:",
          ctx.channel(), cause);
      ResponseEvent responseEvent = mResponseEventFactory.createTransportResponseEvent(cause);
      mResponseEventQueue.offer(responseEvent);
      ctx.close();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
      LOG.warn("Channel is closed while reading data from channel {}.", ctx.channel());
      ResponseEvent responseEvent = mResponseEventFactory.createTransportResponseEvent(
          new UnavailableException(String.format("Channel %s is closed.", ctx.channel())));
      mResponseEventQueue.offer(responseEvent);
      ctx.fireChannelUnregistered();
    }
  }

  private static class DiscardingWritableChannel implements WritableByteChannel {
    private boolean mClosed = false;

    @Override
    public int write(ByteBuffer src) throws IOException {
      return src.remaining();
    }

    @Override
    public boolean isOpen() {
      return !mClosed;
    }

    @Override
    public void close() throws IOException {
      mClosed = true;
    }
  }
}
