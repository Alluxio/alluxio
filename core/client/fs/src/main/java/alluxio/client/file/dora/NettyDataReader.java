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

import static alluxio.client.file.dora.NettyDataReader.Payload.Type.CANCEL;
import static alluxio.client.file.dora.NettyDataReader.Payload.Type.DATA;
import static alluxio.client.file.dora.NettyDataReader.Payload.Type.EOF;
import static alluxio.client.file.dora.NettyDataReader.Payload.Type.HEART_BEAT;
import static alluxio.client.file.dora.NettyDataReader.Payload.Type.SERVER_ERROR;
import static alluxio.client.file.dora.NettyDataReader.Payload.Type.TRANSPORT_ERROR;

import alluxio.client.file.FileSystemContext;
import alluxio.client.file.dora.PartialReadException.CauseType;
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
import io.netty.buffer.ByteBuf;
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
    BlockingQueue<Payload<?>> queue = new LinkedBlockingQueue<>();
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
      BlockingQueue<Payload<?>> queue, WritableByteChannel outChannel) throws PartialReadException {
    int bytesRead = 0;

    Payload<?> payload;
    while (bytesRead < length) {
      if (!tooManyPacketsPending(queue, mMaxPacketsInFlight)) {
        NettyUtils.enableAutoRead(channel);
      }
      try {
        payload = queue.poll(mReadTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PartialReadException(length, bytesRead, CauseType.INTERRUPT, e);
      }
      if (payload == null) {
        throw new PartialReadException(length, bytesRead, CauseType.TIMEOUT,
            new TimeoutException(String.format("Timeout to read from %s.", channel)));
      }
      final Payload.Type<?> type = payload.type();
      if (type == HEART_BEAT) {
        continue;
      } else if (type == CANCEL) {
        CancelledException exception = payload.payload(CANCEL);
        throw new PartialReadException(length, bytesRead, CauseType.CANCELLED, exception);
      } else if (type == EOF) {
        break;
      } else if (type == SERVER_ERROR) {
        AlluxioStatusException exception = payload.payload(SERVER_ERROR);
        throw new PartialReadException(length, bytesRead, CauseType.SERVER_ERROR, exception);
      } else if (type == TRANSPORT_ERROR) {
        Throwable exception = payload.payload(TRANSPORT_ERROR);
        throw new PartialReadException(length, bytesRead, CauseType.TRANSPORT_ERROR, exception);
      } else if (type == DATA) {
        ByteBuf byteBuf = payload.payload(DATA).slice();
        int readableBytes = byteBuf.readableBytes();
        int sliceEnd = Math.min(readableBytes, length - bytesRead);
        // todo(bowen): handle case where ByteBuf does not support getting a bytebuffer
        ByteBuffer toWrite = byteBuf.nioBuffer(0, sliceEnd);
        try {
          bytesRead += outChannel.write(toWrite);
        } catch (IOException ioe) {
          throw new PartialReadException(length, bytesRead, CauseType.OUTPUT, ioe);
        } finally {
          // previously retained in packet read handler
          byteBuf.release();
        }
      } else {
        throw new IllegalStateException("Invalid packet type received: " + type);
      }
    }
    if (bytesRead == 0) {
      return -1;
    }
    return bytesRead;
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
      BlockingQueue<Payload<?>> queue, int maxPacketsInFlight) {
    return queue.size() >= maxPacketsInFlight;
  }

  /**
   * The netty handler that reads packets from the channel.
   */
  private static class PacketReadHandler extends ChannelInboundHandlerAdapter {
    private final BlockingQueue<Payload<?>> mPackets;
    private final int mMaxPacketsInFlight;

    PacketReadHandler(BlockingQueue<Payload<?>> queue, int maxPacketsInFlight) {
      mPackets = queue;
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

      Payload<?> payload;
      RPCProtoMessage rpcProtoMessage = (RPCProtoMessage) msg;
      ProtoMessage message = rpcProtoMessage.getMessage();
      if (message.isReadResponse()) {
        Preconditions.checkState(
            message.asReadResponse().getType() == Protocol.ReadResponse.Type.UFS_READ_HEARTBEAT);
        payload = Payload.ufsReadHeartBeat();
      } else if (message.isResponse()) {
        Protocol.Response response = message.asResponse();
        // Canceled is considered a valid status and handled in the reader. We avoid creating a
        // CanceledException as an optimization.
        switch (response.getStatus()) {
          case CANCELLED:
            payload = Payload.cancel(
                new CancelledException("Server canceled: " + response.getMessage()));
            break;
          case OK:
            DataBuffer dataBuffer = rpcProtoMessage.getPayloadDataBuffer();
            if (dataBuffer != null) {
              Preconditions.checkState(dataBuffer.getNettyOutput() instanceof ByteBuf,
                  "dataBuffer.getNettyOutput is not of type ByteBuf");
              ByteBuf data = (ByteBuf) dataBuffer.getNettyOutput();
              // need to retain this buffer so that it won't get recycled before we are able to
              // process it
              // will be released in reader
              payload = Payload.data(data.retain());
            } else {
              // an empty response indicates the worker has done sending data
              payload = Payload.eof();
            }
            break;
          default:
            Status status = ProtoUtils.fromProto(response.getStatus());
            AlluxioStatusException error = AlluxioStatusException.from(
                status.withDescription(String.format("Error from server %s: %s",
                    ctx.channel().remoteAddress(), response.getMessage())));
            payload = Payload.serverError(error);
        }
      } else {
        throw new IllegalStateException(
            String.format("Incorrect response type %s.", message));
      }

      if (tooManyPacketsPending(mPackets, mMaxPacketsInFlight)) {
        NettyUtils.disableAutoRead(ctx.channel());
      }
      mPackets.offer(payload);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Exception is caught while reading data from channel {}:",
          ctx.channel(), cause);
      Payload<?> payload = Payload.transportError(cause);
      mPackets.offer(payload);
      ctx.close();
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
      LOG.warn("Channel is closed while reading data from channel {}.", ctx.channel());
      Payload<?> payload = Payload.transportError(
          new UnavailableException(String.format("Channel %s is closed.", ctx.channel())));
      mPackets.offer(payload);
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

  static class Payload<T extends Payload.Type<?>> {
    interface Type<P> {
      Class<P> payloadType();

      Data DATA = new Data();
      UfsReadHeartBeat HEART_BEAT = new UfsReadHeartBeat();
      Eof EOF = new Eof();
      Cancel CANCEL = new Cancel();
      ServerError SERVER_ERROR = new ServerError();
      TransportError TRANSPORT_ERROR = new TransportError();
    }

    static class Data implements Type<ByteBuf> {
      @Override
      public Class<ByteBuf> payloadType() {
        return ByteBuf.class;
      }
    }

    static class UfsReadHeartBeat implements Type<Void> {
      @Override
      public Class<Void> payloadType() {
        return Void.TYPE;
      }
    }

    static class Eof implements Type<Void> {
      @Override
      public Class<Void> payloadType() {
        return Void.TYPE;
      }
    }

    static class Cancel implements Type<CancelledException> {
      @Override
      public Class<CancelledException> payloadType() {
        return CancelledException.class;
      }
    }

    static class ServerError implements Type<AlluxioStatusException> {
      @Override
      public Class<AlluxioStatusException> payloadType() {
        return AlluxioStatusException.class;
      }
    }

    static class TransportError implements Type<Throwable> {
      @Override
      public Class<Throwable> payloadType() {
        return Throwable.class;
      }
    }

    private final T mType;
    private final Object mPayload;

    private Payload(T type, Object payload) {
      Preconditions.checkArgument((type.payloadType() == Void.TYPE && payload == null)
          || type.payloadType().isInstance(payload));
      mType = type;
      mPayload = payload;
    }

    static Payload<Data> data(ByteBuf buf) {
      return new Payload<>(Type.DATA, buf);
    }

    static Payload<UfsReadHeartBeat> ufsReadHeartBeat() {
      return new Payload<>(HEART_BEAT, null);
    }

    static Payload<Eof> eof() {
      return new Payload<>(EOF, null);
    }

    static Payload<Cancel> cancel(CancelledException exception) {
      return new Payload<>(CANCEL, exception);
    }

    static Payload<ServerError> serverError(AlluxioStatusException error) {
      return new Payload<>(SERVER_ERROR, error);
    }

    static Payload<TransportError> transportError(Throwable error) {
      return new Payload<>(TRANSPORT_ERROR, error);
    }

    public T type() {
      return mType;
    }

    public <P> P payload(Type<P> type) {
      Preconditions.checkArgument(type == mType, "payload type mismatch");
      Class<P> clazz = type.payloadType();
      return clazz.cast(mPayload);
    }
  }
}
