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

package alluxio.worker.netty;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.exception.status.InternalException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.resource.LockResource;
import alluxio.util.network.NettyUtils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles {@link alluxio.proto.dataserver.Protocol.WriteRequest}s.
 *
 * Protocol: Check {@link alluxio.client.block.stream.NettyPacketWriter} for more information.
 * 1. The netty channel handler streams packets from the channel and buffers them. The netty
 *    reader is paused if the buffer is full by turning off the auto read, and is resumed when
 *    the buffer is not full.
 * 2. The {@link PacketWriter} polls packets from the buffer and writes to the block worker. The
 *    writer becomes inactive if there is nothing on the buffer to free up the executor. It is
 *    resumed when the buffer becomes non-empty.
 * 3. An EOF or CANCEL message signifies the completion of this request.
 * 4. When an error occurs, the channel is closed.
 *
 * Threading model:
 * Only two threads are involved at a given point of time: netty I/O thread, packet writer thread.
 * 1. The netty I/O thread reads packets from the wire and pushes them to the buffer if there is
 *    no error seen so far. This packet reading can be ended by an EOF packet, a CANCEL packet or
 *    an exception. When one of these 3 happens, a special packet is pushed to the buffer.
 * 2. The packet writer thread keeps polling packets from the buffer and processes them.
 *    NOTE: it is guaranteed that there is only one packet writer thread active at a given time.
 * 3. Once a complete or cancel response is sent, there is no guarantee that the context instance
 *    still represents the acked packet or a new packet. Therefore, we should make no further
 *    modification on a context after the response is sent.
 *
 * @param <T> type of write request
 */
@NotThreadSafe
abstract class AbstractWriteHandler<T extends WriteRequestContext<?>>
    extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractWriteHandler.class);

  private static final int MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS);

  /** The executor service to run the {@link PacketWriter}s. */
  private final ExecutorService mPacketWriterExecutor;

  /**
   * Special packets used to pass control information from the I/O thread to the packet writer
   * thread.
   * EOF: the end of file.
   * CANCEL: the write request is cancelled by the client.
   * ABORT: a non-recoverable error is detected, abort this channel.
   */
  private static final ByteBuf EOF = Unpooled.buffer(0);
  private static final ByteBuf CANCEL = Unpooled.buffer(0);
  private static final ByteBuf ABORT = Unpooled.buffer(0);

  private ReentrantLock mLock = new ReentrantLock();

  /**
   * This is initialized only once for a whole file or block in
   * {@link AbstractReadHandler#channelRead(ChannelHandlerContext, Object)}.
   * After that, it should only be used by the packet writer thread.
   * It is safe to read those final primitive fields (e.g. mId, mSessionId) if mError is not set
   * from any thread (not such usage in the code now). It is destroyed when the write request is
   * done (complete or cancel) or an error is seen.
   *
   * Using "volatile" because we want any value change of this variable to be
   * visible across both netty and I/O threads, meanwhile no atomicity of operation is assumed;
   */
  private volatile T mContext;

  /**
   * Creates an instance of {@link AbstractWriteHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s
   */
  AbstractWriteHandler(ExecutorService executorService) {
    mPacketWriterExecutor = executorService;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
    if (!acceptMessage(object)) {
      ctx.fireChannelRead(object);
      return;
    }

    RPCProtoMessage msg = (RPCProtoMessage) object;
    Protocol.WriteRequest writeRequest = msg.getMessage().asWriteRequest();

    try (LockResource lr = new LockResource(mLock)) {
      boolean isNewContextCreated = false;
      if (mContext == null || mContext.isDoneUnsafe()) {
        // We create a new context if the previous request completes (done flag is true) or the
        // context is still null (an empty channel so far). And in this case, we create a new one as
        // catching exceptions and replying errors
        // leverages data structures in context, regardless of the request is valid or not.
        // TODO(binfan): remove the dependency on an instantiated request context which is required
        // to reply errors to client side.
        mContext = createRequestContext(writeRequest);
        isNewContextCreated = true;
      }
      // Only initialize (open the writers) if this is the first packet in the block/file.
      if (writeRequest.getOffset() == 0) {
        // Expected state: context equals null as this handler is new for request, or the previous
        // context is not active (done / cancel / abort). Otherwise, notify the client an illegal
        // state. Note that, we reset the context before validation msg as validation may require to
        // update error in context.
        Preconditions.checkState(isNewContextCreated);
        initRequestContext(mContext);
      }

      // If we have seen an error, return early and release the data. This can
      // happen for (1) those mis-behaving clients who first sends some invalid requests, then
      // then some random data, or (2) asynchronous requests arrive after the previous request fails
      // and triggers abortion. It can leak memory if we do not release buffers here.
      if (mContext.getError() != null) {
        if (msg.getPayloadDataBuffer() != null) {
          msg.getPayloadDataBuffer().release();
        }
        LOG.warn("Ignore the request {} due to the error {} on context", mContext.getRequest(),
            mContext.getError());
        return;
      } else {
        // Validate the write request. The validation is performed only when no error is in the
        // context in order to prevent excessive logging on the subsequent arrived asynchronous
        // requests after a previous request fails and triggers the abortion
        validateWriteRequest(writeRequest, msg.getPayloadDataBuffer());
      }

      ByteBuf buf;
      if (writeRequest.getEof()) {
        buf = EOF;
      } else if (writeRequest.getCancel()) {
        buf = CANCEL;
      } else {
        DataBuffer dataBuffer = msg.getPayloadDataBuffer();
        Preconditions.checkState(dataBuffer != null && dataBuffer.getLength() > 0);
        Preconditions.checkState(dataBuffer.getNettyOutput() instanceof ByteBuf);
        buf = (ByteBuf) dataBuffer.getNettyOutput();
        mContext.setPosToQueue(mContext.getPosToQueue() + buf.readableBytes());
      }
      if (!mContext.isPacketWriterActive()) {
        mContext.setPacketWriterActive(true);
        mPacketWriterExecutor.submit(createPacketWriter(mContext, ctx.channel()));
      }
      mContext.getPackets().offer(buf);
      if (tooManyPacketsInFlight()) {
        NettyUtils.disableAutoRead(ctx.channel());
      }
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Exception caught in AbstractWriteHandler for channel {}:", ctx.channel(), cause);
    pushAbortPacket(ctx.channel(), new Error(AlluxioStatusException.fromThrowable(cause), true));
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    pushAbortPacket(ctx.channel(), new Error(new InternalException("channel unregistered"), false));
    ctx.fireChannelUnregistered();
  }

  /**
   * @return true if there are too many packets in flight
   */
  @GuardedBy("mLock")
  private boolean tooManyPacketsInFlight() {
    return mContext.getPackets().size() >= MAX_PACKETS_IN_FLIGHT;
  }

  /**
   * Validates a block write request.
   *
   * @param request the block write request
   * @throws InvalidArgumentException if the write request is invalid
   */
  @GuardedBy("mLock")
  private void validateWriteRequest(Protocol.WriteRequest request, DataBuffer payload)
      throws InvalidArgumentException {
    if (request.getOffset() != mContext.getPosToQueue()) {
      throw new InvalidArgumentException(String.format(
          "Offsets do not match [received: %d, expected: %d].",
          request.getOffset(), mContext.getPosToQueue()));
    }
    if (payload != null && payload.getLength() > 0 && (request.getCancel() || request.getEof())) {
      throw new InvalidArgumentException("Found data in a cancel/eof message.");
    }
  }

  /**
   * A runnable that polls from the packets queue and writes to the block worker or UFS.
   */
  protected abstract class PacketWriter implements Runnable {
    private final Channel mChannel;
    private final T mContext;

    /**
     * Creates an instance of {@link PacketWriter}.
     *
     * @param context context of the request to complete
     * @param channel the netty channel
     */
    PacketWriter(T context, Channel channel) {
      mContext = context;
      mChannel = channel;
    }

    @Override
    public void run() {
      try {
        runInternal();
      } catch (Throwable e) {
        // This should never happen.
        LOG.error("Failed to run PacketWriter.", e);
        throw e;
      }
    }

    private void runInternal() {
      boolean eof;
      boolean cancel;
      boolean abort;

      while (true) {
        ByteBuf buf;
        try (LockResource lr = new LockResource(mLock)) {
          buf = mContext.getPackets().poll();
          if (buf == null || buf == EOF || buf == CANCEL || buf == ABORT) {
            eof = buf == EOF;
            cancel = buf == CANCEL;
            // mError is checked here so that we can override EOF and CANCEL if error happens
            // after we receive EOF or CANCEL signal.
            // TODO(peis): Move to the pattern used in AbstractReadHandler to avoid
            // using special packets.
            abort = mContext.getError() != null;
            mContext.setPacketWriterActive(false);
            break;
          }
          // Release all the packets if we have encountered an error. We guarantee that no more
          // packets should be queued after we have received one of the done signals (EOF, CANCEL
          // or ABORT).
          if (mContext.getError() != null) {
            release(buf);
            continue;
          }
          if (!tooManyPacketsInFlight()) {
            NettyUtils.enableAutoRead(mChannel);
          }
        }

        try {
          int readableBytes = buf.readableBytes();
          mContext.setPosToWrite(mContext.getPosToWrite() + readableBytes);
          writeBuf(mContext, mChannel, buf, mContext.getPosToWrite());
          incrementMetrics(readableBytes);
        } catch (Exception e) {
          LOG.error("Failed to write packet for request {}", mContext.getRequest(), e);
          Throwables.propagateIfPossible(e);
          pushAbortPacket(mChannel,
              new Error(AlluxioStatusException.fromCheckedException(e), true));
        } finally {
          release(buf);
        }
      }

      if (abort) {
        try {
          cleanupRequest(mContext);
          replyError();
        } catch (Exception e) {
          LOG.warn("Failed to cleanup states with error {}.", e.getMessage());
        }
      } else if (cancel || eof) {
        try {
          if (cancel) {
            cancelRequest(mContext);
            replyCancel();
          } else {
            completeRequest(mContext, mChannel);
            replySuccess();
          }
        } catch (Exception e) {
          Throwables.propagateIfPossible(e);
          pushAbortPacket(mChannel,
              new Error(AlluxioStatusException.fromCheckedException(e), true));
        }
      }
    }

    /**
     * Completes this write. This is called when the write completes.
     *
     * @param context context of the request to complete
     * @param channel netty channel
     */
    protected abstract void completeRequest(T context, Channel channel) throws Exception;

    /**
     * Cancels this write. This is called when the client issues a cancel request.
     *
     * @param context context of the request to complete
     */
    protected abstract void cancelRequest(T context) throws Exception;

    /**
     * Cleans up this write. This is called when the write request is aborted due to any exception
     * or session timeout.
     *
     * @param context context of the request to complete
     */
    protected abstract void cleanupRequest(T context) throws Exception;

    /**
     * Writes the buffer.
     *
     * @param context context of the request to complete
     * @param channel the netty channel
     * @param buf the buffer
     * @param pos the pos
     */
    protected abstract void writeBuf(
        T context, Channel channel, ByteBuf buf, long pos) throws Exception;

    /**
     * Writes a response to signify the success of the write request.
     */
    private void replySuccess() {
      NettyUtils.enableAutoRead(mChannel);
      mContext.setDoneUnsafe(true);
      mChannel.writeAndFlush(RPCProtoMessage.createOkResponse(null))
          .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    /**
     * Writes a response to signify the successful cancellation of the write request.
     */
    private void replyCancel() {
      NettyUtils.enableAutoRead(mChannel);
      mContext.setDoneUnsafe(true);
      mChannel.writeAndFlush(RPCProtoMessage.createCancelResponse())
          .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    /**
     * Writes an error response to the channel and closes the channel after that.
     */
    private void replyError() {
      Error error;
      try (LockResource lr = new LockResource(mLock)) {
        error = Preconditions.checkNotNull(mContext.getError());
      }

      if (error.isNotifyClient()) {
        mChannel.writeAndFlush(RPCProtoMessage.createResponse(error.getCause()))
            .addListener(ChannelFutureListener.CLOSE);
      }
    }
  }

  /**
   * Pushes {@link AbstractWriteHandler#ABORT} to the buffer if there has been no error so far.
   *
   * @param channel the channel
   * @param error the error
   */
  private void pushAbortPacket(Channel channel, Error error) {
    try (LockResource lr = new LockResource(mLock)) {
      if (mContext == null || mContext.getError() != null || mContext.isDoneUnsafe()) {
        // Note, we may reach here via channelUnregistered due to network errors bubbling up before
        // mContext is initialized, or channel garbage collection after the request is finished.
        return;
      }
      mContext.setError(error);
      mContext.getPackets().offer(ABORT);
      if (!mContext.isPacketWriterActive()) {
        mContext.setPacketWriterActive(true);
        mPacketWriterExecutor.submit(createPacketWriter(mContext, channel));
      }
    }
  }

  /**
   * Releases a {@link ByteBuf}.
   *
   * @param buf the netty byte buffer
   */
  private static void release(ByteBuf buf) {
    if (buf != null && buf != EOF && buf != CANCEL && buf != ABORT) {
      buf.release();
    }
  }

  /**
   * Checks whether this object should be processed by this handler.
   *
   * @param object the object
   * @return true if this object should be processed
   */
  protected boolean acceptMessage(Object object) {
    if (!(object instanceof RPCProtoMessage)) {
      return false;
    }
    RPCProtoMessage message = (RPCProtoMessage) object;
    return message.getType() == RPCMessage.Type.RPC_WRITE_REQUEST;
  }

  /**
   * Creates a new request context. This method must be exception free.
   *
   * @param msg the block write request
   */
  protected abstract T createRequestContext(Protocol.WriteRequest msg);

  /**
   * Initializes the given request context.
   *
   * @param context the created request context
   */
  protected abstract void initRequestContext(T context) throws Exception;

  /**
   * Creates a read writer.
   *
   * @param context read request context
   * @param channel channel
   * @return the packet reader for this handler
   */
  protected abstract PacketWriter createPacketWriter(T context, Channel channel);

  /**
   * @param bytesWritten bytes written
   */
  private void incrementMetrics(long bytesWritten) {
    Counter counter = mContext.getCounter();
    Meter meter = mContext.getMeter();
    Preconditions.checkState(counter != null, "counter");
    Preconditions.checkState(meter != null, "meter");
    counter.inc(bytesWritten);
    meter.mark(bytesWritten);
  }
}
