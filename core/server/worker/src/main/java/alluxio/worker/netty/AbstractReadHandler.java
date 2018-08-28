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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles {@link alluxio.proto.dataserver.Protocol.ReadRequest}s.
 *
 * Protocol: Check {@link alluxio.client.block.stream.NettyPacketReader} for additional information.
 * 1. Once a read request is received, the handler creates a {@link PacketReader} which reads
 *    packets from the block worker and pushes them to the buffer.
 * 2. The {@link PacketReader} pauses if there are too many packets in flight, and resumes if there
 *    is room available.
 * 3. The channel is closed if there is any exception during the packet read/write.
 *
 * Threading model:
 * Only two threads are involved at a given point of time: netty I/O thread, packet reader thread.
 * 1. The netty I/O thread accepts the read request, handles write callbacks. If any exception
 *    occurs (e.g. failed to read from netty or write to netty) or the read request is cancelled by
 *    the client, the netty I/O thread notifies the packet reader thread.
 * 2. The packet reader thread keeps reading from the file and writes to netty. Before reading a
 *    new packet, it checks whether there are notifications (e.g. cancel, error), if
 *    there is, handle them properly. See more information about the notifications in the javadoc
 *    of {@link ReadRequestContext#mCancel#mEof#mError}.
 *
 * @param <T> type of read request
 */
@NotThreadSafe
abstract class AbstractReadHandler<T extends ReadRequestContext<?>>
    extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractReadHandler.class);

  private static final long MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS);

  /** The executor to run {@link PacketReader}. */
  private final ExecutorService mPacketReaderExecutor;

  private final ReentrantLock mLock = new ReentrantLock();

  /**
   * This is only created in the netty I/O thread when a read request is received, reset when
   * another request is received.
   * Using "volatile" because we want any value change of this variable to be
   * visible across both netty and I/O threads, meanwhile no atomicity of operation is assumed;
   */
  private volatile T mContext;

  /**
   * Creates an instance of {@link AbstractReadHandler}.
   *
   * @param executorService the executor service to run {@link PacketReader}s
   */
  AbstractReadHandler(ExecutorService executorService) {
    mPacketReaderExecutor = executorService;
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    // The channel is closed so the client cannot receive this message.
    setError(ctx.channel(),
        new Error(new InternalException("Channel has been unregistered"), false));
    ctx.fireChannelUnregistered();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
    if (!acceptMessage(object)) {
      ctx.fireChannelRead(object);
      return;
    }
    Protocol.ReadRequest msg = ((RPCProtoMessage) object).getMessage().asReadRequest();
    if (msg.getCancel()) {
      setCancel(ctx.channel());
      return;
    }

    // Expected state: context equals null as this handler is new for request, or the previous
    // context is not active (done / cancel / abort). Otherwise, notify the client an illegal state.
    // Note that, we reset the context before validation msg as validation may require to update
    // error in context.
    try (LockResource lr = new LockResource(mLock)) {
      Preconditions.checkState(mContext == null || !mContext.isPacketReaderActive());
      mContext = createRequestContext(msg);
    }

    validateReadRequest(msg);

    try (LockResource lr = new LockResource(mLock)) {
      mContext.setPosToQueue(mContext.getRequest().getStart());
      mContext.setPosToWrite(mContext.getRequest().getStart());
      mPacketReaderExecutor.submit(createPacketReader(mContext, ctx.channel()));
      mContext.setPacketReaderActive(true);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Exception caught in AbstractReadHandler for channel {}:", ctx.channel(), cause);
    setError(ctx.channel(), new Error(AlluxioStatusException.fromThrowable(cause), true));
  }

  /**
   * @return true if there are too many packets in-flight
   */
  @GuardedBy("mLock")
  public boolean tooManyPendingPackets() {
    return mContext.getPosToQueue() - mContext.getPosToWrite() >= MAX_PACKETS_IN_FLIGHT * mContext
        .getRequest().getPacketSize();
  }

  /**
   * Validates a read request.
   *
   * @param request the block read request
   * @throws InvalidArgumentException if the request is invalid
   */
  private void validateReadRequest(Protocol.ReadRequest request) throws InvalidArgumentException {
    if (request.getBlockId() < 0) {
      throw new InvalidArgumentException(
          String.format("Invalid blockId (%d) in read request.", request.getBlockId()));
    }
    if (!request.getCancel() && (request.getOffset() < 0 || request.getLength() <= 0)) {
      throw new InvalidArgumentException(
          String.format("Invalid read bounds in read request %s.", request.toString()));
    }
  }

  /**
   * @param channel the channel
   * @param error the error
   */
  private void setError(Channel channel, Error error) {
    Preconditions.checkNotNull(error, "error");
    try (LockResource lr = new LockResource(mLock)) {
      if (mContext == null || mContext.getError() != null || mContext.isDoneUnsafe()) {
        // Note, we may reach here via channelUnregistered due to network errors bubbling up before
        // mContext is initialized, or channel garbage collection after the request is finished.
        return;
      }
      mContext.setError(error);
      if (!mContext.isPacketReaderActive()) {
        mContext.setPacketReaderActive(true);
        mPacketReaderExecutor.submit(createPacketReader(mContext, channel));
      }
    }
  }

  /**
   * @param channel the channel
   */
  private void setEof(Channel channel) {
    try (LockResource lr = new LockResource(mLock)) {
      if (mContext == null || mContext.getError() != null || mContext.isCancel()
          || mContext.isEof()) {
        return;
      }
      mContext.setEof(true);
      if (!mContext.isPacketReaderActive()) {
        mContext.setPacketReaderActive(true);
        mPacketReaderExecutor.submit(createPacketReader(mContext, channel));
      }
    }
  }

  /**
   * @param channel the channel
   */
  private void setCancel(Channel channel) {
    try (LockResource lr = new LockResource(mLock)) {
      if (mContext == null || mContext.getError() != null || mContext.isEof()
          || mContext.isCancel()) {
        return;
      }
      mContext.setCancel(true);
      if (!mContext.isPacketReaderActive()) {
        mContext.setPacketReaderActive(true);
        mPacketReaderExecutor.submit(createPacketReader(mContext, channel));
      }
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
    return message.getType() == RPCMessage.Type.RPC_READ_REQUEST;
  }

  /**
   * @param request the block read request
   * @return an instance of read request based on the request read from channel
   */
  protected abstract T createRequestContext(Protocol.ReadRequest request);

  /**
   * Creates a read reader.
   *
   * @param context read request context
   * @param channel channel
   * @return the packet reader for this handler
   */
  protected abstract PacketReader createPacketReader(T context, Channel channel);

  /**
   * @param bytesRead bytes read
   */
  private void incrementMetrics(long bytesRead) {
    Counter counter = mContext.getCounter();
    Meter meter = mContext.getMeter();
    Preconditions.checkState(counter != null);
    counter.inc(bytesRead);
    meter.mark(bytesRead);
  }

  /**
   * The channel handler listener that runs after a packet write is flushed.
   */
  private final class WriteListener implements ChannelFutureListener {
    private final long mPosToWriteUncommitted;

    /**
     * Creates an instance of the {@link WriteListener}.
     *
     * @param posToWriteUncommitted the position to commit (i.e. update mPosToWrite)
     */
    WriteListener(long posToWriteUncommitted) {
      mPosToWriteUncommitted = posToWriteUncommitted;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
      if (!future.isSuccess()) {
        LOG.error("Failed to send packet.", future.cause());
        setError(future.channel(),
            new Error(AlluxioStatusException.fromThrowable(future.cause()), true));
        return;
      }

      try (LockResource lr = new LockResource(mLock)) {
        Preconditions.checkState(
            mPosToWriteUncommitted - mContext.getPosToWrite() <= mContext.getRequest()
                .getPacketSize(), "Some packet is not acked.");
        incrementMetrics(mPosToWriteUncommitted - mContext.getPosToWrite());
        mContext.setPosToWrite(mPosToWriteUncommitted);

        if (shouldRestartPacketReader()) {
          mPacketReaderExecutor.submit(createPacketReader(mContext, future.channel()));
          mContext.setPacketReaderActive(true);
        }
      }
    }

    /**
     * @return true if we should restart the packet reader
     */
    @GuardedBy("mLock")
    private boolean shouldRestartPacketReader() {
      return !mContext.isPacketReaderActive() && !tooManyPendingPackets()
          && mContext.getPosToQueue() < mContext.getRequest().getEnd()
          && mContext.getError() == null && !mContext.isCancel() && !mContext.isEof();
    }
  }

  /**
   * A runnable that reads packets and writes them to the channel.
   */
  protected abstract class PacketReader implements Runnable {
    private final Channel mChannel;
    private final T mContext;
    private final ReadRequest mRequest;

    /**
     * Creates an instance of the {@link PacketReader}.
     *
     * @param context context of the request to complete
     * @param channel the channel
     */
    PacketReader(T context, Channel channel) {
      mContext = context;
      mRequest = context.getRequest();
      mChannel = channel;
    }

    @Override
    public void run() {
      try {
        runInternal();
      } catch (Throwable e) {
        LOG.error("Failed to run PacketReader.", e);
        throw e;
      }
    }

    private void runInternal() {
      boolean eof;  // End of file. Everything requested has been read.
      boolean cancel;
      Error error;  // error occured, abort requested.
      while (true) {
        final long start;
        final int packetSize;
        try (LockResource lr = new LockResource(mLock)) {
          start = mContext.getPosToQueue();
          eof = mContext.isEof();
          cancel = mContext.isCancel();
          error = mContext.getError();

          if (eof || cancel || error != null || tooManyPendingPackets()) {
            mContext.setPacketReaderActive(false);
            break;
          }
          packetSize = (int) Math
              .min(mRequest.getEnd() - mContext.getPosToQueue(), mRequest.getPacketSize());

          // packetSize should always be > 0 here when reaches here.
          Preconditions.checkState(packetSize > 0);
        }

        DataBuffer packet;
        try {
          packet = getDataBuffer(mContext, mChannel, start, packetSize);
        } catch (Exception e) {
          LOG.error("Failed to read data.", e);
          setError(mChannel, new Error(AlluxioStatusException.fromThrowable(e), true));
          continue;
        }
        if (packet != null) {
          try (LockResource lr = new LockResource(mLock)) {
            mContext.setPosToQueue(mContext.getPosToQueue() + packet.getLength());
          }
        }
        if (packet == null || packet.getLength() < packetSize || start + packetSize == mRequest
            .getEnd()) {
          // This can happen if the requested read length is greater than the actual length of the
          // block or file starting from the given offset.
          setEof(mChannel);
        }

        if (packet != null) {
          RPCProtoMessage response = RPCProtoMessage.createOkResponse(packet);
          mChannel.writeAndFlush(response).addListener(new WriteListener(start + packetSize));
        }
      }

      if (error != null) {
        try {
          // mRequest is null if an exception is thrown when initializing mRequest.
          if (mRequest != null) {
            completeRequest(mContext);
          }
        } catch (Exception e) {
          LOG.error("Failed to close the request.", e);
        }
        if (error.isNotifyClient()) {
          replyError(error.getCause());
        }
      } else if (eof || cancel) {
        try {
          completeRequest(mContext);
        } catch (IOException e) {
          setError(mChannel, new Error(AlluxioStatusException.fromIOException(e), true));
        } catch (Exception e) {
          setError(mChannel, new Error(AlluxioStatusException.fromThrowable(e), true));
        }
        if (eof) {
          replyEof();
        } else {
          replyCancel();
        }
      }
    }

    /**
     * Completes the read request. When the request is closed, we should clean up any temporary
     * state it may have accumulated.
     *
     * @param context context of the request to complete
     */
    protected abstract void completeRequest(T context) throws Exception;

    /**
     * Returns the appropriate {@link DataBuffer} representing the data to send, depending on the
     * configurable transfer type.
     *
     * @param context context of the request to complete
     * @param channel the netty channel
     * @param len The length, in bytes, of the data to read from the block
     * @return a {@link DataBuffer} representing the data
     */
    protected abstract DataBuffer getDataBuffer(T context, Channel channel, long offset, int len)
        throws Exception;

    /**
     * Writes an error read response to the channel and closes the channel after that.
     */
    private void replyError(AlluxioStatusException e) {
      mChannel.writeAndFlush(RPCProtoMessage.createResponse(e))
          .addListener(ChannelFutureListener.CLOSE);
    }

    /**
     * Writes a success response.
     */
    private void replyEof() {
      Preconditions.checkState(!mContext.isDoneUnsafe());
      mContext.setDoneUnsafe(true);
      mChannel.writeAndFlush(RPCProtoMessage.createOkResponse(null))
          .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    /**
     * Writes a cancel response.
     */
    private void replyCancel() {
      Preconditions.checkState(!mContext.isDoneUnsafe());
      mContext.setDoneUnsafe(true);
      mChannel.writeAndFlush(RPCProtoMessage.createCancelResponse())
          .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE);
    }
  }
}
