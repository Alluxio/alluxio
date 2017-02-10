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
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
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
 * 3. When an error occurs, the channel is closed. All the buffered packets are released when the
 *    channel is deregistered.
 */
@NotThreadSafe
public abstract class DataServerWriteHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final int MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS);

  /** The executor service to run the {@link PacketWriter}s. */
  private final ExecutorService mPacketWriterExecutor;

  private ReentrantLock mLock = new ReentrantLock();
  /** The buffer for packets read from the channel. */
  @GuardedBy("mLock")
  private Queue<ByteBuf> mPackets = new LinkedList<>();
  /** Set to true if the packet writer is active. */
  @GuardedBy("mLock")
  private boolean mPacketWriterActive = false;

  protected volatile WriteRequestInternal mRequest = null;

  protected abstract class WriteRequestInternal implements Closeable {
    // This ID can either be block ID or temp UFS file ID.
    public long mId = -1;
    public long mSessionId = -1;
  }

  /**
   * The next pos to queue to the buffer. Updated by the packet reader. Mostly used to validate
   * the validity of the request.
   */
  private volatile long mPosToQueue = 0;
  /**
   * The next pos to write to the block worker. Updated by the packet writer.
   */
  protected volatile long mPosToWrite = 0;

  /**
   * Creates an instance of {@link DataServerWriteHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s
   */
  public DataServerWriteHandler(ExecutorService executorService) {
    mPacketWriterExecutor = executorService;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
    if (!acceptMessage(object)) {
      ctx.fireChannelRead(object);
      return;
    }

    RPCProtoMessage msg = (RPCProtoMessage) object;
    initializeRequest(msg);

    // Validate msg and return error if invalid. Init variables if necessary.
    String error = validateRequest(msg);
    if (!error.isEmpty()) {
      replyError(ctx.channel(), Protocol.Status.Code.INVALID_ARGUMENT, error, null);
      return;
    }

    mLock.lock();
    try {
      DataBuffer dataBuffer = msg.getPayloadDataBuffer();
      ByteBuf buf;
      if (dataBuffer == null) {
        buf = ctx.alloc().buffer(0, 0);
      } else {
        Preconditions.checkState(dataBuffer.getLength() > 0);
        assert dataBuffer.getNettyOutput() instanceof ByteBuf;
        buf = (ByteBuf) dataBuffer.getNettyOutput();
      }
      mPosToQueue += buf.readableBytes();
      mPackets.offer(buf);
      if (!mPacketWriterActive) {
        mPacketWriterExecutor.submit(new PacketWriter(ctx));
        mPacketWriterActive = true;
      }
      if (tooManyPacketsInFlight()) {
        ctx.channel().config().setAutoRead(false);
      }
    } finally {
      mLock.unlock();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Failed to write block " + (mRequest == null ? -1 : mRequest.mId) + ".", cause);
    replyError(ctx.channel(), Protocol.Status.Code.INTERNAL, "", cause);
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    try {
      reset();
    } catch (IOException e) {
      LOG.warn("Failed to reset the write request inside channelUnregistered.");
    }
    ctx.fireChannelUnregistered();
  }

  /**
   * @return true if there are too many packets in flight
   */
  private boolean tooManyPacketsInFlight() {
    return mPackets.size() >= MAX_PACKETS_IN_FLIGHT;
  }

  /**
   * Validates the block write request.
   *
   * @param msg the block write request
   * @return empty string if the request valid
   */
  private String validateRequest(RPCProtoMessage msg) {
    Protocol.WriteRequest request = msg.getMessage().getMessage();
    if (request.getId() != mRequest.mId) {
      return "The Ids do not match.";
    }
    if (request.getOffset() != mPosToQueue) {
      return String
          .format("Offsets do not match [received: %d, expected: %d].", request.getOffset(),
              mPosToQueue);
    }
    return "";
  }

  /**
   * Writes an error block write response to the channel and closes the channel after that.
   *
   * @param channel the channel
   */
  private void replyError(Channel channel, Protocol.Status.Code code, String message, Throwable e) {
    channel.writeAndFlush(RPCProtoMessage.createResponse(code, message, e, null))
        .addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Writes a response to signify the success of the block write. Also resets the channel.
   *
   * @param channel the channel
   */
  private void replySuccess(Channel channel) {
    channel.writeAndFlush(RPCProtoMessage.createOkResponse(null))
        .addListeners(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE, new ChannelFutureListener() {
          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            reset();
          }
        });
    if (!channel.config().isAutoRead()) {
      channel.config().setAutoRead(true);
      channel.read();
    }
  }

  private void reset() throws IOException {
    if (mRequest != null) {
      mRequest.close();
      mRequest = null;
    }

    mPosToQueue = 0;
    mPosToWrite = 0;

    try {
      mLock.lock();
      for (ByteBuf buf : mPackets) {
        ReferenceCountUtil.release(buf);
      }
      mPacketWriterActive = false;
    } finally {
      mLock.unlock();
    }
  }

  /**
   * A runnable that polls from the packets queue and writes to the block worker.
   */
  private final class PacketWriter implements Runnable {
    private ChannelHandlerContext mCtx;

    /**
     * Creates an instance of {@link PacketWriter}.
     *
     * @param ctx the netty channel handler context
     */
    PacketWriter(ChannelHandlerContext ctx) {
      mCtx = ctx;
    }

    @Override
    public void run() {
      try {
        runInternal();
      } catch (Throwable e) {
        LOG.error("Failed to run PacketWriter.", e);
        throw e;
      }
    }

    /**
     * The actual implementation of the runnable.
     */
    private void runInternal() {
      while (true) {
        ByteBuf buf;
        mLock.lock();
        try {
          buf = mPackets.poll();
          if (buf == null) {
            mPacketWriterActive = false;
            break;
          }
          if (!tooManyPacketsInFlight()) {
            mCtx.channel().config().setAutoRead(true);
            mCtx.read();
          }
        } finally {
          mLock.unlock();
        }

        try {
          if (buf.readableBytes() == 0) {
            // This is the last packet.
            replySuccess(mCtx.channel());
            break;
          }
          mPosToWrite += buf.readableBytes();
          incrementMetrics(buf.readableBytes());
          writeBuf(buf, mPosToWrite);
        } catch (Exception e) {
          mPacketWriterActive = false;
          exceptionCaught(mCtx, e);
          break;
        } finally {
          buf.release();
        }
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
    return message.getType() == RPCMessage.Type.RPC_WRITE_REQUEST;
  }

  /**
   * Initializes the handler if necessary.
   *
   * @param msg the block write request
   * @throws Exception if it fails to initialize
   */
  protected void initializeRequest(RPCProtoMessage msg) throws Exception {
    if (mRequest == null) {
      mPosToQueue = 0;
      mPosToWrite = 0;
    }
  }

  /**
   * Writes the buffer.
   *
   * @param buf the buffer
   * @param pos the pos
   * @throws Exception if it fails to write the buffer
   */
  protected abstract void writeBuf(ByteBuf buf, long pos) throws Exception;

  /**
   * @param bytesWritten bytes written
   */
  protected abstract void incrementMetrics(long bytesWritten);
}
