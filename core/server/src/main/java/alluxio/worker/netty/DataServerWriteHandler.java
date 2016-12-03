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
import alluxio.StorageTierAssoc;
import alluxio.WorkerStorageTierAssoc;
import alluxio.network.protocol.RPCBlockWriteRequest;
import alluxio.network.protocol.RPCBlockWriteResponse;
import alluxio.network.protocol.RPCResponse;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockWriter;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
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
 * This class handles {@link RPCBlockWriteRequest}s.
 *
 * Protocol: Check {@link alluxio.client.block.stream.NettyBlockWriter} for more information.
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
public abstract class DataServerWriteHandler
    extends SimpleChannelInboundHandler<RPCBlockWriteRequest> {
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
   * Creates an instance of {@link BlockWriteDataServerHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s.
   */
  public DataServerWriteHandler(ExecutorService executorService) {
    mPacketWriterExecutor = executorService;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, RPCBlockWriteRequest msg) throws Exception {
    initializeRequest(msg);

    // Validate msg and return error if invalid. Init variables if necessary.
    if (!validateRequest(msg)) {
      replyError(ctx);
      return;
    }

    mLock.lock();
    try {
      ByteBuf buf = (ByteBuf) msg.getPayloadDataBuffer().getNettyOutput();
      mPackets.offer(buf);
      mPosToQueue += buf.readableBytes();
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
    replyError(ctx);
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    try {
      reset();
    } catch (IOException e) {
      LOG.warn("Failed to reset the write request inside channelUnregistered.");
    }
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
   * @return true if the request valid
   */
  private boolean validateRequest(RPCBlockWriteRequest msg) {
    if (msg.getBlockId() != mRequest.mId || msg.getLength() < 0) {
      return false;
    }
    if (msg.getOffset() != mPosToQueue) {
      return false;
    }

    // The last packet (signified by msg.getLength()) should not contain any data.
    if (msg.getLength() == 0 && msg.getPayloadDataBuffer().getLength() > 0) {
      return false;
    }
    return true;
  }

  /**
   * Writes an error response to the channel and closes the channel.
   *
   * @param ctx the channel handler context
   */
  private void replyError(ChannelHandlerContext ctx) {
    ctx.writeAndFlush(new RPCBlockWriteResponse(mRequest.mSessionId, mRequest.mId, mPosToQueue, 0,
        RPCResponse.Status.FAILED)).addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Writes a response to signify the success of the block write. Also resets the channel.
   *
   * @param ctx the channel handler context
   */
  private void replySuccess(ChannelHandlerContext ctx) {
    RPCBlockWriteResponse response =
        new RPCBlockWriteResponse(mRequest.mSessionId, mRequest.mId, mPosToQueue, 0,
            RPCResponse.Status.SUCCESS);
    try {
      reset();
    } catch (IOException e) {
      ctx.channel().pipeline().fireExceptionCaught(e);
    }
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);

    ctx.channel().config().setAutoRead(true);
    ctx.read();
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
      ByteBuf buf;
      do {
        try {
          mLock.lock();
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
          // This is the last packet.
          if (buf.readableBytes() == 0) {
            replySuccess(mCtx);
            break;
          }
          writeBuf(buf);
          mPosToWrite += buf.readableBytes();
        } catch (Exception e) {
          mCtx.fireExceptionCaught(e);
          break;
        } finally {
          ReferenceCountUtil.release(buf);
        }
      } while (true);
    }
  }

  /**
   * Initializes the handler if necessary.
   *
   * @param msg the block write request
   * @throws Exception if it fails to initialize
   */
  protected void initializeRequest(RPCBlockWriteRequest msg) throws Exception {
    if (mRequest == null) {
      mPosToQueue = 0;
      mPosToWrite = 0;
    }
  }

  protected abstract void writeBuf(ByteBuf buf) throws Exception;
}
