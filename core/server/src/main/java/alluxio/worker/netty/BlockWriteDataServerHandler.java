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
final public class BlockWriteDataServerHandler
    extends SimpleChannelInboundHandler<RPCBlockWriteRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final int MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();;
  /** The executor service to run the {@link PacketWriter}s. */
  private final ExecutorService mPacketWriterExecutor;

  private ReentrantLock mLock = new ReentrantLock();
  /** The buffer for packets read from the channel. */
  @GuardedBy("mLock")
  private Queue<ByteBuf> mPackets = new LinkedList<>();
  /** Set to true if the packet writer is active. */
  @GuardedBy("mLock")
  private boolean mPacketWriterActive = false;

  // The following are the states associated with a block write. They are re-initialized before
  // every new block write.
  private volatile BlockWriter mBlockWriter = null;
  private volatile long mBlockId = -1;
  private volatile long mSessionId = -1;
  /**
   * The next pos to queue to the buffer. Updated by the packet reader. Mostly used to validate
   * the validity of the request.
   */
  private volatile long mPosToQueue = 0;
  /**
   * The next pos to write to the block worker. Updated by the packet writer.
   */
  private volatile long mPosToWrite = 0;

  /**
   * Creates an instance of {@link BlockWriteDataServerHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s.
   * @param blockWorker the block worker
   */
  public BlockWriteDataServerHandler(ExecutorService executorService, BlockWorker blockWorker) {
    mPacketWriterExecutor = executorService;
    mWorker = blockWorker;
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, RPCBlockWriteRequest msg) throws Exception {
    init(msg);

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
    LOG.error("Failed to write block " + mBlockId + ".", cause);
    replyError(ctx);
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    if (mBlockWriter != null) {
      try {
        mBlockWriter.close();
      } catch (Exception e) {
        LOG.warn("Failed to close block writer.", e);
      }
      reset();
    }
  }

  /**
   * @return true if there are too many packets in flight
   */
  private boolean tooManyPacketsInFlight() {
    return mPackets.size() >= MAX_PACKETS_IN_FLIGHT;
  }

  /**
   * Initializes the handler if necessary.
   *
   * @param msg the block write request
   * @throws Exception if it fails to initialize
   */
  private void init(RPCBlockWriteRequest msg) throws Exception {
    if (mBlockWriter == null) {
      mBlockWriter = mWorker.getTempBlockWriterRemote(mSessionId, mBlockId);
      mBlockId = msg.getBlockId();
      mSessionId = msg.getSessionId();
      mPosToQueue = 0;
      mPosToWrite = 0;
    }
  }

  /**
   * Validates the block write request.
   *
   * @param msg the block write request
   * @return true if the request valid
   */
  private boolean validateRequest(RPCBlockWriteRequest msg) {
    if (msg.getOffset() == 0 && (mPosToQueue != 0 || mPosToWrite != 0)) {
      return false;
    }
    if (msg.getBlockId() != mBlockId || msg.getLength() < 0) {
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
    ctx.writeAndFlush(
        new RPCBlockWriteResponse(mSessionId, mBlockId, mPosToQueue, 0, RPCResponse.Status.FAILED))
        .addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Writes a response to signify the success of the block write. Also resets the channel.
   *
   * @param ctx
   */
  private void replySuccess(ChannelHandlerContext ctx) {
    RPCBlockWriteResponse response =
        new RPCBlockWriteResponse(mSessionId, mBlockId, mPosToQueue, 0, RPCResponse.Status.SUCCESS);
    reset();
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    ctx.channel().config().setAutoRead(true);
    ctx.read();
  }

  private void reset() {
    mBlockWriter = null;
    mBlockId = 0;
    mSessionId = 0;
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
            mBlockWriter.close();
            replySuccess(mCtx);
            break;
          }
          if (mPosToWrite == 0) {
            // This is the first write to the block, so create the temp block file. The file will
            // only
            // be created if the first write starts at offset 0. This allocates enough space for the
            // write.
            mWorker.createBlockRemote(mSessionId, mBlockId, mStorageTierAssoc.getAlias(0),
                buf.readableBytes());
          } else {
            // Allocate enough space in the existing temporary block for the write.
            mWorker.requestSpace(mSessionId, mBlockId, buf.readableBytes());
          }
          mBlockWriter.append(buf.nioBuffer());
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
}
