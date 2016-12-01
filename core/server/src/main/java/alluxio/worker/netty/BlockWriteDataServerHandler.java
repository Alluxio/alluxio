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

import alluxio.Constants;
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

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles {@link RPCBlockWriteRequest}s.
 */
@NotThreadSafe
final public class BlockWriteDataServerHandler
    extends SimpleChannelInboundHandler<RPCBlockWriteRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** An object storing the mapping of tier aliases to ordinals. */
  private final StorageTierAssoc mStorageTierAssoc = new WorkerStorageTierAssoc();;


  // TODO(now): init these.
  private static final ExecutorService PACKET_WRITERS = null;

  // 1. When an exception is thrown from packet writer, no more packets will be written. No more
  //    packets will be enqueued. But note that mPackets is not guaranteed to be empty.
  // 2. When an exception is thrown from the read path, no more packets will be read as the channel
  //    will be closed.
  private ReentrantLock mLock = new ReentrantLock();
  @GuardedBy("mLock")
  private Queue<ByteBuf> mPackets = new LinkedList<>();
  @GuardedBy("mLock")
  private boolean mPacketWriterActive = false;
  @GuardedBy("mLock")
  private Exception mPacketWriterException = null;

  private final int MAX_BUFFER_SIZE = 10;

  // The following are the states associated with a block write. They are re-initialized before
  // every new block write.
  private volatile BlockWriter mBlockWriter = null;
  private volatile long mBlockId = 0;
  private volatile long mSessionId = 0;
  // The next pos to queue to the buffer.
  private volatile long mPosToQueue = 0;
  // The next pos to write to the block worker.
  private volatile long mPosToWrite = 0;

  public BlockWriteDataServerHandler(BlockWorker blockWorker) {
    mWorker = blockWorker;
  }

  /**
   * A runnable that polls from the packets queue and writes to the block worker.
   */
  private final class PacketWriter implements Runnable {
    private ChannelHandlerContext mCtx;

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
          if (mPackets.size() < MAX_BUFFER_SIZE) {
            mCtx.channel().config().setAutoRead(true);
            mCtx.read();
          }
        } finally {
          mLock.unlock();
        }

        // This is the last packet.
        if (buf.readableBytes() == 0) {
          replySuccess(mCtx);
          break;
        }
        try {
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
          try {
            mLock.lock();
            mPacketWriterException = e;
            mPacketWriterActive = false;
          } finally {
            mLock.unlock();
          }
          break;
        } finally {
          ReferenceCountUtil.release(buf);
        }
      } while (true);
    }
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, RPCBlockWriteRequest msg) throws Exception {
    init(msg);

    // Validate msg and return error if invalid. Init variables if necessary.
    if (!validateRequest(msg)) {
      replyError(ctx);
      return;
    }

    ByteBuf buf = (ByteBuf) msg.getPayloadDataBuffer().getNettyOutput();
    mLock.lock();
    try {
      if (mPacketWriterException != null) {
        throw mPacketWriterException;
      }
      mPackets.offer(buf);
      mPosToQueue += buf.readableBytes();
      if (!mPacketWriterActive) {
        PACKET_WRITERS.submit(new PacketWriter(ctx));
        mPacketWriterActive = true;
      }
      if (mPackets.size() >= MAX_BUFFER_SIZE) {
        ctx.channel().config().setAutoRead(false);
      }
    } finally {
      mLock.unlock();
    }

    // Read packets and put that into a queue.
    // Stop and close the channel when there is an exception.
    // Pause if the queue is full.
    // Resume if the queue is drained.
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Failed to write block " + mBlockId + ".", cause);
    replyError(ctx);
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    cleanUp();
  }

  private void init(RPCBlockWriteRequest msg) throws Exception {
    if (mBlockWriter == null) {
      mBlockWriter = mWorker.getTempBlockWriterRemote(mSessionId, mBlockId);
      mBlockId = msg.getBlockId();
      mSessionId = msg.getSessionId();
      mPosToQueue = 0;
      mPosToWrite = 0;
    }
  }

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

  private void replyError(ChannelHandlerContext ctx) {
    ctx.writeAndFlush(
        new RPCBlockWriteResponse(mSessionId, mBlockId, mPosToQueue, 0, RPCResponse.Status.FAILED))
        .addListener(ChannelFutureListener.CLOSE);
  }

  private void replySuccess(ChannelHandlerContext ctx) {
    RPCBlockWriteResponse response =
        new RPCBlockWriteResponse(mSessionId, mBlockId, mPosToQueue, 0, RPCResponse.Status.SUCCESS);
    cleanUp();
    ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    ctx.channel().config().setAutoRead(true);
    ctx.read();
  }

  private void cleanUp() {
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
      mPacketWriterException = null;
      mPacketWriterActive = false;
    } finally {
      mLock.unlock();
    }
  }
}
