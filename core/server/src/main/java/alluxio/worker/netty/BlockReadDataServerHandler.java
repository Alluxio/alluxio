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
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCBlockReadRequest;
import alluxio.network.protocol.RPCBlockReadResponse;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.network.protocol.databuffer.DataFileChannel;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.io.BlockReader;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles {@link RPCBlockReadRequest}s.
 */
@NotThreadSafe
final public class BlockReadDataServerHandler
    extends SimpleChannelInboundHandler<RPCBlockReadRequest> {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** The Block Worker which handles blocks stored in the Alluxio storage of the worker. */
  private final BlockWorker mWorker;
  /** The transfer type used by the data server. */
  private final FileTransferType mTransferType;

  private volatile BlockReader mBlockReader = null;
  private volatile long mBlockId = -1;
  private volatile long mStart = -1;
  private volatile long mEnd = -1;

  private static final int PACKET_SIZE = 64 * 1024;
  private static final long MAX_PACKETS_IN_FLIGHT = 128;

  private ReentrantLock mLock = new ReentrantLock();
  @GuardedBy("mLock")
  private boolean mPacketReaderActive = false;
  // The next pos to queue.
  @GuardedBy("mLock")
  private long mPos = -1;
  // The next pos to ack.
  @GuardedBy("mLock")
  private long mPosToAck = -1;
  @GuardedBy("mLock")
  private Throwable mThrowable = null;

  // TODO(now): init these.
  private static final ExecutorService PACKET_READERS = null;

  private boolean validate(RPCBlockReadRequest request) {
    if (request.isCancelRequest()) {
      if (request.getBlockId() != mBlockId) {
        return false;
      }
      return true;
    }
    if (request.getOffset() < 0 || request.getLength() < 0) {
      return false;
    }

    // Everything must be reset.
    try {
      mLock.lock();
      if (mBlockId != -1 || mBlockReader != null || mStart != -1 || mEnd != -1
          || mPacketReaderActive || mPos != -1 || mPosToAck != -1 || mThrowable != null) {
        return false;
      }
    } finally {
      mLock.unlock();
    }
    return true;
  }

  private void cleanup() throws Exception {
    mLock.lock();
    mBlockId = -1;
    if (mBlockReader != null) {
      mBlockReader.close();
    }
    mBlockReader = null;
    mStart = -1;
    mEnd = -1;
    mPacketReaderActive = false;
    mPos = -1;
    mPosToAck = -1;
    mThrowable = null;
  }

  private void init(RPCBlockReadRequest request) throws Exception {
    mBlockId = request.getBlockId();
    mBlockReader = mWorker.readBlockRemote(request.getSessionId(), mBlockId, request.getLockId());
    mWorker.accessBlock(request.getSessionId(), mBlockId);

    validateBounds(request, mBlockReader.getLength());

    mStart = request.getOffset();
    mEnd = mStart + request.getLength();
    mPos = mStart;
    mPosToAck = mStart;
  }

  private final class WriteListener implements ChannelFutureListener {
    private final long mPosToAckUncommitted;

    public WriteListener(long pos) {
      mPosToAckUncommitted = pos;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
      if (!future.isSuccess()) {
        future.channel().close();
      }

      try {
        mLock.lock();
        Preconditions.checkState(mPosToAck - mPosToAckUncommitted <= PACKET_SIZE,
            "Some packet is not acked.");
        mPosToAck = mPosToAckUncommitted;

        if (future.cause() != null) {
          mThrowable = future.cause();
          return;
        }

        if (shouldStartPacketReader()) {
          mPacketReaderActive = true;
          PACKET_READERS.submit(new PacketReader(future.channel()));
        }
        long blockId = mBlockId;
        if (mPosToAck >= mEnd) {
          try {
            cleanup();
          } catch (Exception e) {
            future.channel().pipeline().fireExceptionCaught(e);
            return;
          }
          future.channel().writeAndFlush(RPCBlockReadResponse.createSuccessResponse(blockId));
        }
      } finally {
        mLock.unlock();
      }
    }
  }

  private class PacketReader implements Runnable {
    Channel mChannel;

    public PacketReader(Channel channel) {
      mChannel = channel;
    }

    @Override
    public void run() {
      while (true) {
        final long start;
        final int packet_size;
        try {
          mLock.lock();
          start = mPos;
          long remaining = mEnd - mPos;
          if (tooManyPendingPackets() || mThrowable != null || remaining <= 0) {
            mPacketReaderActive = false;
            break;
          }

          packet_size = (int) Math.min(remaining, (long) PACKET_SIZE);
          mPos += packet_size;
        } finally {
          mLock.unlock();
        }

        try {
          DataBuffer packet = getDataBuffer(start, packet_size);
          final RPCBlockReadResponse response =
              new RPCBlockReadResponse(mBlockId, start, packet_size, packet,
                  RPCResponse.Status.SUCCESS);
          mChannel.eventLoop().submit(new Runnable() {
            @Override
            public void run() {
              mChannel.write(response).addListener(new WriteListener(start + packet_size));
            }
          });
        } catch (IOException e) {
          try {
            mLock.lock();
            mThrowable = e;
            mPacketReaderActive = false;
          } finally {
            mLock.unlock();
          }
          break;
        }
      }
    }
  }

  private boolean tooManyPendingPackets() {
    return mPos - mPosToAck > MAX_PACKETS_IN_FLIGHT * PACKET_SIZE;
  }

  private boolean shouldStartPacketReader() {
    return !mPacketReaderActive && mPos - mPosToAck < MAX_PACKETS_IN_FLIGHT * PACKET_SIZE
        && mPos < mEnd;
  }

  public BlockReadDataServerHandler(BlockWorker worker, FileTransferType transferType) {
    mWorker = worker;
    mTransferType = transferType;
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    if (mBlockReader != null) {
      mBlockReader.close();
      mBlockReader = null;
    }
  }

  private void replyError(ChannelHandlerContext ctx) {
    ctx.writeAndFlush(RPCBlockReadResponse.createErrorResponse(mBlockId, RPCResponse.Status.FAILED))
        .addListener(ChannelFutureListener.CLOSE);
  }

  @Override
  public void channelRead0(ChannelHandlerContext ctx, RPCBlockReadRequest msg) throws Exception {
    if (!validate(msg)) {
      replyError(ctx);
      return;
    }

    if (msg.isCancelRequest()) {
      // Simply set mEnd to -1 to stop reading more. The client needs to read everything remaining
      // in the channel if the client wants to reuse the channel. Otherwise, the client can
      // simply close the channel to cancel.
      mEnd = -1;
      return;
    }

    init(msg);

    try {
      mLock.lock();
      mPacketReaderActive = true;
      PACKET_READERS.submit(new PacketReader(ctx.channel()));
    } finally {
      mLock.unlock();
    }

    // 1. init
    // 2. validate
    // 3. process cancel request.
    // 4. process normal request. Start packet reader.

    // Very similar as block writer implementation.

    // Packet reader.
    // Read a packet and write to the netty buffer. If buffer is full, pause.
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Exception caught {} in BlockReadDataServerHandler.", cause);
    replyError(ctx);
  }

  /**
   * @return how much of a file to read. When {@code len} is {@code -1}, then
   * {@code fileLength - offset} is used.
   */
  private long returnLength(final long offset, final long len, final long fileLength) {
    return (len == -1) ? fileLength - offset : len;
  }

  /**
   * Validates the bounds of the request. An uncaught exception will be thrown if an
   * inconsistency occurs.
   *
   * @param req The initiating {@link RPCBlockReadRequest}
   * @param fileLength The length of the block being read
   */
  private void validateBounds(final RPCBlockReadRequest req, final long fileLength) {
    Preconditions
        .checkArgument(req.getOffset() <= fileLength, "Offset(%s) is larger than file length(%s)",
            req.getOffset(), fileLength);
    Preconditions
        .checkArgument(req.getOffset() + req.getLength() <= fileLength,
            "Offset(%s) plus length(%s) is larger than file length(%s)", req.getOffset(),
            req.getLength(), fileLength);
  }

  /**
   * Returns the appropriate {@link DataBuffer} representing the data to send, depending on the
   * configurable transfer type.
   *
   * @param len The length, in bytes, of the data to read from the block
   * @return a {@link DataBuffer} representing the data
   * @throws IOException if an I/O error occurs when reading the data
   */
  private DataBuffer getDataBuffer(long offset, int len)
      throws IOException, IllegalArgumentException {
    switch (mTransferType) {
      case MAPPED:
        ByteBuffer data = mBlockReader.read(offset, len);
        return new DataByteBuffer(data, len);
      case TRANSFER: // intend to fall through as TRANSFER is the default type.
      default:
        Preconditions.checkArgument(mBlockReader.getChannel() instanceof FileChannel,
            "Only FileChannel is supported!");
        return new DataFileChannel((FileChannel) mBlockReader.getChannel(), offset, len);
    }
  }

  /**
   * Class that contains metrics for BlockDataServerHandler.
   */
  private static final class Metrics {
    private static final Counter BYTES_READ_REMOTE = MetricsSystem.workerCounter("BytesReadRemote");
    private static final Counter BYTES_WRITTEN_REMOTE =
        MetricsSystem.workerCounter("BytesWrittenRemote");

    private Metrics() {
    } // prevent instantiation
  }
}
