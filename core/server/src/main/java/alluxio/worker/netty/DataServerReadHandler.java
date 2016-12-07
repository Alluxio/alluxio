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
import alluxio.metrics.MetricsSystem;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;

import com.codahale.metrics.Counter;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class handles {@link alluxio.proto.dataserver.Protocol.ReadRequest}s.
 *
 * Protocol: Check {@link alluxio.client.block.stream.NettyPacketReader} for more information.
 * 1. Once a read request is received, the handler creates a {@link PacketReader} which reads
 *    packets from the block worker and pushes them to the buffer.
 * 2. The {@link PacketReader} pauses if there are too many packets in flight, and resumes if there
 *    is room available.
 * 3. The channel is closed if there is any exception during the packet read/write.
 */
@NotThreadSafe
public abstract class DataServerReadHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final long PACKET_SIZE =
      Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES);
  private static final long MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS);

  /** The executor to run {@link PacketReader}s.*/
  private final ExecutorService mPacketReaderExecutor;

  private final ReentrantLock mLock = new ReentrantLock();
  /** Set to true if the packet reader is active .*/
  @GuardedBy("mLock")
  private boolean mPacketReaderActive = false;
  /** The next pos to queue to the netty buffer. */
  @GuardedBy("mLock")
  private long mPosToQueue = -1;
  /** The next pos to write to the channel. */
  @GuardedBy("mLock")
  private long mPosToWrite = -1;

  // This is only updated in the channel event loop thread when a read request starts or cancelled.
  protected volatile ReadRequestInternal mRequest = null;

  protected abstract class ReadRequestInternal implements Closeable {
    // This ID can either be block ID or temp UFS file ID.
    public long mId = -1;
    public long mStart = -1;
    public long mEnd = -1;
    public boolean mCancelled = false;

    /**
     * Closes the request. Note that this close does not throw exception since all the data
     * requested by the client has been sent when this close is called. All the exceptions thrown
     * inside this close will be logged. This is different from the write handlers.
     */
    @Override
    public abstract void close();
  }

  /**
   * Creates an instance of {@link DataServerReadHandler}.
   *
   * @param executorService the executor service to run {@link PacketReader}s.
   */
  public DataServerReadHandler(ExecutorService executorService) {
    mPacketReaderExecutor = executorService;
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    reset();
    ctx.fireChannelUnregistered();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
    if (!acceptMessage(object)) {
      ctx.fireChannelRead(object);
      return;
    }
    Protocol.ReadRequest msg = (Protocol.ReadRequest) ((RPCProtoMessage) object).getMessage();

    if (!validateReadRequest(msg)) {
      replyError(ctx.channel(), Protocol.Status.Code.INVALID_ARGUMENT, "", null);
      return;
    }

    if (msg.getCancel()) {
      mRequest.mCancelled = true;
      return;
    }

    initializeRequest(msg);

    mLock.lock();
    try {
      // TODO(peis): Check the state of mPosToQueue and mPosToWrite.
      mPosToQueue = mRequest.mStart;
      mPosToWrite = mRequest.mStart;
    } finally {
      mLock.unlock();
    }

    mLock.lock();
    try {
      mPacketReaderExecutor.submit(new PacketReader(ctx.channel()));
      mPacketReaderActive = true;
    } finally {
      mLock.unlock();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Exception caught {} in BlockReadDataServerHandler.", cause);
    replyError(ctx.channel(), Protocol.Status.Code.INTERNAL, "", cause);
  }

  /**
   * @return true if there are too many packets in-flight.
   */
  private boolean tooManyPendingPackets() {
    return mPosToQueue - mPosToWrite >= MAX_PACKETS_IN_FLIGHT * PACKET_SIZE;
  }

  /**
   * @return true if we should restart the packet reader.
   */
  private boolean shouldStartPacketReader() {
    return !mPacketReaderActive && !tooManyPendingPackets() && mPosToQueue < mRequest.mEnd
        && !mRequest.mCancelled;
  }

  /**
   * @return the number of bytes remaining to push to the netty queue. Return 0 if it is cancelled.
   */
  private long remainingToQueue() {
    if (mRequest.mCancelled) {
      return 0;
    }
    return mRequest.mEnd - mPosToQueue;
  }

  /**
   * @return the number of bytes remaining to flush. Return 0 if it is cancelled.
   */
  private long remainingToWrite() {
    if (mRequest.mCancelled) {
      return 0;
    }
    return mRequest.mEnd - mPosToWrite;
  }

  /**
   * Writes an error block read response to the channel and closes the channel after that.
   *
   * @param channel the channel
   */
  private void replyError(Channel channel, Protocol.Status.Code code, String message, Throwable e) {
    channel.writeAndFlush(RPCProtoMessage.createResponse(code, message, e, null))
        .addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Writes a success block read response to the channel.
   *
   * @param channel the channel
   */
  private void replySuccess(Channel channel) {
    reset();
    channel.writeAndFlush(RPCProtoMessage.createOkResponse(null))
        .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  /**
   * Returns true if the block read request is valid.
   *
   * @param request the block read request
   * @return true if the block read request is valid
   */
  private boolean validateReadRequest(Protocol.ReadRequest request) {
    if (mRequest == null) {
      return true;
    }

    if (request.getCancel()) {
      if (request.getId() != mRequest.mId) {
        return false;
      }
      return true;
    }
    return false;
  }

  /**
   * Resets the handler to its initial state.
   */
  private void reset() {
    if (mRequest != null) {
      mRequest.close();
      mRequest = null;
    }
    mLock.lock();
    try {
      mPacketReaderActive = false;
      mPosToQueue = -1;
      mPosToWrite = -1;
    } finally {
      mLock.unlock();
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
   * Initializes the handler for the given block read request.
   *
   * @param request the block read request
   * @throws Exception if it fails to initialize
   */
  protected abstract void initializeRequest(Protocol.ReadRequest request) throws Exception;

  /**
   * Returns the appropriate {@link DataBuffer} representing the data to send, depending on the
   * configurable transfer type.
   *
   * @param channel the netty channel
   * @param len The length, in bytes, of the data to read from the block
   * @return a {@link DataBuffer} representing the data
   * @throws IOException if an I/O error occurs when reading the data
   */
  protected abstract DataBuffer getDataBuffer(Channel channel, long offset, int len)
      throws IOException;

  /**
   * The channel handler listener that runs after a packet write is flushed.
   */
  private final class WriteListener implements ChannelFutureListener {
    private final long mPosToWriteUncommitted;

    /**
     * Creates an instance of the {@link WriteListener}.
     *
     * @param pos the pos
     */
    public WriteListener(long pos) {
      mPosToWriteUncommitted = pos;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
      if (!future.isSuccess()) {
        future.channel().pipeline().fireExceptionCaught(future.cause());
        return;
      }

      mLock.lock();
      try {
        Preconditions.checkState(mPosToWrite - mPosToWriteUncommitted <= PACKET_SIZE,
            "Some packet is not acked.");
        mPosToWrite = mPosToWriteUncommitted;

        if (shouldStartPacketReader()) {
          mPacketReaderExecutor.submit(new PacketReader(future.channel()));
          mPacketReaderActive = true;
        }
        if (remainingToWrite() <= 0) {
          replySuccess(future.channel());
        }
      } finally {
        mLock.unlock();
      }
    }
  }

  /**
   * A runnable that reads from block worker and writes to the channel.
   */
  private class PacketReader implements Runnable {
    Channel mChannel;

    /**
     * Creates an instance of the {@link PacketReader}.
     *
     * @param channel the channel
     */
    public PacketReader(Channel channel) {
      mChannel = channel;
    }

    @Override
    public void run() {
      while (true) {
        final long start;
        final int packet_size;
        mLock.lock();
        try {
          start = mPosToQueue;
          long remaining = remainingToQueue();
          if (tooManyPendingPackets() || remaining <= 0) {
            mPacketReaderActive = false;
            break;
          }

          packet_size = (int) Math.min(remaining, PACKET_SIZE);
          mPosToQueue += packet_size;
        } finally {
          mLock.unlock();
        }

        DataBuffer packet;
        try {
          packet = getDataBuffer(mChannel, start, packet_size);
        } catch (Exception e) {
          mChannel.pipeline().fireExceptionCaught(e);
          break;
        }
        if (packet == null) {
          // This can happen if the requested read length is greater than the actual length of the
          // block or file starting from the given offset.
          mChannel.eventLoop().submit(new Runnable() {
            @Override
            public void run() {
              replySuccess(mChannel);
            }
          });
          break;
        }

        final RPCProtoMessage response = RPCProtoMessage.createOkResponse(packet);
        mChannel.eventLoop().submit(new Runnable() {
          @Override
          public void run() {
            mChannel.write(response).addListener(new WriteListener(start + packet_size));
          }
        });
      }
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
