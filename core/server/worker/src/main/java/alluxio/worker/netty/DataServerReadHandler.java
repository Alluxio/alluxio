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
import java.util.concurrent.atomic.AtomicBoolean;
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
  private static final Logger LOG = LoggerFactory.getLogger(DataServerReadHandler.class);

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
  /**
   * The next pos to queue to the netty buffer. mPosToQueue - mPosToWrite is the bytes that are
   * in netty buffer.
   */
  @GuardedBy("mLock")
  private long mPosToQueue = -1;
  /** The next pos to write to the channel. */
  @GuardedBy("mLock")
  private long mPosToWrite = -1;

  /** Makes sure we send EOF only once. */
  private final AtomicBoolean mEOFSent = new AtomicBoolean(false);

  /**
   * This is only updated in the channel event loop thread when a read request starts or cancelled.
   * No need to be locked.
   * When it is accessed outside of the event loop thread, make sure it is not null. For now, all
   * netty channel handlers are executed in the event loop. The packet reader is not executed in
   * the event loop thread.
   */
  protected volatile ReadRequestInternal mRequest = null;

  protected abstract class ReadRequestInternal implements Closeable {
    // This ID can either be block ID or temp UFS file ID.
    public long mId = -1;
    public long mStart = -1;
    public long mEnd = -1;
    // The position at which the read request is cancelled. If this is no smaller than mEnd,
    // the cancel request does nothing.
    public long mCancelled = Long.MAX_VALUE;

    /**
     * @return the effective end of the stream
     */
    public long end() {
      return Math.min(mEnd, mCancelled);
    }

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
   * @param executorService the executor service to run {@link PacketReader}s
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
    Protocol.ReadRequest msg = ((RPCProtoMessage) object).getMessage().getMessage();

    String error = validateReadRequest(msg);
    if (!error.isEmpty()) {
      replyError(ctx.channel(), Protocol.Status.Code.INVALID_ARGUMENT, error, null);
      return;
    }

    if (msg.getCancel()) {
      if (mRequest != null) {
        mLock.lock();
        try {
          mRequest.mCancelled = mPosToQueue;
          if (remainingToWrite() <= 0) {
            // This can only happen when everything before mPosToQueue has been sent to the client.
            // This is not a common event.
            replySuccess(ctx.channel());
          }
        } finally {
          mLock.unlock();
        }
      }
      return;
    }

    mEOFSent.set(false);
    initializeRequest(msg);

    mLock.lock();
    try {
      // TODO(peis): Check the state of mPosToQueue and mPosToWrite.
      mPosToQueue = mRequest.mStart;
      mPosToWrite = mRequest.mStart;

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
   * @return true if there are too many packets in-flight
   */
  @GuardedBy("mLock")
  private boolean tooManyPendingPackets() {
    return mPosToQueue - mPosToWrite >= MAX_PACKETS_IN_FLIGHT * PACKET_SIZE;
  }

  /**
   * @return true if we should restart the packet reader
   */
  @GuardedBy("mLock")
  private boolean shouldRestartPacketReader() {
    return !mPacketReaderActive && !tooManyPendingPackets() && mPosToQueue < mRequest.end();
  }

  /**
   * @return the number of bytes remaining to push to the netty queue. Return 0 if it is cancelled
   */
  @GuardedBy("mLock")
  private long remainingToQueue() {
    // mRequest is not guarded by mLock and is volatile. It can be reset because of cancel before
    // the packet reader is done. We need to make a copy here to make sure the packet reader
    // thread is not killed due to null pointer exception.
    ReadRequestInternal request = mRequest;
    return (request == null || mPosToQueue == -1) ? 0 : request.end() - mPosToQueue;
  }

  /**
   * @return the number of bytes remaining to flush. Return 0 if it is cancelled
   */
  @GuardedBy("mLock")
  private long remainingToWrite() {
    // mRequest is not guarded by mLock and is volatile. It can be reset because of cancel before
    // the packet reader is done. We need to make a copy here to make sure the packet reader
    // thread is not killed due to null pointer exception.
    ReadRequestInternal request = mRequest;
    return (request == null || mPosToWrite == -1) ? 0 : request.end() - mPosToWrite;
  }

  /**
   * Writes an error read response to the channel and closes the channel after that.
   */
  private void replyError(Channel channel, Protocol.Status.Code code, String message, Throwable e) {
    channel.writeAndFlush(RPCProtoMessage.createResponse(code, message, e, null))
        .addListener(ChannelFutureListener.CLOSE);
  }

  /**
   * Writes a success read response to the channel.
   */
  private void replySuccess(Channel channel) {
    if (mEOFSent.compareAndSet(false, true)) {
      channel.writeAndFlush(RPCProtoMessage.createOkResponse(null))
          .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE, new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              reset();
            }
          });
    }
  }

  /**
   * Returns true if the block read request is valid.
   *
   * @param request the block read request
   * @return the error message (empty string indicates success)
   */
  private String validateReadRequest(Protocol.ReadRequest request) {
    if (request.getId() < 0) {
      return String.format("Invalid blockId (%d) in read request.", request.getId());
    }
    if (mRequest == null) {
      if (!request.getCancel()) {
        if (request.getOffset() < 0 || request.getLength() <= 0) {
          return String.format("Invalid read bounds in read request %s.", request.toString());
        }
      }
      return "";
    }

    if (request.getCancel()) {
      if (request.getId() != mRequest.mId) {
        return String.format("The IDs do not match: [actual: %d, expected: %d].", mRequest.mId,
            request.getId());
      }
      return "";
    }
    return String
        .format("Received a read request %s on a busy channel (used by block %d).", request,
            mRequest.mId);
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
     * @param posToWriteUncommitted the position to commit (i.e. update mPosToWrite)
     */
    public WriteListener(long posToWriteUncommitted) {
      mPosToWriteUncommitted = posToWriteUncommitted;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
      if (!future.isSuccess()) {
        LOG.error("Failed to send packet.", future.cause());
        future.channel().close();
        return;
      }

      mLock.lock();
      try {
        Preconditions.checkState(mPosToWriteUncommitted - mPosToWrite <= PACKET_SIZE,
            "Some packet is not acked.");
        incrementMetrics(mPosToWriteUncommitted - mPosToWrite);
        mPosToWrite = mPosToWriteUncommitted;

        if (shouldRestartPacketReader()) {
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
      try {
        runInternal();
      } catch (Throwable e) {
        LOG.error("Failed to run PacketReader.", e);
        throw e;
      }
    }

    /**
     * The actual implementation of the runnable.
     */
    private void runInternal() {
      while (true) {
        final long start;
        final int packetSize;
        mLock.lock();
        try {
          start = mPosToQueue;
          long remaining = remainingToQueue();
          if (tooManyPendingPackets() || remaining <= 0) {
            mPacketReaderActive = false;
            break;
          }

          packetSize = (int) Math.min(remaining, PACKET_SIZE);
          mPosToQueue += packetSize;
        } finally {
          mLock.unlock();
        }

        DataBuffer packet;
        try {
          packet = getDataBuffer(mChannel, start, packetSize);
        } catch (Exception e) {
          LOG.error("Failed to read data.", e);
          replyError(mChannel, Protocol.Status.Code.INTERNAL, "", e);
          break;
        }
        if (packet == null) {
          // This can happen if the requested read length is greater than the actual length of the
          // block or file starting from the given offset.
          replySuccess(mChannel);
          break;
        }

        RPCProtoMessage response = RPCProtoMessage.createOkResponse(packet);
        mChannel.writeAndFlush(response).addListener(new WriteListener(start + packetSize));
      }
    }
  }

  /**
   * @param bytesRead bytes read
   */
  protected abstract void incrementMetrics(long bytesRead);
}
