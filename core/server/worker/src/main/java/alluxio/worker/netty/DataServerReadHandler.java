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
 *    of {@link DataServerReadHandler#mCancel#mEof#mError}.
 */
@NotThreadSafe
abstract class DataServerReadHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(DataServerReadHandler.class);

  private static final long PACKET_SIZE =
      Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_READER_PACKET_SIZE_BYTES);
  private static final long MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_READER_BUFFER_SIZE_PACKETS);

  /** The executor to run {@link PacketReader}. */
  private final ExecutorService mPacketReaderExecutor;

  private final ReentrantLock mLock = new ReentrantLock();
  /**
   * Set to true if the packet reader is active. The following invariants must be maintained:
   * 1. If true, there will be at least one more packet (data, eof or error) to be sent to netty.
   * 2. If false, there will be no more packets sent to netty until it is set to true again.
   */
  @GuardedBy("mLock")
  private boolean mPacketReaderActive;
  /**
   * The next pos to queue to the netty buffer. mPosToQueue - mPosToWrite is the bytes that are
   * in netty buffer.
   */
  @GuardedBy("mLock")
  private long mPosToQueue;
  /** The next pos to write to the channel. */
  @GuardedBy("mLock")
  private long mPosToWrite;

  /**
   * mEof, mCancel and mError are the notifications processed by the packet reader thread. They can
   * be set by either the netty I/O thread or the packet reader thread. mError overrides mCancel
   * and mEof, mEof overrides mCancel.
   *
   * These notifications determine 3 ways to complete a read request.
   * 1. mEof: The read request is fulfilled. All the data requested by the client or all the data in
   *    the block/file has been read. The packet reader replies a SUCCESS response when processing
   *    mEof.
   * 2. mCancel: The read request is cancelled by the client. A cancel request is ignored if mEof
   *    is set. The packet reader replies a CANCEL response when processing mCancel.
   *    Note: The client can send a cancel request after the server has sent a SUCCESS response. But
   *    it is not possible for the client to send a CANCEL request after the channel has been
   *    released. So it is impossible for a CANCEL request from one read request to cancel
   *    another read request.
   * 3. mError: mError is set whenever an error occurs. It can be from an exception when reading
   *    packet, or writing packet to netty or the client closes the channel etc. An ERROR response
   *    is optionally sent to the client when packet reader thread process mError. The channel
   *    is closed after this error response is sent.
   *
   * Note: it is guaranteed that only one of SUCCESS and CANCEL responses is sent at most once
   * because the packet reader thread won't be restarted as long as mCancel or mEof is set except
   * when error happens (mError overrides mCancel and mEof).
   */
  @GuardedBy("mLock")
  private boolean mEof;
  @GuardedBy("mLock")
  private boolean mCancel;
  @GuardedBy("mLock")
  private Error mError;

  /** This is set when the SUCCESS or CANCEL response is sent. This is only for sanity check. */
  private volatile boolean mDone;

  /**
   * A wrapper on an error used to pass error information from the netty I/O thread to the packet
   * reader thread.
   */
  private class Error {
    final Throwable mCause;
    final boolean mNotifyClient;
    final Protocol.Status.Code mErrorCode;

    Error(Throwable cause, boolean notifyClient, Protocol.Status.Code code) {
      mCause = cause;
      mNotifyClient = notifyClient;
      mErrorCode = code;
    }
  }

  /**
   * This is only created in the netty I/O thread when a read request is received, reset when
   * another request is received.
   */
  protected volatile ReadRequestInternal mRequest;

  abstract class ReadRequestInternal implements Closeable {
    final long mId;
    final long mStart;
    final long mEnd;

    ReadRequestInternal(long id, long start, long end) {
      mId = id;
      mStart = start;
      mEnd = end;
    }
  }

  /**
   * Creates an instance of {@link DataServerReadHandler}.
   *
   * @param executorService the executor service to run {@link PacketReader}s
   */
  DataServerReadHandler(ExecutorService executorService) {
    mPacketReaderExecutor = executorService;
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    setError(ctx.channel(), new Error(null, false, Protocol.Status.Code.INTERNAL));
    ctx.fireChannelUnregistered();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
    if (!acceptMessage(object)) {
      ctx.fireChannelRead(object);
      return;
    }
    Protocol.ReadRequest msg = ((RPCProtoMessage) object).getMessage().getMessage();
    if (msg.getCancel()) {
      setCancel(ctx.channel());
      return;
    }

    reset();
    String error = validateReadRequest(msg);
    if (!error.isEmpty()) {
      setError(ctx.channel(), new Error(new IllegalArgumentException(error), true,
          Protocol.Status.Code.INVALID_ARGUMENT));
      return;
    }

    initializeRequest(msg);
    mLock.lock();
    try {
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
    setError(ctx.channel(), new Error(cause, true, Protocol.Status.Code.INTERNAL));
  }

  /**
   * @return true if there are too many packets in-flight
   */
  @GuardedBy("mLock")
  private boolean tooManyPendingPackets() {
    return mPosToQueue - mPosToWrite >= MAX_PACKETS_IN_FLIGHT * PACKET_SIZE;
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
    if (!request.getCancel() && (request.getOffset() < 0 || request.getLength() <= 0)) {
      return String.format("Invalid read bounds in read request %s.", request.toString());
    }
    return "";
  }

  /**
   * @param channel the channel
   * @param error the error
   */
  private void setError(Channel channel, Error error) {
    Preconditions.checkNotNull(error);
    mLock.lock();
    try {
      if (mError != null) {
        return;
      }
      mError = error;
      if (!mPacketReaderActive) {
        mPacketReaderActive = true;
        mPacketReaderExecutor.submit(new PacketReader(channel));
      }
    } finally {
      mLock.unlock();
    }
  }

  /**
   * @param channel the channel
   */
  private void setEof(Channel channel) {
    mLock.lock();
    try {
      if (mError != null || mCancel || mEof) {
        return;
      }
      mEof = true;
      if (!mPacketReaderActive) {
        mPacketReaderActive = true;
        mPacketReaderExecutor.submit(new PacketReader(channel));
      }
    } finally {
      mLock.unlock();
    }
  }

  /**
   * @param channel the channel
   */
  private void setCancel(Channel channel) {
    mLock.lock();
    try {
      if (mError != null || mEof || mCancel) {
        return;
      }
      mCancel = true;
      if (!mPacketReaderActive) {
        mPacketReaderActive = true;
        mPacketReaderExecutor.submit(new PacketReader(channel));
      }
    } finally {
      mLock.unlock();
    }
  }

  /**
   * Resets all the states.
   */
  private void reset() {
    mLock.lock();
    try {
      Preconditions.checkState(mPacketReaderActive == false);
      mPosToQueue = 0;
      mPosToWrite = 0;
      mEof = false;
      mCancel = false;
      mError = null;
      mRequest = null;
      mDone = false;
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
   * @param bytesRead bytes read
   */
  protected abstract void incrementMetrics(long bytesRead);

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
        setError(future.channel(), new Error(future.cause(), true, Protocol.Status.Code.INTERNAL));
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
      } finally {
        mLock.unlock();
      }
    }

    /**
     * @return true if we should restart the packet reader
     */
    @GuardedBy("mLock")
    private boolean shouldRestartPacketReader() {
      return !mPacketReaderActive && !tooManyPendingPackets() && mPosToQueue < mRequest.mEnd
          && mError == null && !mCancel && !mEof;
    }
  }

  /**
   * A runnable that reads packets and writes them to the channel.
   */
  private class PacketReader implements Runnable {
    private Channel mChannel;

    /**
     * Creates an instance of the {@link PacketReader}.
     *
     * @param channel the channel
     */
    PacketReader(Channel channel) {
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
        mLock.lock();
        try {
          start = mPosToQueue;
          eof = mEof;
          cancel = mCancel;
          error = mError;

          if (eof || cancel || error != null || tooManyPendingPackets()) {
            mPacketReaderActive = false;
            break;
          }

          packetSize = (int) Math.min(mRequest.mEnd - mPosToQueue, PACKET_SIZE);

          // packetSize should always be > 0 here when reaches here.
          Preconditions.checkState(packetSize > 0);
        } finally {
          mLock.unlock();
        }

        DataBuffer packet;
        try {
          packet = getDataBuffer(mChannel, start, packetSize);
        } catch (Exception e) {
          LOG.error("Failed to read data.", e);
          setError(mChannel, new Error(e, true, Protocol.Status.Code.INTERNAL));
          continue;
        }
        if (packet != null) {
          mLock.lock();
          try {
            mPosToQueue += packet.getLength();
          } finally {
            mLock.unlock();
          }
        }
        if (packet == null || packet.getLength() < packetSize
            || start + packetSize == mRequest.mEnd) {
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
            mRequest.close();
          }
        } catch (IOException e) {
          LOG.error("Failed to close the request.", e);
        }
        if (error.mNotifyClient) {
          replyError(error.mErrorCode, "", error.mCause);
        }
      } else if (eof || cancel) {
        try {
          Preconditions.checkNotNull(mRequest);
          mRequest.close();
        } catch (IOException e) {
          setError(mChannel, new Error(e, true, Protocol.Status.Code.INTERNAL));
        }
        if (eof) {
          replyEof();
        } else if (cancel) {
          replyCancel();
        }
      }
    }

    /**
     * Writes an error read response to the channel and closes the channel after that.
     */
    private void replyError(Protocol.Status.Code code, String message, Throwable e) {
      mChannel.writeAndFlush(RPCProtoMessage.createResponse(code, message, e, null))
          .addListener(ChannelFutureListener.CLOSE);
    }

    /**
     * Writes a success response.
     */
    private void replyEof() {
      Preconditions.checkState(!mDone);
      mDone = true;
      mChannel.writeAndFlush(RPCProtoMessage.createOkResponse(null))
          .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE);
    }

    /**
     * Writes a cancel response.
     */
    private void replyCancel() {
      Preconditions.checkState(!mDone);
      mDone = true;
      mChannel.writeAndFlush(RPCProtoMessage.createCancelResponse())
          .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE);
    }
  }
}
