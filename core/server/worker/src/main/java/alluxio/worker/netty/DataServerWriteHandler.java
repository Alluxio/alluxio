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
import alluxio.util.network.NettyUtils;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
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
 * Thread model:
 * Only two threads are involved at a given point of time: netty I/O thread, packet writer thread.
 * 1. The netty I/O thread reads packets from the wire and pushes them to the buffer if there is
 *    no error seen so far. This packet reading can be ended by an EOF packet, a CANCEL packet or
 *    an exception. When one of these 3 happens, a special packet is pushed to the buffer.
 * 2. The packet writer thread keeps polling packets from the buffer and processes them.
 *    NOTE: it is guaranteed that there is only one packet writer thread active at a given time.
 */
@NotThreadSafe
abstract class DataServerWriteHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(DataServerWriteHandler.class);

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
  /** The buffer for packets read from the channel. */
  @GuardedBy("mLock")
  private Queue<ByteBuf> mPackets = new LinkedList<>();

  /**
   * Set to true if the packet writer is active.
   *
   * The following invariants (happens-before orders) must be maintained:
   * 1. When mPacketWriterActive is true, it is guaranteed that mPackets is polled at least
   *    once after the lock is released. This is guaranteed even when there is an exception
   *    thrown when writing the packet.
   * 2. When mPacketWriterActive is false, it is guaranteed that mPackets won't be polled before
   *    before someone sets it to true again.
   *
   * The above are achieved by protecting it with "mLock". It is set to true when a new packet
   * is read when it is false. It set to false when one of the these is true: 1) The mPackets queue
   * is empty; 2) The write request is fulfilled (eof or cancel is received); 3) A failure occurs.
   */
  @GuardedBy("mLock")
  private boolean mPacketWriterActive;

  /**
   * The error seen in either the netty I/O thread (e.g. failed to read from the network) or the
   * packet writer thread (e.g. failed to write the packet).
   */
  @GuardedBy("mLock")
  private Error mError;

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
   * mRequest is initialized only once for a whole file or block in
   * {@link DataServerReadHandler#channelRead(ChannelHandlerContext, Object)}.
   * After that, it should only be used by the packet writer thread.
   * It is safe to read those final primitive fields (e.g. mId, mSessionId) if mError is not set
   * from any thread (not such usage in the code now). It is destroyed when the write request is
   * done (complete or cancel) or an error is seen.
   */
  protected volatile  WriteRequestInternal mRequest;

  abstract class WriteRequestInternal implements Closeable {
    /** This ID can either be block ID or temp UFS file ID. */
    final long mId;
    final long mSessionId;

    WriteRequestInternal(long id, long sessionId) {
      mId = id;
      mSessionId = sessionId;
    }

    /**
     * Cancels the request.
     *
     * @throws IOException if I/O errors occur
     */
    abstract void cancel() throws IOException;
  }

  /**
   * The next pos to queue to the buffer. This is only updated and used by the netty I/O thread.
   */
  private long mPosToQueue;
  /**
   * The next pos to write to the block worker. This is only updated by the packet writer
   * thread. The netty I/O reads this only for sanity check during initialization.
   */
  protected volatile long mPosToWrite;

  /**
   * Creates an instance of {@link DataServerWriteHandler}.
   *
   * @param executorService the executor service to run {@link PacketWriter}s
   */
  DataServerWriteHandler(ExecutorService executorService) {
    mPacketWriterExecutor = executorService;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object object) throws Exception {
    if (!acceptMessage(object)) {
      ctx.fireChannelRead(object);
      return;
    }

    RPCProtoMessage msg = (RPCProtoMessage) object;
    Protocol.WriteRequest writeRequest = msg.getMessage().getMessage();
    if (writeRequest.getOffset() == 0) {
      initializeRequest(msg);
    }

    // Validate msg and return error if invalid. Init variables if necessary.
    String error = validateRequest(msg);
    if (!error.isEmpty()) {
      pushAbortPacket(ctx, new Error(null, true, Protocol.Status.Code.INVALID_ARGUMENT));
      return;
    }

    mLock.lock();
    try {
      // If we have seen an error, return early and release the data. This can only
      // happen for those mis-behaving clients who first sends some invalid requests, then
      // then some random data. It can leak memory if we do not release buffers here.
      if (mError != null) {
        if (msg.getPayloadDataBuffer() != null) {
          msg.getPayloadDataBuffer().release();
        }
        return;
      }

      ByteBuf buf;
      if (writeRequest.getEof()) {
        buf = EOF;
      } else if (writeRequest.getCancel()) {
        buf = CANCEL;
      } else {
        DataBuffer dataBuffer = msg.getPayloadDataBuffer();
        Preconditions.checkState(dataBuffer != null && dataBuffer.getLength() > 0);
        assert dataBuffer.getNettyOutput() instanceof ByteBuf;
        buf = (ByteBuf) dataBuffer.getNettyOutput();
        mPosToQueue += buf.readableBytes();
      }
      if (!mPacketWriterActive) {
        mPacketWriterActive = true;
        mPacketWriterExecutor.submit(new PacketWriter(ctx));
      }
      mPackets.offer(buf);
      if (tooManyPacketsInFlight()) {
        NettyUtils.disableAutoRead(ctx.channel());
      }
    } finally {
      mLock.unlock();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("Failed to write block.", cause);
    pushAbortPacket(ctx, new Error(cause, true, Protocol.Status.Code.INTERNAL));
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    pushAbortPacket(ctx, new Error(null, false, Protocol.Status.Code.INTERNAL));
    ctx.fireChannelUnregistered();
  }

  /**
   * @return true if there are too many packets in flight
   */
  @GuardedBy("mLock")
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
    if (request.getOffset() != mPosToQueue) {
      return String
          .format("Offsets do not match [received: %d, expected: %d].", request.getOffset(),
              mPosToQueue);
    }
    if (msg.getPayloadDataBuffer() != null && msg.getPayloadDataBuffer().getLength() > 0 && (
        request.getCancel() || request.getEof())) {
      return String.format("Found data in a cancel/eof message.");
    }
    return "";
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
        // This should never happen.
        LOG.error("Failed to run PacketWriter.", e);
        throw e;
      }
    }

    /**
     * The actual implementation of the runnable.
     */
    private void runInternal() {
      boolean eof;
      boolean cancel;
      boolean abort;

      while (true) {
        ByteBuf buf;
        mLock.lock();
        try {
          buf = mPackets.poll();
          if (buf == null || buf == EOF || buf == CANCEL || buf == ABORT) {
            eof = buf == EOF;
            cancel = buf == CANCEL;
            abort = buf == ABORT;
            mPacketWriterActive = false;
            break;
          }
          if (mError != null) {
            release(buf);
            continue;
          }
          if (!tooManyPacketsInFlight()) {
            NettyUtils.enableAutoRead(mCtx.channel());
          }
        } finally {
          mLock.unlock();
        }

        try {
          mPosToWrite += buf.readableBytes();
          incrementMetrics(buf.readableBytes());
          writeBuf(buf, mPosToWrite);
        } catch (Exception e) {
          LOG.warn("Failed to write packet {}", e.getMessage());
          pushAbortPacket(mCtx, new Error(e, true, Protocol.Status.Code.INTERNAL));
        } finally {
          release(buf);
        }
      }

      try {
        if (abort) {
          cancel();
          replyError(mCtx.channel());
        }
        if (cancel) {
          cancel();
          replyCancel(mCtx.channel());
        }
        if (eof) {
          complete();
          replySuccess(mCtx.channel());
        }
      } catch (IOException e) {
        LOG.warn("Failed to abort, cancel or complete the write request with error {}.",
            e.getMessage());
        mCtx.close();
      }
    }

    /**
     * Completes this write.
     *
     * @throws IOException if I/O related errors occur
     */
    private void complete() throws IOException {
      if (mRequest != null) {
        mRequest.close();
        mRequest = null;
      }
      mPosToWrite = 0;
    }

    /**
     * Cancels the write.
     *
     * @throws IOException if I/O related errors occur
     */
    private void cancel() throws IOException {
      if (mRequest != null) {
        mRequest.cancel();
        mRequest = null;
      }
      mPosToWrite = 0;
    }

    /**
     * Writes a response to signify the success of the write request.
     *
     * @param channel the channel
     */
    private void replySuccess(Channel channel) {
      channel.writeAndFlush(RPCProtoMessage.createOkResponse(null))
          .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE);
      NettyUtils.enableAutoRead(channel);
    }

    /**
     * Writes a response to signify the successful cancellation of the write request.
     *
     * @param channel the channel
     */
    private void replyCancel(Channel channel) {
      channel.writeAndFlush(RPCProtoMessage.createCancelResponse())
          .addListeners(ChannelFutureListener.CLOSE_ON_FAILURE);
      NettyUtils.enableAutoRead(channel);
    }

    /**
     * Writes an error response to the channel and closes the channel after that.
     *
     * @param channel the channel
     */
    private void replyError(Channel channel) {
      Error error;
      mLock.lock();
      try {
        error = Preconditions.checkNotNull(mError);
      } finally {
        mLock.unlock();
      }

      if (error.mNotifyClient) {
        channel
            .writeAndFlush(RPCProtoMessage.createResponse(error.mErrorCode, "", error.mCause, null))
            .addListener(ChannelFutureListener.CLOSE);
      }
    }
  }

  /**
   * Pushes {@link DataServerWriteHandler#ABORT} to the buffer if there has been no error so far.
   *
   * @param ctx the channel context
   * @param error the error
   */
  private void pushAbortPacket(ChannelHandlerContext ctx, Error error) {
    mLock.lock();
    if (mError != null) {
      return;
    }
    mError = error;
    try {
      mPackets.offer(ABORT);
      if (!mPacketWriterActive) {
        mPacketWriterActive = true;
        mPacketWriterExecutor.submit(new PacketWriter(ctx));
      }
    } finally {
      mLock.unlock();
    }
  }

  /**
   * Releases a {@link ByteBuf}.
   *
   * @param buf the netty byte buffer
   */
  private static void release(ByteBuf buf) {
    if (buf != null && buf != EOF && buf != CANCEL) {
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
   * Initializes the handler if necessary.
   *
   * @param msg the block write request
   * @throws Exception if it fails to initialize
   */
  protected void initializeRequest(RPCProtoMessage msg) throws Exception {
    Preconditions.checkState(mRequest == null);
    mPosToQueue = 0;
    Preconditions.checkState(mPosToWrite == 0);
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
