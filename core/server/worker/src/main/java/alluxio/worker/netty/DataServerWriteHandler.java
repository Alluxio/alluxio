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
 * 4. When an error occurs, the channel is closed. All the buffered packets are released when the
 *    channel is deregistered.
 */
@NotThreadSafe
public abstract class DataServerWriteHandler extends ChannelInboundHandlerAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(DataServerWriteHandler.class);

  private static final int MAX_PACKETS_IN_FLIGHT =
      Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_WRITER_BUFFER_SIZE_PACKETS);

  /** The executor service to run the {@link PacketWriter}s. */
  private final ExecutorService mPacketWriterExecutor;

  /**
   * Special packets used to pass control information from the I/O thread to the packet writer
   * thread. This makes the code cleaner. It can be replaced with booleans guarded by locks.
   * Note: if anyone wants to change this to booleans, make sure EOF or CANCEL response is sent
   * only once.
   */
  private static final ByteBuf EOF = Unpooled.buffer(0);
  private static final ByteBuf CANCEL = Unpooled.buffer(0);

  private ReentrantLock mLock = new ReentrantLock();
  /**
   * The buffer for packets read from the channel. {@link java.util.concurrent.BlockingQueue} is
   * used here because we do not want the consumer thread to be blocked when the queue is empty.
   * {@link java.util.concurrent.ConcurrentLinkedQueue} is not used because we want to make sure
   * netty's autoread is set properly. Consider the following event sequence:
   * t1: queue full. (I/O thread)
   * t2: all packets are consumed from the queue. The queue became empty. (PacketWriter thread)
   * t3: turn off the netty auto read.
   *
   * After t3, nothing will be read. We need synchronization when turning on/off auto-read and
   * producing/consuming packets from the queue.
   */
  @GuardedBy("mLock")
  private Queue<ByteBuf> mPackets = new LinkedList<>();

  /**
   * Set to true if the packet writer is active.
   *
   * The following invariant must be maintained.
   * 1. After this is set to true, there must be a thread that reads from mPackets at least once
   *    and eventually sets mPacketWriterActive to false.
   * 2. After this is set to false, there must be no thread that reads from mPackets.
   *
   * The above can be achieved by protecting it with "mLock". It is set to true when a new packet
   * is read and it is false. It set to false when one of the these is true: 1) The mPackets queue
   * is empty; 2) The write request is fulfilled (eof or cancel is received); 3) A failure occurs.
   */
  @GuardedBy("mLock")
  private boolean mPacketWriterActive = false;

  /**
   * mRequest is only updated once per block/file read. It needs to be updated 'atomically' since
   * it is not guarded by any lock. All fields in mRequest must be final.
   */
  protected volatile WriteRequestInternal mRequest = null;

  protected abstract class WriteRequestInternal implements Closeable {
    // This ID can either be block ID or temp UFS file ID.
    public final long mId;
    public final long mSessionId;

    WriteRequestInternal(long id, long sessionId) {
      mId = id;
      mSessionId = sessionId;
    }

    /**
     * Cancels the request.
     *
     * @throws IOException if I/O errors occur
     */
    public abstract void cancel() throws IOException;
  }

  /**
   * The next pos to queue to the buffer. Updated by the packet reader (the netty I/O thread). It is
   * used to validate the request.
   */
  private volatile long mPosToQueue = 0;
  /**
   * The next pos to write to the block worker. Updated by the packet writer thread.
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
    Protocol.WriteRequest writeRequest = msg.getMessage().getMessage();
    initializeRequest(msg);

    // Validate msg and return error if invalid. Init variables if necessary.
    String error = validateRequest(msg);
    if (!error.isEmpty()) {
      replyError(ctx.channel(), Protocol.Status.Code.INVALID_ARGUMENT, error, null);
      return;
    }

    mLock.lock();
    try {
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
    LOG.error("Failed to write block " + (mRequest == null ? -1 : mRequest.mId) + ".", cause);
    replyError(ctx.channel(), Protocol.Status.Code.INTERNAL, "", cause);
  }

  @Override
  public void channelUnregistered(ChannelHandlerContext ctx) {
    try {
      abortAbnormally();
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
    if (msg.getPayloadDataBuffer() != null && msg.getPayloadDataBuffer().getLength() > 0 && (
        request.getCancel() || request.getEof())) {
      return String.format("Found data in a cancel/eof message.");
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
   * Aborts the write abnormally due to some error. Called after the channel is closed.
   *
   * @throws IOException if an I/O error occur
   */
  private void abortAbnormally() throws IOException {
    mLock.lock();
    try {
      for (ByteBuf buf : mPackets) {
        buf.release();
      }
    } finally {
      mLock.unlock();
    }

    WriteRequestInternal request = mRequest;
    mRequest = null;
    if (request != null) {
      request.cancel();
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
        // This should never happen.
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
          if (buf == EOF || buf == CANCEL) {
            Preconditions.checkState(mPackets.isEmpty());
          }
          if (buf == null) {
            // Case 1 to set mPacketWriterActive to false. See mPacketWriterActive's javadoc.
            mPacketWriterActive = false;
            break;
          }
          if (!tooManyPacketsInFlight()) {
            NettyUtils.enableAutoRead(mCtx.channel());
          }
        } finally {
          mLock.unlock();
        }

        try {
          // NOTE: We must break if it is EOF or CANCEL to maintain the invariant
          // that whenever mPacketWriterActive is false, no one should be polling from
          // mPackets afterwards.
          if (buf == EOF) {
            complete();
            replySuccess(mCtx.channel());
            break;
          } else if (buf == CANCEL) {
            cancel();
            replyCancel(mCtx.channel());
            break;
          } else {
            mPosToWrite += buf.readableBytes();
            incrementMetrics(buf.readableBytes());
            writeBuf(buf, mPosToWrite);
          }
        } catch (Exception e) {
          // Case 3 to set mPacketWriterActive to false. See mPacketWriterActive's javadoc.
          mLock.lock();
          try {
            mPacketWriterActive = false;
          } finally {
            mLock.unlock();
          }
          exceptionCaught(mCtx, e);
          break;
        } finally {
          buf.release();
        }
      }
    }

    /**
     * Completes this write.
     *
     * @throws IOException if I/O related errors occur
     */
    private void complete() throws IOException {
      // Case 2 to set mPacketWriterActive to false. See mPacketWriterActive's javadoc.
      mLock.lock();
      try {
        mPacketWriterActive = false;
      } finally {
        mLock.unlock();
      }
      mRequest.close();
      mRequest = null;
      mPosToQueue = 0;
      mPosToWrite = 0;
    }

    /**
     * Cancels the write.
     *
     * @throws IOException if I/O related errors occur
     */
    private void cancel() throws IOException {
      // Case 2 to set mPacketWriterActive to false. See mPacketWriterActive's javadoc.
      mLock.lock();
      try {
        mPacketWriterActive = false;
      } finally {
        mLock.unlock();
      }
      mRequest.cancel();
      mRequest = null;
      mPosToQueue = 0;
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
