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

package alluxio.client.block.stream;

import alluxio.Constants;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.exception.runtime.CancelledRuntimeException;
import alluxio.exception.runtime.DeadlineExceededRuntimeException;
import alluxio.exception.runtime.InternalRuntimeException;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.network.NettyUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.util.proto.ProtoUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Preconditions;
import io.grpc.Status;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A netty packet reader that streams a region from a netty data server.
 *
 * Protocol:
 * 1. The client sends a read request (id, offset, length).
 * 2. Once the server receives the request, it streams packets to the client. The streaming pauses
 *    if the server's buffer is full and resumes if the buffer is not full.
 * 3. The client reads packets from the stream. Reading pauses if the client buffer is full and
 *    resumes if the buffer is not full. If the client can keep up with network speed, the buffer
 *    should have at most one packet.
 * 4. The client stops reading if it receives an empty packet which signifies the end of the stream.
 * 5. The client can cancel the read request at anytime. The cancel request is ignored by the
 *    server if everything has been sent to channel.
 * 6. If the client wants to reuse the channel, the client must read all the packets in the channel
 *    before releasing the channel to the channel pool.
 * 7. To make it simple to handle errors, the channel is closed if any error occurs.
 */
@ThreadSafe
public final class PositionNettyDataReader {
  private static final Logger LOG = LoggerFactory.getLogger(PositionNettyDataReader.class);
  private static final long READ_TIMEOUT = 60 * Constants.SECOND_MS;
  private final FileSystemContext mContext;
  private final Channel mChannel;
  private final Protocol.ReadRequest.Builder mReadRequestBuilder;
  private final WorkerNetAddress mAddress;
  // Map from request id to read operation
  private final ConcurrentHashMap<Long, ReadOperation> mReadOperations = new ConcurrentHashMap<>();

  private static AtomicLong mRequestIdCounter = new AtomicLong();

  private boolean mClosed = false;

  /**
   * Creates an instance of {@link NettyDataReader}. If this is used to read a block remotely, it
   * requires the block to be locked beforehand and the lock ID is passed to this class.
   *
   * @param context the file system context
   * @param address the netty data server address
   * @param readRequest the read request
   */
  private PositionNettyDataReader(FileSystemContext context, WorkerNetAddress address,
      Protocol.ReadRequest.Builder readRequest) throws IOException {
    mContext = context;
    mAddress = address;
    mReadRequestBuilder = readRequest;

    mChannel = mContext.acquireNettyChannel(address);
    mChannel.pipeline().addLast(new PositionReadHandler());
  }

  public int read(ByteBuffer buf, long size, long offset) {
    Preconditions.checkState(!mClosed, "Netty reader is closed while reading");
    long requestId = mRequestIdCounter.getAndIncrement();
    mChannel.writeAndFlush(new RPCProtoMessage(new ProtoMessage(
        mReadRequestBuilder.setOffset(offset).setLength(size).setRequestId(requestId).build())))
        .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
    
    ReadOperation op = new ReadOperation(buf);
    mReadOperations.put(mRequestIdCounter.getAndIncrement(), op);
    try {
      op.wait(READ_TIMEOUT);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new CancelledRuntimeException("Thread interrupted");
    }
    if (!op.mDone) {
      throw new DeadlineExceededRuntimeException(String.format("Read block %s of size %s from %s exceed read timeout %s",
          mReadRequestBuilder.getBlockId(), size, offset, READ_TIMEOUT));
    }
    if (op.mException != null) {
      throw op.mException;
    }
    
    return op.mReadSize;
  }

  // TODO(lu) better deal with close
  public synchronized void close() {
    if (mClosed) {
      return;
    }
    if (!mChannel.isOpen()) {
      return;
    }

    if (mChannel.isOpen()) {
      mChannel.pipeline().removeLast();

      // Make sure "autoread" is on before releasing the channel.
      NettyUtils.enableAutoRead(mChannel);
    }
    mContext.releaseNettyChannel(mAddress, mChannel);
    mClosed = true;
  }

  /**
   * The netty handler that reads read response from worker
   * and sync to corresponding client thread.
   */
  private class PositionReadHandler extends ChannelInboundHandlerAdapter {
    /**
     * Default constructor.
     */
    PositionReadHandler() {}

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
      // Precondition check is not used here to avoid calling msg.getClass().getCanonicalName()
      // all the time.
      if (!(msg instanceof RPCProtoMessage)) {
        throw new IllegalStateException(String
            .format("Incorrect response type %s, %s.", msg.getClass().getCanonicalName(), msg));
      }
      
      ByteBuf buf;
      RPCProtoMessage response = (RPCProtoMessage) msg;
      ProtoMessage message = response.getMessage();
      if (message.isResponse()) {
        Protocol.Response readResponse = message.asResponse();
        long request_id = readResponse.getRequestId();
        ReadOperation op = mReadOperations.get(request_id);
        Status status = ProtoUtils.fromProto(readResponse.getStatus());
        if (status != Status.OK) {
          op.mException = new InternalRuntimeException(String.format("Read status is %s instead of OK", status));
        } else {
          DataBuffer dataBuffer = response.getPayloadDataBuffer();
          if (dataBuffer == null) {
            op.mReadSize = 0;
          } else {
            Preconditions.checkState(dataBuffer.getNettyOutput() instanceof ByteBuf);
            buf = (ByteBuf) dataBuffer.getNettyOutput();
            op.mBuffer.limit(buf.readableBytes());
            buf.readBytes(op.mBuffer);
          }
        }
        mReadOperations.remove(request_id);
        op.mDone = true;
        op.notify();
      } else {
        throw new IllegalStateException(
            String.format("Incorrect response type %s.", message.toString()));
      }
    }

    // TODO(lu) exception handling / channel unregistered?
  }
  
  class ReadOperation {
    private boolean mDone;
    private final ByteBuffer mBuffer;
    private int mReadSize;
    private AlluxioRuntimeException mException;

    public ReadOperation(ByteBuffer data) {
      mBuffer = data;
    }
  }
}

