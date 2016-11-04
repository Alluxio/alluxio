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
import alluxio.client.block.BlockStoreContext;
import alluxio.exception.DataTransferException;
import alluxio.network.netty.MessageQueue;
import alluxio.network.protocol.RPCBlockReadRequest;
import alluxio.network.protocol.RPCBlockReadResponse;
import alluxio.network.protocol.RPCResponse;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The class to read remote block from data server.
 */
@NotThreadSafe
public class NettyBlockReader implements BlockReader {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final Channel mChannel;
  private final InetSocketAddress mAddress;
  private final long mBlockId;
  private long mPos;
  private int mBytesRemaining = -1;

  private volatile boolean mDone = false;

  private final class NettyPacketQueue extends MessageQueue<ByteBuf> {
    public NettyPacketQueue() {
      super(MessageQueue.Options.defaultOptions());
    }

    protected void signal() {
      mChannel.read();
    }
  }

  private NettyPacketQueue mPacketQueue = new NettyPacketQueue();
  private Handler mHandler = new Handler();

  public class Handler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      Preconditions.checkState(msg instanceof RPCBlockReadResponse, "Incorrect response type.");
      RPCBlockReadResponse response = (RPCBlockReadResponse) msg;
      if (response.getStatus() == RPCResponse.Status.SUCCESS ||
          response.getStatus() == RPCResponse.Status.STREAM_PACKET) {
        try {
          mPacketQueue.offerMessage(response.getPayloadData(),
              response.getStatus() == RPCResponse.Status.SUCCESS);
        } catch (Throwable e) {
          ReferenceCountUtil.release(response.getPayloadData());
        }
      } else {
        mPacketQueue.exceptionCaught(new DataTransferException(String
            .format("Failed to read block %d from %s with status %s.", mBlockId, mAddress,
                response.getStatus().getMessage())));
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Exception caught while reading response from netty channel {}.",
          cause.getMessage());
      mPacketQueue.exceptionCaught(cause);
      ctx.close();
    }
  }

  public NettyBlockReader(final InetSocketAddress address, long blockId, long offset, int len,
      long lockId, long sessionId) throws IOException {
    mAddress = address;
    mBlockId = blockId;
    mBytesRemaining = len;
    mPos = offset;

    mChannel = BlockStoreContext.acquireNettyChannel(address);
    ChannelPipeline pipeline = mChannel.pipeline();
    if (pipeline.last() instanceof Handler) {
      pipeline.removeLast();
    }
    mChannel.pipeline().addLast(mHandler);

    Preconditions.checkArgument(len >= 0, "Len must be >= 0");
    mChannel.writeAndFlush(new RPCBlockReadRequest(blockId, offset, len, lockId, sessionId))
        .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public ByteBuf readPacket() throws IOException {
    try {
      ByteBuf buf = mPacketQueue.pollMessage();
      mBytesRemaining -= buf.readableBytes();
      mPos += buf.readableBytes();
      Preconditions.checkState(mBytesRemaining >= 0, "mBytesRemaining must be >= 0.");
      mDone = mBytesRemaining == 0;
      return buf;
    } catch (EOFException e) {
      // This should never happen.
      throw Throwables.propagate(e);
    } catch (DataTransferException e) {
      mDone = true;
      throw new IOException(e);
    } catch (Throwable e) {
      // TODO(peis): Retry once if e is caused by ClosedChannelException.
      mChannel.close();
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (mDone) {
        return;
      }
      try {
        ChannelFuture channelFuture =
            mChannel.writeAndFlush(RPCBlockReadRequest.createCancelRequest(mBlockId)).sync();
        channelFuture.addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
        if (!channelFuture.isSuccess()) {
          throw new IOException(channelFuture.cause());
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      // wait for response.
      while (true) {
        try {
          ByteBuf buf = readPacket();
          ReferenceCountUtil.release(buf);
        } catch (IOException e) {
          if (mDone) {
            return;
          }
          throw e;
        }
      }
    } finally {
      BlockStoreContext.releaseNettyChannel(mAddress, mChannel);
    }
  }
}

