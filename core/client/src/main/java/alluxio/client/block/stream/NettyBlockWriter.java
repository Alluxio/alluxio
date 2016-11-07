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
import alluxio.network.protocol.RPCBlockReadResponse;
import alluxio.network.protocol.RPCBlockWriteRequest;
import alluxio.network.protocol.RPCBlockWriteResponse;
import alluxio.network.protocol.RPCResponse;
import alluxio.network.protocol.databuffer.DataNettyBuffer;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The class to write block to data server.
 */
@NotThreadSafe
public class NettyBlockWriter implements BlockWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private final Channel mChannel;
  private final InetSocketAddress mAddress;
  private final long mBlockId;
  private final long mSessionId;

  // TODO(now): Fix
  private static final long PACKET_SIZE = 64 * 1024;
  private static final long MAX_PACKETS_IN_FLIGHT = 128;
  private static final long WRITE_TIMEOUT_MS = Constants.MINUTE_MS;

  private ReentrantLock mLock = new ReentrantLock();
  private Condition mPendingWriteFull = mLock.newCondition();
  @GuardedBy("mLock")
  private long mPosAcked = -1;
  @GuardedBy("mLock")
  private long mPos;
  @GuardedBy("mLock")
  private Throwable mThrowable = null;
  @GuardedBy("mLock")
  private boolean mDone = false;
  private Condition mDoneCondition = mLock.newCondition();
  @GuardedBy("mLock")
  private boolean mLastPacketSent = false;

  private Handler mHandler = new Handler();

  private final class Handler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws DataTransferException {
      Preconditions.checkState(msg instanceof RPCBlockWriteResponse, "Incorrect response type.");
      RPCBlockReadResponse response = (RPCBlockReadResponse) msg;
      if (response.getStatus() != RPCResponse.Status.SUCCESS) {
        throw new DataTransferException(String
            .format("Failed to write block %d from %s with status %s.", mBlockId, mAddress,
                response.getStatus().getMessage()));
      }
      try {
        mLock.lock();
        mDone = true;
        mDoneCondition.notify();
      } finally {
        mLock.unlock();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.error("Exception caught while reading response from netty channel {}.",
          cause.getMessage());
      try {
        mLock.lock();
        mThrowable = cause;
        mPendingWriteFull.notify();
        mDoneCondition.notify();
      } finally {
        mLock.unlock();
      }

      ctx.close();
    }
  }

  private final class WriteListener implements ChannelFutureListener {
    private final long mPosToAck;

    public WriteListener(long pos) {
      mPosToAck = pos;
    }
    @Override
    public void operationComplete(ChannelFuture future) {
      if (!future.isSuccess()) {
        future.channel().close();
      }

      try {
        mLock.lock();
        Preconditions.checkState(mPosToAck - mPosAcked <= PACKET_SIZE, "Some packet is not acked.");
        mPosAcked = mPosToAck;

        if (future.cause() != null) {
          mThrowable = future.cause();
          mDoneCondition.notify();
          mPendingWriteFull.notify();
          return;
        }

        if (mPos - mPosToAck < MAX_PACKETS_IN_FLIGHT * PACKET_SIZE) {
          mPendingWriteFull.notify();
        }
      } finally {
        mLock.unlock();
      }
    }
  }

  public NettyBlockWriter(final InetSocketAddress address, long blockId, long sessionId)
      throws IOException {
    mAddress = address;
    mSessionId = sessionId;
    mBlockId = blockId;

    mChannel = BlockStoreContext.acquireNettyChannel(address);
    mChannel.pipeline().addLast(mHandler);
  }

  @Override
  public long pos() {
    try {
      mLock.lock();
      return mPos;
    } finally {
      mLock.unlock();
    }
  }

  @Override
  public void writePacket(final ByteBuf buf) throws IOException {
    Preconditions.checkArgument(buf.readableBytes() <= PACKET_SIZE);
    Preconditions.checkState(!mLastPacketSent);
    final long len;
    final long offset;
    try {
      mLock.lock();
      while (true) {
        if (mThrowable != null) {
          throw new IOException(mThrowable);
        }
        if (mPos - mPosAcked < MAX_PACKETS_IN_FLIGHT * PACKET_SIZE) {
          offset = mPos;
          mPos += buf.readableBytes();
          len = buf.readableBytes();
          break;
        }
        try {
          if (!mPendingWriteFull.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            throw new IOException("Timeout while writing packet.");
          }
        } catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      }
    } finally {
      mLock.unlock();
    }

    mChannel.eventLoop().submit(new Runnable() {
      @Override
      public void run() {
        mChannel.write(new RPCBlockWriteRequest(mSessionId, mBlockId, offset, len,
            new DataNettyBuffer(buf, len))).addListener(new WriteListener(offset + len - 1));
      }
    });
  }

  public void writeLastPacket() throws IOException {
    final long offset;
    try {
      mLock.lock();
      offset = mPos;
      if (mThrowable != null) {
        throw new IOException(mThrowable);
      }
      mLastPacketSent = true;
    } finally {
      mLock.unlock();
    }

    mChannel.eventLoop().submit(new Runnable() {
      @Override
      public void run() {
        mChannel.write(new RPCBlockWriteRequest(mSessionId, mBlockId, offset, 0, null))
            .addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
      }
    });
  }

  @Override
  public void close() throws IOException {
    try {
      mLock.lock();
      while (true) {
        if (mThrowable != null) {
          throw new IOException(mThrowable);
        }
        if (mDone) {
          return;
        }
        try {
          if (!mDoneCondition.await(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
            throw new IOException("Timeout while closing NettyBlockWriter.");
          }
        } catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      }
    } finally {
      mChannel.pipeline().removeLast();
      BlockStoreContext.releaseNettyChannel(mAddress, mChannel);
    }
  }
}

