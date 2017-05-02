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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.network.protocol.RPCProtoMessage;
import alluxio.network.protocol.databuffer.DataBuffer;
import alluxio.network.protocol.databuffer.DataByteBuffer;
import alluxio.util.CommonUtils;
import alluxio.util.proto.ProtoMessage;
import alluxio.worker.block.io.LocalFileBlockReader;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A packet reader that simply reads packets from a local file.
 */
@NotThreadSafe
public final class LocalFilePacketReader implements PacketReader {
  private static final long LOCAL_READ_PACKET_SIZE =
      Configuration.getBytes(PropertyKey.USER_LOCAL_READER_PACKET_SIZE_BYTES);

  /** The file reader to read a local block. */
  private final LocalFileBlockReader mReader;

  private long mPos;
  private final long mEnd;

  /**
   * Creates an instance of {@link LocalFilePacketReader}.
   *
   * @param reader the local file block reader
   * @param offset the offset
   * @param len the length to read
   */
  public LocalFilePacketReader(LocalFileBlockReader reader, long offset, long len) {
    mReader = reader;
    mPos = offset;
    mEnd = offset + len;
  }

  @Override
  public DataBuffer readPacket() {
    if (mPos >= mEnd) {
      return null;
    }
    ByteBuffer buffer = mReader.read(mPos, Math.min(LOCAL_READ_PACKET_SIZE, mEnd - mPos));
    DataBuffer dataBuffer = new DataByteBuffer(buffer, buffer.remaining());
    mPos += dataBuffer.getLength();
    return dataBuffer;
  }

  @Override
  public long pos() {
    return mPos;
  }

  @Override
  public void close() {
    mReader.close();
  }

  /**
   * Factory class to create {@link LocalFilePacketReader}s.
   */
  public static class Factory implements PacketReader.Factory {
    private final String mPath;

    /**
     * Creates an instance of {@link Factory}.
     *
     * @param path the file path
     */
    public Factory(String path) {
      mPath = path;
    }

    @Override
    public PacketReader create(long offset, long len) {
      return new LocalFilePacketReader(new LocalFileBlockReader(mPath), offset, len);
    }

    @Override
    public boolean isShortCircuit() {
      return true;
    }
  }

  public static class NettyRPCContext {
    private Channel mChannel;
    private long mTimeoutMs;

    public Channel getChannel() {
      return mChannel;
    }

    public long getTimeoutMs() {
      return mTimeoutMs;
    }
  }

  public static class NettyRPC {
    public static ProtoMessage call(final NettyRPCContext context, ProtoMessage request)
        throws InterruptedException {
      Promise<ProtoMessage> promise = context.getChannel().eventLoop().newPromise();
      context.getChannel().pipeline().addLast(new RPCHandler(promise));
      context.getChannel().writeAndFlush(new RPCProtoMessage(request));
      try {
        return promise.get(context.getTimeoutMs(), TimeUnit.MILLISECONDS);
      } catch (ExecutionException e) {
        throw CommonUtils.propagate(e.getCause() == null ? e : e.getCause());
      } catch (TimeoutException e) {
        throw CommonUtils.propagate(e);
      } finally {
        context.getChannel().pipeline().removeLast();
      }
    }

    private static class RPCHandler extends ChannelInboundHandlerAdapter {
      private final Promise<ProtoMessage> mPromise;

      public RPCHandler(Promise<ProtoMessage> promise) {
        mPromise = promise;
      }

      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!acceptMessage(msg)) {
          ctx.fireChannelRead(msg);
          return;
        }

        ProtoMessage message = ((RPCProtoMessage) msg).getMessage();
        mPromise.trySuccess(message);
      }

      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        mPromise.tryFailure(cause);
        ctx.close();
      }

      @Override
      public void channelUnregistered(ChannelHandlerContext ctx) {
        mPromise.tryFailure(new IOException("ChannelClosed"));
      }

      protected boolean acceptMessage(Object msg) {
        return msg instanceof RPCProtoMessage;
      }

    }
  }
}

