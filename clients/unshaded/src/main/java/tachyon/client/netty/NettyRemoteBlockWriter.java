/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

import tachyon.Constants;
import tachyon.client.RemoteBlockWriter;
import tachyon.conf.TachyonConf;
import tachyon.network.ChannelType;
import tachyon.network.NettyUtils;
import tachyon.network.protocol.RPCBlockWriteRequest;
import tachyon.network.protocol.RPCBlockWriteResponse;
import tachyon.network.protocol.RPCMessage;
import tachyon.network.protocol.RPCMessageDecoder;
import tachyon.network.protocol.RPCMessageEncoder;
import tachyon.network.protocol.RPCResponse;
import tachyon.network.protocol.databuffer.DataByteArrayChannel;

/**
 * Write data to a remote data server using Netty.
 */
public final class NettyRemoteBlockWriter implements RemoteBlockWriter {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  // TODO: refactor netty client code common to both reading and writing blocks.

  // The maximum number of seconds to wait for a response from the server.
  private static final long TIMEOUT_SECOND = 1L;

  // Share both the encoder and decoder with all the client pipelines.
  private static final RPCMessageEncoder ENCODER = new RPCMessageEncoder();
  private static final RPCMessageDecoder DECODER = new RPCMessageDecoder();

  private static final TachyonConf TACHYON_CONF = new TachyonConf();
  private static final ChannelType CHANNEL_TYPE = TACHYON_CONF.getEnum(
      Constants.USER_NETTY_CHANNEL, ChannelType.defaultType());
  private static final Class<? extends SocketChannel> CLIENT_CHANNEL_CLASS = NettyUtils
      .getClientChannelClass(CHANNEL_TYPE);
  // Reuse EventLoopGroup for all clients.
  // Use daemon threads so the JVM is allowed to shutdown even when daemon threads are alive.
  // If number of worker threads is 0, Netty creates (#processors * 2) threads by default.
  private static final EventLoopGroup WORKER_GROUP = NettyUtils.createEventLoop(CHANNEL_TYPE,
      TACHYON_CONF.getInt(Constants.USER_NETTY_WORKER_THREADS, 0), "netty-client-worker-%d", true);

  private final Bootstrap mClientBootstrap;
  private final ClientHandler mHandler;

  private boolean mOpen;
  private InetSocketAddress mAddress;
  private long mBlockId;
  private long mUserId;

  // Total number of bytes written to the remote block.
  private long mWrittenBytes;

  public NettyRemoteBlockWriter() {
    mHandler = new ClientHandler();
    mClientBootstrap = createClientBootstrap();
    mOpen = false;
  }

  @Override
  public void open(InetSocketAddress address, long blockId, long userId) throws IOException {
    if (mOpen) {
      throw new IOException("This writer is already open for address: " + mAddress + ", blockId: "
          + mBlockId + ", userId: " + mUserId);
    }
    mAddress = address;
    mBlockId = blockId;
    mUserId = userId;
    mWrittenBytes = 0;
    mOpen = true;
  }

  @Override
  public void close() {
    if (mOpen) {
      mOpen = false;
    }
  }

  @Override
  public void write(byte[] bytes, int offset, int length) throws IOException {
    SingleResponseListener listener = new SingleResponseListener();
    try {
      // TODO: keep connection open across multiple write calls.
      ChannelFuture f = mClientBootstrap.connect(mAddress).sync();

      LOG.info("Connected to remote machine " + mAddress);
      Channel channel = f.channel();
      mHandler.addListener(listener);
      channel.writeAndFlush(new RPCBlockWriteRequest(mUserId, mBlockId, mWrittenBytes, length,
          new DataByteArrayChannel(bytes, offset, length)));

      RPCResponse response = listener.get(TIMEOUT_SECOND, TimeUnit.SECONDS);
      channel.close().sync();

      if (response.getType() == RPCMessage.Type.RPC_BLOCK_WRITE_RESPONSE) {
        RPCBlockWriteResponse resp = (RPCBlockWriteResponse) response;
        LOG.info("response: ({}, {}, {}, {}) from remote machine {} received", resp.getUserId(),
            resp.getBlockId(), resp.getOffset(), resp.getLength(), mAddress);

        if (resp.getBlockId() < 0) {
          throw new IOException("error writing blockId: " + mBlockId + ", userId: " + mUserId
              + ", address: " + mAddress);
        }
        mWrittenBytes += length;
      } else {
        LOG.error("Unexpected response message type: " + response.getType() + " (expected: "
            + RPCMessage.Type.RPC_BLOCK_WRITE_RESPONSE + ")");
      }
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    } finally {
      mHandler.removeListener(listener);
    }
  }

  private Bootstrap createClientBootstrap() {
    final Bootstrap boot = new Bootstrap();

    boot.group(WORKER_GROUP).channel(CLIENT_CHANNEL_CLASS);
    boot.option(ChannelOption.SO_KEEPALIVE, true);
    boot.option(ChannelOption.TCP_NODELAY, true);
    boot.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

    boot.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast(RPCMessage.createFrameDecoder());
        pipeline.addLast(ENCODER);
        pipeline.addLast(DECODER);
        pipeline.addLast(mHandler);
      }
    });

    return boot;
  }
}
