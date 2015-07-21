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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.network.ChannelType;
import tachyon.network.NettyUtils;
import tachyon.network.protocol.RPCMessage;
import tachyon.network.protocol.RPCMessageDecoder;
import tachyon.network.protocol.RPCMessageEncoder;

/**
 * Shared configuration and methods for the Netty client.
 */
public final class NettyClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

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

  // The maximum number of milliseconds to wait for a response from the server.
  public static final long TIMEOUT_MS =
      TACHYON_CONF.getInt(Constants.USER_NETTY_TIMEOUT_MS, 3000);

  /**
   * Creates and returns a new Netty client bootstrap for clients to connect to remote servers.
   *
   * @param handler the handler that should be added to new channel pipelines
   * @return the new client {@link Bootstrap}
   */
  public static Bootstrap createClientBootstrap(final ClientHandler handler) {
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
        pipeline.addLast(handler);
      }
    });

    return boot;
  }
}
