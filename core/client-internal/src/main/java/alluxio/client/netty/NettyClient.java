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

package alluxio.client.netty;

import javax.annotation.concurrent.ThreadSafe;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

import alluxio.Constants;
import alluxio.client.ClientContext;
import alluxio.Configuration;
import alluxio.network.ChannelType;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCMessageDecoder;
import alluxio.network.protocol.RPCMessageEncoder;
import alluxio.util.network.NettyUtils;

/**
 * Shared configuration and methods for the Netty client.
 */
@ThreadSafe
public final class NettyClient {
  /**  Share both the encoder and decoder with all the client pipelines. */
  private static final RPCMessageEncoder ENCODER = new RPCMessageEncoder();
  private static final RPCMessageDecoder DECODER = new RPCMessageDecoder();

  private static final Configuration CONF = ClientContext.getConf();
  private static final ChannelType CHANNEL_TYPE =
      CONF.getEnum(Constants.USER_NETWORK_NETTY_CHANNEL, ChannelType.class);
  private static final Class<? extends SocketChannel> CLIENT_CHANNEL_CLASS = NettyUtils
      .getClientChannelClass(CHANNEL_TYPE);
  /**
   * Reuse {@link EventLoopGroup} for all clients. Use daemon threads so the JVM is allowed to
   * shutdown even when daemon threads are alive. If number of worker threads is 0, Netty creates
   * (#processors * 2) threads by default.
   */
  private static final EventLoopGroup WORKER_GROUP = NettyUtils.createEventLoop(CHANNEL_TYPE,
      CONF.getInt(Constants.USER_NETWORK_NETTY_WORKER_THREADS), "netty-client-worker-%d",
      true);

  /** The maximum number of milliseconds to wait for a response from the server. */
  public static final long TIMEOUT_MS =
      CONF.getInt(Constants.USER_NETWORK_NETTY_TIMEOUT_MS);

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
