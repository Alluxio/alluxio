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

package alluxio.client.netty;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.network.ChannelType;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCMessageDecoder;
import alluxio.network.protocol.RPCMessageEncoder;
import alluxio.util.network.NettyUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Shared configuration and methods for the Netty client.
 */
@ThreadSafe
public final class NettyClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**  Share both the encoder and decoder with all the client pipelines. */
  private static final RPCMessageEncoder ENCODER = new RPCMessageEncoder();
  private static final RPCMessageDecoder DECODER = new RPCMessageDecoder();
  private static final boolean PACKET_STREAMING_ENABLED =
      Configuration.getBoolean(PropertyKey.USER_PACKET_STREAMING_ENABLED);

  private static final ChannelType CHANNEL_TYPE = getChannelType();
  /**
   * Reuse {@link EventLoopGroup} for all clients. Use daemon threads so the JVM is allowed to
   * shutdown even when daemon threads are alive. If number of worker threads is 0, Netty creates
   * (#processors * 2) threads by default.
   */
  private static final EventLoopGroup WORKER_GROUP = NettyUtils.createEventLoop(CHANNEL_TYPE,
      Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_WORKER_THREADS), "netty-client-worker-%d",
      true);

  /** The maximum number of milliseconds to wait for a response from the server. */
  public static final long TIMEOUT_MS =
      Configuration.getInt(PropertyKey.USER_NETWORK_NETTY_TIMEOUT_MS);

  private NettyClient() {} // prevent instantiation

  /**
   * Creates and returns a new Netty client bootstrap for clients to connect to remote servers.
   *
   * @param address the socket address
   * @return the new client {@link Bootstrap}
   */
  public static Bootstrap createClientBootstrap(SocketAddress address) {
    final Bootstrap boot = new Bootstrap();

    boot.group(WORKER_GROUP).channel(
        NettyUtils.getClientChannelClass(CHANNEL_TYPE, !(address instanceof InetSocketAddress)));
    boot.option(ChannelOption.SO_KEEPALIVE, true);
    boot.option(ChannelOption.TCP_NODELAY, true);
    boot.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    if (PACKET_STREAMING_ENABLED && CHANNEL_TYPE == ChannelType.EPOLL) {
      boot.option(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
    }

    boot.handler(new ChannelInitializer<Channel>() {
      @Override
      public void initChannel(Channel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast(RPCMessage.createFrameDecoder());
        pipeline.addLast(ENCODER);
        pipeline.addLast(DECODER);
      }
    });

    return boot;
  }

  /**
   * @return true if the domain socket is enabled on this client
   */
  public static boolean isDomainSocketEnabled() {
    return getChannelType() == ChannelType.EPOLL;
  }

  /**
   * Note: Packet streaming requires {@link io.netty.channel.epoll.EpollMode} to be set to
   * LEVEL_TRIGGERED which is not supported in netty versions < 4.0.26.Final. Without shading
   * netty in Alluxio, we cannot use epoll.
   *
   * @return {@link ChannelType} to use
   */
  private static ChannelType getChannelType() {
    if (PACKET_STREAMING_ENABLED) {
      try {
        EpollChannelOption.class.getField("EPOLL_MODE");
      } catch (Throwable e) {
        LOG.warn("EPOLL_MODE is not supported in netty with version < 4.0.26.Final.");
        return ChannelType.NIO;
      }
    }
    return Configuration.getEnum(PropertyKey.USER_NETWORK_NETTY_CHANNEL, ChannelType.class);
  }
}
