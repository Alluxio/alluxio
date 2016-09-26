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
import alluxio.PropertyKey;
import alluxio.network.ChannelType;
import alluxio.network.protocol.RPCMessage;
import alluxio.network.protocol.RPCMessageDecoder;
import alluxio.network.protocol.RPCMessageEncoder;
import alluxio.util.network.NettyUtils;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;

import java.util.concurrent.Callable;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Shared configuration and methods for the Netty client.
 */
@ThreadSafe
public final class NettyClient {
  /**  Share both the encoder and decoder with all the client pipelines. */
  private static final RPCMessageEncoder ENCODER = new RPCMessageEncoder();
  private static final RPCMessageDecoder DECODER = new RPCMessageDecoder();

  private static final ChannelType CHANNEL_TYPE =
      Configuration.getEnum(PropertyKey.USER_NETWORK_NETTY_CHANNEL, ChannelType.class);
  private static final Class<? extends SocketChannel> CLIENT_CHANNEL_CLASS = NettyUtils
      .getClientChannelClass(CHANNEL_TYPE);
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
   * @return the new client {@link Bootstrap}
   */
  public static Bootstrap createClientBootstrap() {
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
        // ClientHandler is not sharable.
        pipeline.addLast(new ClientHandler());
      }
    });

    return boot;
  }

  /**
   * Creates a callable which returns a new bootstrap upon called. This is needed
   * because Bootstrap#clone doesn't do deep handler deep copy.
   *
   * @return a {@link Callable} to build a fresh new bootstrap
   */
  public static Callable<Bootstrap> bootstrapBuilder() {
    return new Callable<Bootstrap>() {
      @Override
      public Bootstrap call() {
        return createClientBootstrap();
      }
    };
  }
}
