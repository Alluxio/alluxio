/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

import tachyon.conf.WorkerConf;
import tachyon.worker.BlocksLocker;
import tachyon.worker.DataServer;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

/**
 * Runs a netty server that will response to block requests.
 */
public final class NettyDataServer implements DataServer {
  private final EventExecutorGroup SYNC_GROUP =
      new DefaultEventExecutorGroup(WorkerConf.get().NETTY_DATA_PROCESS_THREADS);
  private final ServerBootstrap BOOTSTRAP;
  private final ChannelFuture CHANNEL_FUTURE;

  public NettyDataServer(final SocketAddress address, final BlocksLocker locker)
      throws InterruptedException {
    BOOTSTRAP =
        createBootstrap()
            .childHandler(new PipelineHandler(locker, SYNC_GROUP))
            .option(ChannelOption.SO_BACKLOG, 1024)
            .childOption(ChannelOption.SO_KEEPALIVE, true);

    this.CHANNEL_FUTURE = BOOTSTRAP.bind(address).sync();
  }

  @Override
  public void close() throws IOException {
    CHANNEL_FUTURE.channel().close().awaitUninterruptibly();
    BOOTSTRAP.group().shutdownGracefully();
    BOOTSTRAP.childGroup().shutdownGracefully();
    SYNC_GROUP.shutdownGracefully();
  }

  /**
   * Gets the port listening on.
   */
  @Override
  public int getPort() {
    // according to the docs, a InetSocketAddress is returned and the user must down-cast
    return ((InetSocketAddress) CHANNEL_FUTURE.channel().localAddress()).getPort();
  }

  @Override
  public boolean isClosed() {
    return BOOTSTRAP.group().isShutdown();
  }

  private static ServerBootstrap createBootstrap() {
    ServerBootstrap boot = new ServerBootstrap();
    boot = setupGroups(boot, WorkerConf.get().NETTY_CHANNEL_TYPE);

    // use pooled buffers
    boot.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    boot.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    return boot;
  }

  /**
   * Creates a default {@link io.netty.bootstrap.ServerBootstrap} where the channel and
   * groups are preset. Current channel type supported are nio and epoll.
   */
  private static ServerBootstrap setupGroups(final ServerBootstrap boot, final ChannelType type) {
    ThreadFactory workerFactory = createThreadFactory("data-server-%d");
    EventLoopGroup bossGroup, workerGroup;
    switch (type) {
    case EPOLL:
      bossGroup = workerGroup = new EpollEventLoopGroup(0, workerFactory);
      boot.channel(EpollServerSocketChannel.class);
      break;
    default:
      bossGroup = workerGroup = new NioEventLoopGroup(0, workerFactory);
      boot.channel(NioServerSocketChannel.class);
    }
    boot.group(bossGroup, workerGroup);
    return boot;
  }

  private static ThreadFactory createThreadFactory(final String nameFormat) {
    return new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
  }
}
