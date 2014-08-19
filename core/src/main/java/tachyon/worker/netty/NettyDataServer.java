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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ThreadFactory;

import tachyon.conf.WorkerConf;
import tachyon.worker.BlocksLocker;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

public final class NettyDataServer implements Closeable {
  // private final EventExecutorGroup SYNC_GROUP = new DefaultEventExecutorGroup(16);
  private final ServerBootstrap BOOTSTRAP;
  private final ChannelFuture CHANNEL_FUTURE;

  public NettyDataServer(final SocketAddress address, final BlocksLocker locker)
      throws InterruptedException {
    ChannelInitializer<SocketChannel> childHandler = new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("blockRequestDecoder", new BlockRequest.Decoder());
        pipeline.addLast("blockRequestEncoder", new BlockResponse.Encoder());
        pipeline.addLast("nioChunckedWriter", new ChunkedWriteHandler());
        // TODO move out of worker group
        // pipeline.addLast(SYNC_GROUP, "dataServerHandler", new DataServerHandler(locker));
        pipeline.addLast("dataServerHandler", new DataServerHandler(locker));
      }
    };

    BOOTSTRAP =
        createBootstrap().handler(new LoggingHandler(LogLevel.DEBUG)).childHandler(childHandler)
            .option(ChannelOption.SO_BACKLOG, 1024).childOption(ChannelOption.SO_KEEPALIVE, true);

    this.CHANNEL_FUTURE = BOOTSTRAP.bind(address).sync();
  }

  private static ServerBootstrap createBootstrap() {
    ServerBootstrap boot = new ServerBootstrap();
    boot = setupGroups(boot);

    // use pooled buffers
    // wait for perf test results before commenting back in
    // boot.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    // boot.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    return boot;
  }

  private static ServerBootstrap setupGroups(final ServerBootstrap boot) {
    ThreadFactory bossFactory =
        new ThreadFactoryBuilder().setNameFormat("data-server-boss-%d").build();
    ThreadFactory workerFactory =
        new ThreadFactoryBuilder().setNameFormat("data-server-worker-%d").build();
    // one thread to accept connections, 2 * num_cores for workers
    if (WorkerConf.get().NETTY_USER_EPOLL) {
      EpollEventLoopGroup bossGroup = new EpollEventLoopGroup(1, bossFactory);
      EpollEventLoopGroup workerGroup = new EpollEventLoopGroup(0, workerFactory);
      boot.group(bossGroup, workerGroup);
      boot.channel(EpollServerSocketChannel.class);
    } else {
      NioEventLoopGroup bossGroup = new NioEventLoopGroup(1, bossFactory);
      NioEventLoopGroup workerGroup = new NioEventLoopGroup(0, workerFactory);
      boot.group(bossGroup, workerGroup);
      boot.channel(NioServerSocketChannel.class);
    }
    return boot;
  }

  @Override
  public void close() throws IOException {
    CHANNEL_FUTURE.channel().close().awaitUninterruptibly();
    BOOTSTRAP.group().shutdownGracefully();
    BOOTSTRAP.childGroup().shutdownGracefully();
  }

  /**
   * Gets the port listening on.
   */
  public int getPort() {
    // according to the docs, a InetSocketAddress is returned and the user must down-cast
    return ((InetSocketAddress) CHANNEL_FUTURE.channel().localAddress()).getPort();
  }

  public boolean isClosed() {
    return BOOTSTRAP.group().isShutdown();
  }
}
