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

package tachyon.worker.netty;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.network.ChannelType;
import tachyon.util.network.NettyUtils;
import tachyon.worker.DataServer;
import tachyon.worker.block.BlockWorker;

/**
 * Runs a netty data server that responds to block requests.
 */
public final class NettyDataServer implements DataServer {
  private final ServerBootstrap mBootstrap;
  private final ChannelFuture mChannelFuture;
  private final TachyonConf mTachyonConf;
  // Use a shared handler for all pipelines.
  private final DataServerHandler mDataServerHandler;

  /**
   * Creates a new instance of {@link NettyDataServer}.
   *
   * @param address the server address
   * @param dataManager a block data manager handle
   * @param tachyonConf Tachyon configuration
   */
  public NettyDataServer(final InetSocketAddress address, final BlockWorker blockWorker,
      final TachyonConf tachyonConf) {
    mTachyonConf = Preconditions.checkNotNull(tachyonConf);
    mDataServerHandler =
        new DataServerHandler(Preconditions.checkNotNull(blockWorker), mTachyonConf);
    mBootstrap = createBootstrap().childHandler(new PipelineHandler(mDataServerHandler));

    try {
      mChannelFuture = mBootstrap.bind(address).sync();
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() throws IOException {
    int quietPeriodSecs = mTachyonConf.getInt(Constants.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD);
    int timeoutSecs = mTachyonConf.getInt(Constants.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT);
    // TODO(binfan): investigate when timeoutSecs is zero (e.g., set in integration tests), does
    // this still complete successfully.
    mChannelFuture.channel().close().awaitUninterruptibly(timeoutSecs, TimeUnit.SECONDS);
    mBootstrap.group().shutdownGracefully(quietPeriodSecs, timeoutSecs, TimeUnit.SECONDS);
    mBootstrap.childGroup().shutdownGracefully(quietPeriodSecs, timeoutSecs, TimeUnit.SECONDS);
  }

  private ServerBootstrap createBootstrap() {
    final ServerBootstrap boot =
        createBootstrapOfType(mTachyonConf.getEnum(Constants.WORKER_NETWORK_NETTY_CHANNEL,
            ChannelType.class));

    // use pooled buffers
    boot.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    boot.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

    // set write buffer
    // this is the default, but its recommended to set it in case of change in future netty.
    boot.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK,
        (int) mTachyonConf.getBytes(Constants.WORKER_NETWORK_NETTY_WATERMARK_HIGH));
    boot.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK,
        (int) mTachyonConf.getBytes(Constants.WORKER_NETWORK_NETTY_WATERMARK_LOW));

    // more buffer settings on Netty socket option, one can tune them by specifying
    // properties, e.g.:
    // tachyon.worker.network.netty.backlog=50
    // tachyon.worker.network.netty.buffer.send=64KB
    // tachyon.worker.network.netty.buffer.receive=64KB
    if (mTachyonConf.containsKey(Constants.WORKER_NETWORK_NETTY_BACKLOG)) {
      boot.option(ChannelOption.SO_BACKLOG,
          mTachyonConf.getInt(Constants.WORKER_NETWORK_NETTY_BACKLOG));
    }
    if (mTachyonConf.containsKey(Constants.WORKER_NETWORK_NETTY_BUFFER_SEND)) {
      boot.option(ChannelOption.SO_SNDBUF,
          (int) mTachyonConf.getBytes(Constants.WORKER_NETWORK_NETTY_BUFFER_SEND));
    }
    if (mTachyonConf.containsKey(Constants.WORKER_NETWORK_NETTY_BUFFER_RECEIVE)) {
      boot.option(ChannelOption.SO_RCVBUF,
          (int) mTachyonConf.getBytes(Constants.WORKER_NETWORK_NETTY_BUFFER_RECEIVE));
    }
    return boot;
  }

  @Override
  public String getBindHost() {
    // Return value of io.netty.channel.Channel.localAddress() must be down-cast into types like
    // InetSocketAddress to get detailed info such as port.
    // TODO(andrew): Change getHostName() to getHostString() when we drop java 6 support. Also,
    // update the todo in ServiceSocketBindIntegrationTest
    return ((InetSocketAddress) mChannelFuture.channel().localAddress()).getHostName();
  }

  @Override
  public int getPort() {
    // Return value of io.netty.channel.Channel.localAddress() must be down-cast into types like
    // InetSocketAddress to get detailed info such as port.
    return ((InetSocketAddress) mChannelFuture.channel().localAddress()).getPort();
  }

  @Override
  public boolean isClosed() {
    return mBootstrap.group().isShutdown();
  }

  /**
   * Creates a default {@link io.netty.bootstrap.ServerBootstrap} where the channel and groups are
   * preset.
   *
   * @param type the channel type; current channel types supported are nio and epoll
   * @return an instance of {@code ServerBootstrap}
   */
  private ServerBootstrap createBootstrapOfType(final ChannelType type) {
    final ServerBootstrap boot = new ServerBootstrap();
    final int bossThreadCount = mTachyonConf.getInt(Constants.WORKER_NETWORK_NETTY_BOSS_THREADS);
    // If number of worker threads is 0, Netty creates (#processors * 2) threads by default.
    final int workerThreadCount =
        mTachyonConf.getInt(Constants.WORKER_NETWORK_NETTY_WORKER_THREADS);
    final EventLoopGroup bossGroup =
        NettyUtils.createEventLoop(type, bossThreadCount, "data-server-boss-%d", false);
    final EventLoopGroup workerGroup =
        NettyUtils.createEventLoop(type, workerThreadCount, "data-server-worker-%d", false);

    final Class<? extends ServerChannel> socketChannelClass =
        NettyUtils.getServerChannelClass(type);
    boot.group(bossGroup, workerGroup).channel(socketChannelClass);

    return boot;
  }
}
