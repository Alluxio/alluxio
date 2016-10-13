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

package alluxio.worker.netty;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.network.ChannelType;
import alluxio.util.network.NettyUtils;
import alluxio.worker.AlluxioWorkerService;
import alluxio.worker.DataServer;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Runs a netty data server that responds to block requests.
 */
@NotThreadSafe
public final class NettyDataServer implements DataServer {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final ServerBootstrap mBootstrap;
  private final ChannelFuture mChannelFuture;
  // Use a shared handler for all pipelines.
  private final DataServerHandler mDataServerHandler;

  /**
   * Creates a new instance of {@link NettyDataServer}.
   *
   * @param address the server address
   * @param worker the Alluxio worker which contains the appropriate components to handle data
   *               operations
   */
  public NettyDataServer(final InetSocketAddress address, final AlluxioWorkerService worker) {
    mDataServerHandler = new DataServerHandler(Preconditions.checkNotNull(worker));
    mBootstrap = createBootstrap().childHandler(new PipelineHandler(mDataServerHandler));

    try {
      mChannelFuture = mBootstrap.bind(address).sync();
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void close() throws IOException {
    int quietPeriodSecs =
        Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD);
    int timeoutSecs = Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_TIMEOUT);

    // The following steps are needed to shut down the data server:
    //
    // 1) its channel needs to be closed
    // 2) its main EventLoopGroup needs to be shut down
    // 3) its child EventLoopGroup needs to be shut down
    //
    // Each of the above steps can time out. If 1) times out, we simply give up on closing the
    // channel. If 2) or 3) times out, the respective EventLoopGroup failed to shut down
    // gracefully and its shutdown is forced.

    boolean completed;
    completed =
        mChannelFuture.channel().close().awaitUninterruptibly(timeoutSecs, TimeUnit.SECONDS);
    if (!completed) {
      LOG.warn("Closing the channel timed out.");
    }
    completed =
        mBootstrap.group().shutdownGracefully(quietPeriodSecs, timeoutSecs, TimeUnit.SECONDS)
            .awaitUninterruptibly(timeoutSecs, TimeUnit.SECONDS);
    if (!completed) {
      LOG.warn("Forced group shutdown because graceful shutdown timed out.");
    }
    completed =
        mBootstrap.childGroup().shutdownGracefully(quietPeriodSecs, timeoutSecs, TimeUnit.SECONDS)
            .awaitUninterruptibly(timeoutSecs, TimeUnit.SECONDS);
    if (!completed) {
      LOG.warn("Forced child group shutdown because graceful shutdown timed out.");
    }
  }

  private ServerBootstrap createBootstrap() {
    final ServerBootstrap boot = createBootstrapOfType(
        Configuration.getEnum(PropertyKey.WORKER_NETWORK_NETTY_CHANNEL, ChannelType.class));

    // use pooled buffers
    boot.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
    boot.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

    // set write buffer
    // this is the default, but its recommended to set it in case of change in future netty.
    boot.childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK,
        (int) Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_WATERMARK_HIGH));
    boot.childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK,
        (int) Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_WATERMARK_LOW));

    // more buffer settings on Netty socket option, one can tune them by specifying
    // properties, e.g.:
    // alluxio.worker.network.netty.backlog=50
    // alluxio.worker.network.netty.buffer.send=64KB
    // alluxio.worker.network.netty.buffer.receive=64KB
    if (Configuration.containsKey(PropertyKey.WORKER_NETWORK_NETTY_BACKLOG)) {
      boot.option(ChannelOption.SO_BACKLOG,
          Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BACKLOG));
    }
    if (Configuration.containsKey(PropertyKey.WORKER_NETWORK_NETTY_BUFFER_SEND)) {
      boot.option(ChannelOption.SO_SNDBUF,
          (int) Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_BUFFER_SEND));
    }
    if (Configuration.containsKey(PropertyKey.WORKER_NETWORK_NETTY_BUFFER_RECEIVE)) {
      boot.option(ChannelOption.SO_RCVBUF,
          (int) Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_BUFFER_RECEIVE));
    }
    return boot;
  }

  @Override
  public String getBindHost() {
    // Return value of io.netty.channel.Channel.localAddress() must be down-cast into types like
    // InetSocketAddress to get detailed info such as port.
    return ((InetSocketAddress) mChannelFuture.channel().localAddress()).getHostString();
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
    final int bossThreadCount = Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BOSS_THREADS);
    // If number of worker threads is 0, Netty creates (#processors * 2) threads by default.
    final int workerThreadCount =
        Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_WORKER_THREADS);
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
