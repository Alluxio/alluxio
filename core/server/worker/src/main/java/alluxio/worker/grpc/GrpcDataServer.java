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

package alluxio.worker.grpc;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.network.ChannelType;
import alluxio.util.network.NettyUtils;
import alluxio.worker.DataServer;
import alluxio.worker.WorkerProcess;

import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollMode;
import io.netty.channel.unix.DomainSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Runs a gRPC data server that responds to block requests.
 */
@NotThreadSafe
public final class GrpcDataServer implements DataServer {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcDataServer.class);

  private final SocketAddress mSocketAddress;
  private final long mTimeoutMs =
      ServerConfiguration.getMs(PropertyKey.WORKER_NETWORK_SHUTDOWN_TIMEOUT);
  private final long mKeepAliveTimeMs =
      ServerConfiguration.getMs(PropertyKey.WORKER_NETWORK_KEEPALIVE_TIME_MS);
  private final long mKeepAliveTimeoutMs =
      ServerConfiguration.getMs(PropertyKey.WORKER_NETWORK_KEEPALIVE_TIMEOUT_MS);

  private final long mFlowControlWindow =
      ServerConfiguration.getBytes(PropertyKey.WORKER_NETWORK_FLOWCONTROL_WINDOW);
  private final long mQuietPeriodMs =
      ServerConfiguration.getMs(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD);

  private EventLoopGroup mBossGroup;
  private EventLoopGroup mWorkerGroup;
  private GrpcServer mServer;
  /** non-null when the server is used with domain socket address.  */
  private DomainSocketAddress mDomainSocketAddress = null;

  /**
   * Creates a new instance of {@link GrpcDataServer}.
   *
   * @param address the server address
   * @param workerProcess the Alluxio worker process
   */
  public GrpcDataServer(final SocketAddress address, final WorkerProcess workerProcess) {
    mSocketAddress = address;
    try {
      mServer = createServerBuilder(address, NettyUtils.getWorkerChannel(
          ServerConfiguration.global()))
          .addService(new GrpcService(new BlockWorkerImpl(workerProcess)))
          .flowControlWindow((int) mFlowControlWindow)
          .keepAliveTime(mKeepAliveTimeMs, TimeUnit.MILLISECONDS)
          .keepAliveTimeout(mKeepAliveTimeoutMs, TimeUnit.MILLISECONDS)
          .build()
          .start();
      // There is no way to query domain socket address afterwards.
      // So store the bind address if it's domain socket address.
      if (address instanceof DomainSocketAddress) {
        mDomainSocketAddress = (DomainSocketAddress) address;
      }
    } catch (IOException e) {
      LOG.error("Server failed to start on {}", address.toString(), e);
      throw new RuntimeException(e);
    }
    LOG.info("Server started, listening on {}", address.toString());
  }

  private GrpcServerBuilder createServerBuilder(SocketAddress address, ChannelType type) {
    GrpcServerBuilder builder = GrpcServerBuilder.forAddress(address, ServerConfiguration.global());
    int bossThreadCount = ServerConfiguration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BOSS_THREADS);
    // If number of worker threads is 0, Netty creates (#processors * 2) threads by default.
    int workerThreadCount =
        ServerConfiguration.getInt(PropertyKey.WORKER_NETWORK_NETTY_WORKER_THREADS);
    String dataServerEventLoopNamePrefix =
        "data-server-" + ((mSocketAddress instanceof DomainSocketAddress) ? "domain-socket" :
            "tcp-socket");
    mBossGroup = NettyUtils
        .createEventLoop(type, bossThreadCount, dataServerEventLoopNamePrefix + "-boss-%d", true);
    mWorkerGroup = NettyUtils
        .createEventLoop(type, workerThreadCount, dataServerEventLoopNamePrefix + "-worker-%d",
            true);
    Class<? extends ServerChannel> socketChannelClass = NettyUtils.getServerChannelClass(
        mSocketAddress instanceof DomainSocketAddress, ServerConfiguration.global());
    if (type == ChannelType.EPOLL) {
      builder.withChildOption(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED);
    }
    return builder
        .bossEventLoopGroup(mBossGroup)
        .workerEventLoopGroup(mWorkerGroup)
        .channelType(socketChannelClass)
        .withChildOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        // set write buffer
        // this is the default, but its recommended to set it in case of change in future netty.
        .withChildOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK,
            (int) ServerConfiguration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_WATERMARK_HIGH))
        .withChildOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK,
            (int) ServerConfiguration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_WATERMARK_LOW));
  }

  @Override
  public void close() {
    if (mServer != null) {
      boolean completed = mServer.shutdown();
      if (!completed) {
        LOG.warn("RPC Server shutdown timed out.");
      }
      completed = mBossGroup.shutdownGracefully(mQuietPeriodMs, mTimeoutMs, TimeUnit.MILLISECONDS)
          .awaitUninterruptibly(mTimeoutMs);
      if (!completed) {
        LOG.warn("Forced boss group shutdown because graceful shutdown timed out.");
      }
      completed = mWorkerGroup.shutdownGracefully(mQuietPeriodMs, mTimeoutMs, TimeUnit.MILLISECONDS)
          .awaitUninterruptibly(mTimeoutMs);
      if (!completed) {
        LOG.warn("Forced worker group shutdown because graceful shutdown timed out.");
      }
    }
  }

  @Override
  public SocketAddress getBindAddress() {
    if (mDomainSocketAddress != null) {
      return mDomainSocketAddress;
    } else {
      // Server is created with Inet address.
      int port = mServer.getBindPort();
      if (port < 0) {
        return null;
      }
      return new InetSocketAddress(port);
    }
  }

  @Override
  public boolean isClosed() {
    return !mServer.isServing();
  }

  @Override
  public void awaitTermination() {
    mServer.awaitTermination();
  }
}
