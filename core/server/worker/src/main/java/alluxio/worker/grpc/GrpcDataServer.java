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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.network.ChannelType;
import alluxio.util.executor.ExecutorServiceFactories;
import alluxio.util.network.NettyUtils;
import alluxio.worker.DataServer;
import alluxio.worker.WorkerProcess;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.unix.DomainSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
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
      Configuration.getMs(PropertyKey.WORKER_NETWORK_SHUTDOWN_TIMEOUT);
  private final long mFlowControlWindow =
      Configuration.getBytes(PropertyKey.WORKER_NETWORK_FLOWCONTROL_WINDOW);
  private final long mQuietPeriodMs =
      Configuration.getMs(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD);

  private EventLoopGroup mBossGroup;
  private EventLoopGroup mWorkerGroup;
  private GrpcServer mServer;

  /**
   * Creates a new instance of {@link GrpcDataServer}.
   *
   * @param address the server address
   * @param workerProcess the Alluxio worker process
   */
  public GrpcDataServer(final SocketAddress address, final WorkerProcess workerProcess) {
    mSocketAddress = address;
    try {
      mServer = createServerBuilder(address, NettyUtils.WORKER_CHANNEL_TYPE)
          .addService(new GrpcService(new BlockWorkerImpl(workerProcess)))
          .flowControlWindow((int) mFlowControlWindow)
          .build()
          .start();
    } catch (IOException e) {
      LOG.error("Server failed to start on {}", address.toString(), e);
      throw new RuntimeException(e);
    }
    LOG.info("Server started, listening on {}", address.toString());
  }

  private GrpcServerBuilder createServerBuilder(SocketAddress address, ChannelType type) {
    GrpcServerBuilder builder = GrpcServerBuilder.forAddress(address);
    int bossThreadCount = Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BOSS_THREADS);
    // If number of worker threads is 0, Netty creates (#processors * 2) threads by default.
    int workerThreadCount =
        Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_WORKER_THREADS);
    String dataServerEventLoopNamePrefix =
        "data-server-" + ((mSocketAddress instanceof DomainSocketAddress) ? "domain-socket" :
            "tcp-socket");
    mBossGroup = NettyUtils
        .createEventLoop(type, bossThreadCount, dataServerEventLoopNamePrefix + "-boss-%d", true);
    mWorkerGroup = NettyUtils
        .createEventLoop(type, workerThreadCount, dataServerEventLoopNamePrefix + "-worker-%d",
            true);
    Class<? extends ServerChannel> socketChannelClass = NettyUtils.getServerChannelClass(
        mSocketAddress instanceof DomainSocketAddress);
    return builder
        .bossEventLoopGroup(mBossGroup)
        .workerEventLoopGroup(mWorkerGroup)
        .channelType(socketChannelClass);
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
    int port = mServer.getBindPort();
    if (port < 0) {
      return null;
    }
    return new InetSocketAddress(port);
  }

  @Override
  public boolean isClosed() {
    return !mServer.isServing();
  }
}
