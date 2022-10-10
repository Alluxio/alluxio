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

import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.executor.ExecutorServiceBuilder;
import alluxio.grpc.GrpcSerializationUtils;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.master.AlluxioExecutorService;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
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
  private static final long SHUTDOWN_TIMEOUT =
      Configuration.getMs(PropertyKey.WORKER_NETWORK_SHUTDOWN_TIMEOUT);
  private static final long KEEPALIVE_TIME_MS =
      Configuration.getMs(PropertyKey.WORKER_NETWORK_KEEPALIVE_TIME_MS);
  private static final long KEEPALIVE_TIMEOUT_MS =
      Configuration.getMs(PropertyKey.WORKER_NETWORK_KEEPALIVE_TIMEOUT_MS);
  private static final long PERMIT_KEEPALIVE_TIME_MS =
      Configuration.getMs(PropertyKey.WORKER_NETWORK_PERMIT_KEEPALIVE_TIME_MS);
  private static final long FLOWCONTROL_WINDOW =
      Configuration.getBytes(PropertyKey.WORKER_NETWORK_FLOWCONTROL_WINDOW);
  private static final long MAX_INBOUND_MESSAGE_SIZE =
      Configuration.getBytes(PropertyKey.WORKER_NETWORK_MAX_INBOUND_MESSAGE_SIZE);
  private static final long SHUTDOWN_QUIET_PERIOD =
      Configuration.getMs(PropertyKey.WORKER_NETWORK_NETTY_SHUTDOWN_QUIET_PERIOD);

  private final SocketAddress mSocketAddress;
  private EventLoopGroup mBossGroup;
  private EventLoopGroup mWorkerGroup;
  private final GrpcServer mServer;
  /** non-null when the server is used with domain socket address.  */
  private DomainSocketAddress mDomainSocketAddress = null;

  private AlluxioExecutorService mRPCExecutor = null;

  private final FileSystemContext mFsContext =
      FileSystemContext.create(Configuration.global());

  // TODO(Tony Sun): Moving injector from builder to here seems not good. But I can't find a better design.
  private ReadOnlyModeCheckerInjector mROMCheckerInjector;

  /**
   * Creates a new instance of {@link GrpcDataServer}.
   *
   * @param hostName the server host name
   * @param bindAddress the server bind address
   * @param workerProcess the Alluxio worker process
   */
  public GrpcDataServer(final String hostName, final SocketAddress bindAddress,
      final WorkerProcess workerProcess) {
    mSocketAddress = bindAddress;
    try {
      // There is no way to query domain socket address afterwards.
      // So store the bind address if it's domain socket address.
      if (bindAddress instanceof DomainSocketAddress) {
        mDomainSocketAddress = (DomainSocketAddress) bindAddress;
      }
      BlockWorkerClientServiceHandler blockWorkerService =
          new BlockWorkerClientServiceHandler(
              workerProcess, mDomainSocketAddress != null);
      mROMCheckerInjector = new ReadOnlyModeCheckerInjector(blockWorkerService);
      mServer = createServerBuilder(hostName, bindAddress, NettyUtils.getWorkerChannel(
          Configuration.global()))
          .addService(ServiceType.BLOCK_WORKER_CLIENT_SERVICE, new GrpcService(
              GrpcSerializationUtils.overrideMethods(blockWorkerService.bindService(),
                  blockWorkerService.getOverriddenMethodDescriptors())
          ))
          // Insert ROM injector into service.
//          .intercept(mROMCheckerInjector)
          .flowControlWindow((int) FLOWCONTROL_WINDOW)
          .keepAliveTime(KEEPALIVE_TIME_MS, TimeUnit.MILLISECONDS)
          .keepAliveTimeout(KEEPALIVE_TIMEOUT_MS, TimeUnit.MILLISECONDS)
          .permitKeepAlive(PERMIT_KEEPALIVE_TIME_MS, TimeUnit.MILLISECONDS)
          .maxInboundMessageSize((int) MAX_INBOUND_MESSAGE_SIZE)
          .build()
          .start();
    } catch (IOException e) {
      String message =
          String.format("Alluxio worker gRPC server failed to start on %s", bindAddress.toString());
      LOG.error(message, e);
      throw new RuntimeException(message, e);
    }
    LOG.info("Alluxio worker gRPC server started, listening on {}", bindAddress.toString());
  }

  private GrpcServerBuilder createServerBuilder(String hostName,
      SocketAddress bindAddress, ChannelType type) {
    // Create an executor for Worker RPC server.
    mRPCExecutor = ExecutorServiceBuilder.buildExecutorService(
            ExecutorServiceBuilder.RpcExecutorHost.WORKER);
    MetricsSystem.registerGaugeIfAbsent(MetricKey.WORKER_RPC_QUEUE_LENGTH.getName(),
            mRPCExecutor::getRpcQueueLength);
    // Create underlying gRPC server.
    GrpcServerBuilder builder = GrpcServerBuilder
        .forAddress(GrpcServerAddress.create(hostName, bindAddress),
            Configuration.global())
        .executor(mRPCExecutor);
    int bossThreadCount = Configuration.getInt(PropertyKey.WORKER_NETWORK_NETTY_BOSS_THREADS);

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
        mSocketAddress instanceof DomainSocketAddress, Configuration.global());
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
            (int) Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_WATERMARK_HIGH))
        .withChildOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK,
            (int) Configuration.getBytes(PropertyKey.WORKER_NETWORK_NETTY_WATERMARK_LOW));
  }

  @Override
  public void close() throws IOException {
    mFsContext.close();
    if (mServer != null) {
      LOG.info("Shutting down Alluxio worker gRPC server at {}.", getBindAddress());
      boolean completed = mServer.shutdown();
      if (!completed) {
        LOG.warn("Alluxio worker gRPC server shutdown timed out.");
      }
      completed = mBossGroup
          .shutdownGracefully(SHUTDOWN_QUIET_PERIOD, SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)
          .awaitUninterruptibly(SHUTDOWN_TIMEOUT);
      if (!completed) {
        LOG.warn("Forced boss group shutdown because graceful shutdown timed out.");
      }
      completed = mWorkerGroup
          .shutdownGracefully(SHUTDOWN_QUIET_PERIOD, SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)
          .awaitUninterruptibly(SHUTDOWN_TIMEOUT);
      if (!completed) {
        LOG.warn("Forced worker group shutdown because graceful shutdown timed out.");
      }
    }
    if (mRPCExecutor != null) {
      mRPCExecutor.shutdownNow();
      try {
        mRPCExecutor.awaitTermination(
            Configuration.getMs(PropertyKey.NETWORK_CONNECTION_SERVER_SHUTDOWN_TIMEOUT),
            TimeUnit.MILLISECONDS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
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
