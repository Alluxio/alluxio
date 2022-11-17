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

package alluxio.master.service;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.executor.ExecutorServiceBuilder;
import alluxio.grpc.ErrorType;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.master.AlluxioExecutorService;
import alluxio.master.MasterRegistry;
import alluxio.master.SafeModeManager;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.RejectingServer;

import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Manages the behavior of the master's simple service.
 */
public class RpcServerSimpleService implements SimpleService {
  private static final Logger LOG = LoggerFactory.getLogger(RpcServerSimpleService.class);

  private final MasterRegistry mMasterRegistry;
  private final InetSocketAddress mConnectAddress;
  private final InetSocketAddress mBindAddress;
  private final SafeModeManager mSafeModeManager;

  @Nullable
  @GuardedBy("this")
  private GrpcServer mGrpcServer = null;
  @Nullable @GuardedBy("this")
  private AlluxioExecutorService mRpcExecutor = null;
  @Nullable @GuardedBy("this")
  private RejectingServer mRejectingGrpcServer = null;

  private RpcServerSimpleService(MasterRegistry masterRegistry, InetSocketAddress connectAddress,
      InetSocketAddress bindAddress, SafeModeManager safeModeManager) {
    mMasterRegistry = masterRegistry;
    mConnectAddress = connectAddress;
    mBindAddress = bindAddress;
    mSafeModeManager = safeModeManager;
  }

  @Override
  public synchronized void start() {
    mRejectingGrpcServer = new RejectingServer(mBindAddress);
    mRejectingGrpcServer.start();
  }

  @Override
  public synchronized void promote() {
    stopRejectingServer();
    mRpcExecutor = createRpcExecutor();
    MetricsSystem.removeMetrics(MetricKey.MASTER_RPC_QUEUE_LENGTH.getName());
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_RPC_QUEUE_LENGTH.getName(),
        mRpcExecutor::getRpcQueueLength);
    MetricsSystem.removeMetrics(MetricKey.MASTER_RPC_THREAD_ACTIVE_COUNT.getName());
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_RPC_THREAD_ACTIVE_COUNT.getName(),
        mRpcExecutor::getActiveCount);
    MetricsSystem.removeMetrics(MetricKey.MASTER_RPC_THREAD_CURRENT_COUNT.getName());
    MetricsSystem.registerGaugeIfAbsent(MetricKey.MASTER_RPC_THREAD_CURRENT_COUNT.getName(),
        mRpcExecutor::getPoolSize);
    mGrpcServer = createRpcServer(mRpcExecutor);
    try {
      mGrpcServer.start();
    } catch (IOException e) {
      throw new AlluxioRuntimeException(Status.INTERNAL, "Failed to start gRPC server", e,
          ErrorType.Internal, false);
    }
    mSafeModeManager.notifyRpcServerStarted();
  }

  @Override
  public synchronized void demote() {
    stopGrpcServer();
    stopRpcExecutor();
    start(); // start rejecting server again
  }

  @Override
  public synchronized void stop() {
    stopRejectingServer();
    stopGrpcServer();
    stopRpcExecutor();
  }

  private void stopGrpcServer() {
    if (mGrpcServer != null) {
      mGrpcServer.shutdown();
      mGrpcServer.awaitTermination();
    }
  }

  private void stopRpcExecutor() {
    if (mRpcExecutor != null) {
      mRpcExecutor.shutdown();
      try {
        mRpcExecutor.awaitTermination(
            Configuration.getMs(PropertyKey.NETWORK_CONNECTION_SERVER_SHUTDOWN_TIMEOUT),
            TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.warn("rpc executor was interrupted while terminating", e);
      }
    }
  }

  private void stopRejectingServer() {
    if (mRejectingGrpcServer != null) {
      mRejectingGrpcServer.stopAndJoin();
      mRejectingGrpcServer = null;
    }
  }

  private AlluxioExecutorService createRpcExecutor() {
    return ExecutorServiceBuilder.buildExecutorService(
        ExecutorServiceBuilder.RpcExecutorHost.MASTER);
  }

  private GrpcServer createRpcServer(Executor rpcExecutor) {
    GrpcServerBuilder builder = GrpcServerBuilder
        .forAddress(GrpcServerAddress.create(mConnectAddress.getHostName(), mBindAddress),
            Configuration.global())
        .executor(rpcExecutor)
        .flowControlWindow(
            (int) Configuration.getBytes(PropertyKey.MASTER_NETWORK_FLOWCONTROL_WINDOW))
        .keepAliveTime(
            Configuration.getMs(PropertyKey.MASTER_NETWORK_KEEPALIVE_TIME_MS),
            TimeUnit.MILLISECONDS)
        .keepAliveTimeout(
            Configuration.getMs(PropertyKey.MASTER_NETWORK_KEEPALIVE_TIMEOUT_MS),
            TimeUnit.MILLISECONDS)
        .permitKeepAlive(
            Configuration.getMs(PropertyKey.MASTER_NETWORK_PERMIT_KEEPALIVE_TIME_MS),
            TimeUnit.MILLISECONDS)
        .maxInboundMessageSize((int) Configuration.getBytes(
            PropertyKey.MASTER_NETWORK_MAX_INBOUND_MESSAGE_SIZE));
    // register services
    mMasterRegistry.getServers().forEach(master -> {
      master.getServices().forEach((type, service) -> {
        builder.addService(type, service);
        LOG.info("registered service {}", type.name());
      });
    });
    return builder.build();
  }

  /**
   * Factory to create an {@link RpcServerSimpleService}.
   */
  public static class Factory {
    /**
     * Creates a simple service wrapper around a grpc server to manager the grpc server for the
     * master process.
     * @param masterRegistry where the grpc services will be drawn from
     * @param connectAddress the address where the rpc server will connect
     * @param bindAddress the address where the rpc server will bind
     * @param safeModeManager the safe mode manager
     */
    public static RpcServerSimpleService create(
        MasterRegistry masterRegistry,
        InetSocketAddress connectAddress,
        InetSocketAddress bindAddress,
        SafeModeManager safeModeManager) {
      return new RpcServerSimpleService(masterRegistry, connectAddress, bindAddress,
          safeModeManager);
    }
  }
}
