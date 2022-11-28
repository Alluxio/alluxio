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

package alluxio.master.service.rpc;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.grpc.ErrorType;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.master.AlluxioExecutorService;
import alluxio.master.MasterProcess;
import alluxio.master.MasterRegistry;
import alluxio.master.SafeModeManager;
import alluxio.master.service.SimpleService;
import alluxio.network.RejectingServer;

import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Manages the behavior of the master's simple service.
 */
public class RpcServerSimpleService implements SimpleService {
  private static final Logger LOG = LoggerFactory.getLogger(RpcServerSimpleService.class);

  protected final InetSocketAddress mBindAddress;
  protected final MasterProcess mMasterProcess;
  protected final MasterRegistry mMasterRegistry;

  /**
   * The grpc server and its executor service ({@link #mRpcExecutor}) need to be managed
   * independently (i.e. stopping the grpc server will not automatically stop the rpc executor)
   */
  @Nullable @GuardedBy("this")
  protected GrpcServer mGrpcServer = null;
  @Nullable @GuardedBy("this")
  protected AlluxioExecutorService mRpcExecutor = null;
  @Nullable @GuardedBy("this")
  protected RejectingServer mRejectingGrpcServer = null;

  protected RpcServerSimpleService(InetSocketAddress bindAddress, MasterProcess masterProcess,
      MasterRegistry masterRegistry) {
    mBindAddress = bindAddress;
    mMasterRegistry = masterRegistry;
    mMasterProcess = masterProcess;
  }

  /**
   * @return whether the grpc server is serving or not
   */
  public synchronized boolean isServing() {
    return mGrpcServer != null && mGrpcServer.isServing();
  }

  @Override
  public synchronized void start() {
    startRejectingServer();
  }

  @Override
  public synchronized void promote() {
    stopRejectingServer();
    GrpcServerBuilder builder = mMasterProcess.createBaseRpcServer();
    mMasterProcess.createRpcExecutorService().ifPresent(executor -> {
      builder.executor(executor);
      mRpcExecutor = executor;
    });
    mMasterRegistry.getServers().forEach(master -> {
      master.getServices().forEach((type, service) -> {
        builder.addService(type, service);
        LOG.info("registered service {}", type.name());
      });
    });
    mGrpcServer = builder.build();
    try {
      mGrpcServer.start();
      mMasterProcess.getSafeModeManager().ifPresent(SafeModeManager::notifyRpcServerStarted);
    } catch (IOException e) {
      throw new AlluxioRuntimeException(Status.INTERNAL, "Failed to start gRPC server", e,
          ErrorType.Internal, false);
    }
  }

  @Override
  public synchronized void demote() {
    stopGrpcServer();
    stopRpcExecutor();
    startRejectingServer();
  }

  @Override
  public synchronized void stop() {
    stopRejectingServer();
    stopGrpcServer();
    stopRpcExecutor();
  }

  protected void stopGrpcServer() {
    if (mGrpcServer != null) {
      mGrpcServer.shutdown();
      mGrpcServer.awaitTermination();
    }
  }

  protected void stopRpcExecutor() {
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

  protected void startRejectingServer() {
    mRejectingGrpcServer = new RejectingServer(mBindAddress);
    mRejectingGrpcServer.start();
  }

  protected void stopRejectingServer() {
    if (mRejectingGrpcServer != null) {
      mRejectingGrpcServer.stopAndJoin();
      mRejectingGrpcServer = null;
    }
  }

  /**
   * Factory to create an {@link RpcServerSimpleService}.
   */
  public static class Factory {
    /**
     * Creates a simple service wrapper around a grpc server to manager the grpc server for the
     * master process.
     * @param masterProcess the master process that drives the rpc server
     * @param masterRegistry where the grpc services will be drawn from
     * @param bindAddress the address where the rpc server will bind
     * @return a simple service that manages the behavior of the rpc server
     */
    public static RpcServerSimpleService create(
        InetSocketAddress bindAddress,
        MasterProcess masterProcess,
        MasterRegistry masterRegistry) {
      return new RpcServerSimpleService(bindAddress, masterProcess, masterRegistry);
    }
  }
}
