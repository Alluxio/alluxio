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
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.master.AlluxioExecutorService;
import alluxio.master.Master;
import alluxio.master.MasterProcess;
import alluxio.master.MasterRegistry;
import alluxio.master.SafeModeManager;
import alluxio.master.service.SimpleService;
import alluxio.network.RejectingServer;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;

import com.google.common.base.Preconditions;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Created by {@link RpcServerService.Factory}.
 * Manages the behavior of the master's rpc service. It deploys a rpc server only after being
 * promoted. It stops said rpc web server after being demoted or stopped. When a rpc server is not
 * deployed, a rejecting server is deployed instead (after the service has been started).
 */
public class RpcServerService implements SimpleService {
  protected static final Logger LOG = LoggerFactory.getLogger(RpcServerService.class);

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

  protected RpcServerService(InetSocketAddress bindAddress, MasterProcess masterProcess,
      MasterRegistry masterRegistry) {
    mBindAddress = bindAddress;
    mMasterRegistry = masterRegistry;
    mMasterProcess = masterProcess;
  }

  protected final synchronized boolean isGrpcServerServing() {
    return mGrpcServer != null && mGrpcServer.isServing();
  }

  /**
   * @return whether the grpc server is serving or not
   */
  public synchronized boolean isServing() {
    return isServingLeader() || isServingStandby();
  }

  /**
   * @return whether the grpc server is serving in leader mode
   */
  public synchronized boolean isServingLeader() {
    return isGrpcServerServing();
  }

  /**
   * @return whether the grpc server is serving in standby mode
   */
  public synchronized boolean isServingStandby() {
    return false;
  }

  @Override
  public synchronized void start() {
    LOG.info("Starting {}", this.getClass().getSimpleName());
    startRejectingServer();
  }

  @Override
  public synchronized void promote() {
    LOG.info("Promoting {}", this.getClass().getSimpleName());
    Preconditions.checkState(mGrpcServer == null, "rpc server must not be running");
    stopRejectingServer();
    waitForFree();
    startGrpcServer(Master::getServices);
  }

  protected synchronized void startGrpcServer(
      Function<Master, Map<ServiceType, GrpcService>> serviceProvider) {
    GrpcServerBuilder builder = mMasterProcess.createBaseRpcServer();
    Optional<AlluxioExecutorService> executorService = mMasterProcess.createRpcExecutorService();
    if (executorService.isPresent()) {
      builder.executor(executorService.get());
      mRpcExecutor = executorService.get();
    }
    mMasterRegistry.getServers().forEach(master -> {
      serviceProvider.apply(master).forEach((type, service) -> {
        builder.addService(type, service);
        LOG.info("registered service {}", type.name());
      });
    });
    mGrpcServer = builder.build(() -> mMasterProcess.getPrimarySelector().getStateUnsafe());
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
    LOG.info("Demoting {}", this.getClass().getSimpleName());
    stopGrpcServer();
    stopRpcExecutor();
    waitForFree();
    startRejectingServer();
  }

  @Override
  public synchronized void stop() {
    LOG.info("Stopping {}", this.getClass().getSimpleName());
    stopRejectingServer();
    stopGrpcServer();
    stopRpcExecutor();
  }

  protected synchronized void stopGrpcServer() {
    if (mGrpcServer != null) {
      mGrpcServer.shutdown();
      mGrpcServer.awaitTermination();
      mGrpcServer = null;
    }
  }

  protected synchronized void stopRpcExecutor() {
    if (mRpcExecutor != null) {
      mRpcExecutor.shutdown();
      try {
        mRpcExecutor.awaitTermination(
            Configuration.getMs(PropertyKey.NETWORK_CONNECTION_SERVER_SHUTDOWN_TIMEOUT),
            TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.warn("rpc executor was interrupted while terminating", e);
      }
      mRpcExecutor = null;
    }
  }

  protected synchronized void startRejectingServer() {
    Preconditions.checkState(mRejectingGrpcServer == null, "rejecting server must not be running");
    mRejectingGrpcServer = new RejectingServer(mBindAddress);
    mRejectingGrpcServer.start();
    waitForBound();
  }

  protected synchronized void stopRejectingServer() {
    if (mRejectingGrpcServer != null) {
      mRejectingGrpcServer.stopAndJoin();
      mRejectingGrpcServer = null;
    }
  }

  protected void waitForFree() {
    waitFor(false, mBindAddress);
  }

  protected void waitForBound() {
    waitFor(true, mBindAddress);
  }

  /**
   * Creates a buffer between rejecting server and regular serving server of at most 1 second.
   * @param freeOrBound determines if it prematurely returns when the port if free (false) or
   *                    bound (true)
   * @param address the address to test
   */
  public static void waitFor(boolean freeOrBound, InetSocketAddress address) {
    try {
      CommonUtils.waitFor("wait for the address to be " + (freeOrBound ? "bound" : "free"),
          () -> {
            try (Socket ignored = new Socket(address.getAddress(), address.getPort())) {
              return freeOrBound;
            } catch (Exception e) {
              return !freeOrBound;
            }
          }, WaitForOptions.defaults().setInterval(10).setTimeoutMs(1_000));
    } catch (Exception e) {
      // do nothing
    }
  }

  /**
   * Factory to create an {@link RpcServerService}.
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
    public static RpcServerService create(
        InetSocketAddress bindAddress,
        MasterProcess masterProcess,
        MasterRegistry masterRegistry) {
      if (Configuration.getBoolean(PropertyKey.STANDBY_MASTER_GRPC_ENABLED)) {
        return new RpcServerStandbyGrpcService(bindAddress, masterProcess, masterRegistry);
      }
      return new RpcServerService(bindAddress, masterProcess, masterRegistry);
    }
  }
}
