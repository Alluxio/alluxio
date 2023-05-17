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

package alluxio.master.service.snapshotrpc.rpc;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.grpc.ErrorType;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.grpc.ServiceType;
import alluxio.master.AlluxioExecutorService;
import alluxio.master.AlluxioMasterProcess;
import alluxio.master.Master;
import alluxio.master.MasterRegistry;
import alluxio.master.service.SimpleService;

import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

/**
 * Created by {@link Factory}.
 * Manages the behavior of the master's snapshot rpc service.
 */
public abstract class SnapshotRpcServerService implements SimpleService {
  protected static final Logger LOG = LoggerFactory.getLogger(SnapshotRpcServerService.class);

  protected final InetSocketAddress mBindAddress;
  protected final AlluxioMasterProcess mMasterProcess;
  protected final MasterRegistry mMasterRegistry;

  /**
   * The grpc server and its executor service ({@link #mRpcExecutor}) need to be managed
   * independently (i.e. stopping the grpc server will not automatically stop the rpc executor)
   */
  @Nullable @GuardedBy("this")
  protected GrpcServer mGrpcServer = null;
  @Nullable @GuardedBy("this")
  protected AlluxioExecutorService mRpcExecutor = null;

  protected SnapshotRpcServerService(InetSocketAddress bindAddress,
      AlluxioMasterProcess masterProcess,
      MasterRegistry masterRegistry) {
    mBindAddress = bindAddress;
    mMasterRegistry = masterRegistry;
    mMasterProcess = masterProcess;
  }

  protected synchronized void startGrpcServer(
      Function<Master, Map<ServiceType, GrpcService>> serviceProvider) {
    GrpcServerBuilder builder = mMasterProcess.createBaseSnapshotRpcServer();
    Optional<AlluxioExecutorService> executorService =
        mMasterProcess.createSnapshotRpcExecutorService();
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
    } catch (IOException e) {
      throw new AlluxioRuntimeException(Status.INTERNAL, "Failed to start snapshot gRPC server", e,
          ErrorType.Internal, false);
    }
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

  /**
   * Factory to create an {@link SnapshotRpcServerService}.
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
    public static SnapshotRpcServerService create(
        InetSocketAddress bindAddress,
        AlluxioMasterProcess masterProcess,
        MasterRegistry masterRegistry) {
      return new AlwaysOnSnapshotRpcService(bindAddress, masterProcess, masterRegistry);
    }
  }
}
