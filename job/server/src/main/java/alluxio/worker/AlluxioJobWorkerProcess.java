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

package alluxio.worker;

import alluxio.RuntimeConstants;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.PropertyKey;
import alluxio.conf.Configuration;
import alluxio.grpc.GrpcServer;
import alluxio.grpc.GrpcServerAddress;
import alluxio.grpc.GrpcServerBuilder;
import alluxio.grpc.GrpcService;
import alluxio.security.user.ServerUserState;
import alluxio.underfs.JobUfsManager;
import alluxio.underfs.UfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.JobWorkerWebServer;
import alluxio.wire.WorkerNetAddress;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is responsible for initializing the different workers that are configured to run.
 */
@NotThreadSafe
public final class AlluxioJobWorkerProcess implements JobWorkerProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioJobWorkerProcess.class);

  /** The job worker. */
  private final JobWorker mJobWorker;

  /** gRPC server. */
  private GrpcServer mGrpcServer;

  /** Used for auto binding. **/
  private ServerSocket mBindSocket;

  /** The connect address for the rpc server. */
  private final InetSocketAddress mRpcConnectAddress;

  /** The bind address for the rpc server. */
  private final InetSocketAddress mRpcBindAddress;

  /** Worker start time in milliseconds. */
  private final long mStartTimeMs;

  /** The web ui server. */
  private final JobWorkerWebServer mWebServer;

  /**
   * Constructor of {@link AlluxioJobWorker}.
   */
  AlluxioJobWorkerProcess() {
    try {
      FileSystemContext fsContext = FileSystemContext.create(Configuration.global());
      FileSystem fileSystem = FileSystem.Factory.create(fsContext);

      mStartTimeMs = System.currentTimeMillis();
      UfsManager ufsManager = new JobUfsManager();
      mJobWorker = new JobWorker(fileSystem, fsContext, ufsManager);

      // Setup web server
      mWebServer = new JobWorkerWebServer(ServiceType.JOB_WORKER_WEB.getServiceName(),
          NetworkAddressUtils.getBindAddress(ServiceType.JOB_WORKER_WEB,
              Configuration.global()),
          this);

      // Random port binding.
      InetSocketAddress configuredBindAddress =
              NetworkAddressUtils.getBindAddress(ServiceType.JOB_WORKER_RPC,
                  Configuration.global());
      int rpcPort;
      if (configuredBindAddress.getPort() == 0) {
        mBindSocket = new ServerSocket(0);
        rpcPort = mBindSocket.getLocalPort();
      } else {
        rpcPort = configuredBindAddress.getPort();
      }
      // Reset worker RPC port based on assigned port number
      Configuration.set(PropertyKey.JOB_WORKER_RPC_PORT, rpcPort);

      mRpcBindAddress = NetworkAddressUtils.getBindAddress(ServiceType.JOB_WORKER_RPC,
          Configuration.global());
      mRpcConnectAddress = NetworkAddressUtils.getConnectAddress(ServiceType.JOB_WORKER_RPC,
          Configuration.global());
    } catch (Exception e) {
      LOG.error("Failed to create JobWorkerProcess", e);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public InetSocketAddress getRpcAddress() {
    return mRpcConnectAddress;
  }

  @Override
  public long getStartTimeMs() {
    return mStartTimeMs;
  }

  @Override
  public long getUptimeMs() {
    return System.currentTimeMillis() - mStartTimeMs;
  }

  @Override
  public InetSocketAddress getWebAddress() {
    if (mWebServer != null) {
      return new InetSocketAddress(mWebServer.getBindHost(), mWebServer.getLocalPort());
    }
    return null;
  }

  @Override
  public boolean waitForReady(int timeoutMs) {
    try {
      CommonUtils.waitFor(this + " to start",
          () -> isServing()
              && mWebServer != null
              && mWebServer.getServer().isRunning(),
          WaitForOptions.defaults().setTimeoutMs(timeoutMs));
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (TimeoutException e) {
      return false;
    }
  }

  @Override
  public void start() throws Exception {
    // NOTE: the order to start different services is sensitive. If you change it, do it cautiously.

    // Start serving the web server, this will not block.
    mWebServer.start();

    // Start each worker
    // Requirement: NetAddress set in WorkerContext, so block worker can initialize BlockMasterSync
    // Consequence: worker id is granted
    startWorkers();
    LOG.info("Started {} with id {}", this, JobWorkerIdRegistry.getWorkerId());

    // Start serving RPC, this will block
    LOG.info("Alluxio job worker version {} started. "
            + "bindHost={}, connectHost={}, rpcPort={}, webPort={}",
        RuntimeConstants.VERSION,
        NetworkAddressUtils.getBindAddress(ServiceType.JOB_WORKER_RPC,
            Configuration.global()),
        NetworkAddressUtils.getConnectAddress(ServiceType.JOB_WORKER_RPC,
            Configuration.global()),
        NetworkAddressUtils.getPort(ServiceType.JOB_WORKER_RPC, Configuration.global()),
        NetworkAddressUtils.getPort(ServiceType.JOB_WORKER_WEB, Configuration.global()));

    startServingRPCServer();
    LOG.info("Alluxio job worker ended");
  }

  private void startServingRPCServer() {
    try {
      if (mBindSocket != null) {
        // Socket opened for auto bind.
        // Close it.
        mBindSocket.close();
      }

      LOG.info("Starting gRPC server on address {}", mRpcConnectAddress);
      GrpcServerBuilder serverBuilder = GrpcServerBuilder.forAddress(
          GrpcServerAddress.create(mRpcConnectAddress.getHostName(), mRpcBindAddress),
          Configuration.global(), ServerUserState.global());

      for (Map.Entry<alluxio.grpc.ServiceType, GrpcService> serviceEntry : mJobWorker.getServices()
          .entrySet()) {
        LOG.info("Registered service:{}", serviceEntry.getKey().name());
        serverBuilder.addService(serviceEntry.getValue());
      }

      mGrpcServer = serverBuilder.build().start();
      LOG.info("Started gRPC server on address {}", mRpcConnectAddress);

      // Wait until the server is shut down.
      mGrpcServer.awaitTermination();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isServing() {
    return mGrpcServer != null && mGrpcServer.isServing();
  }

  @Override
  public void stop() throws Exception {
    LOG.info("Stopping RPC server on {} @ {}", this, mRpcConnectAddress);
    if (isServing()) {
      stopServing();
      stopWorkers();
    }
  }

  @Override
  public WorkerNetAddress getAddress() {
    return new WorkerNetAddress()
        .setHost(NetworkAddressUtils.getConnectHost(ServiceType.JOB_WORKER_RPC,
            Configuration.global()))
        .setContainerHost(Configuration.global()
            .getOrDefault(PropertyKey.WORKER_CONTAINER_HOSTNAME, ""))
        .setRpcPort(Configuration.getInt(PropertyKey.JOB_WORKER_RPC_PORT))
        .setDataPort(Configuration.getInt(PropertyKey.JOB_WORKER_DATA_PORT))
        .setWebPort(Configuration.getInt(PropertyKey.JOB_WORKER_WEB_PORT));
  }

  private void startWorkers() throws Exception {
    mJobWorker.start(getAddress());
  }

  private void stopWorkers() throws Exception {
    // stop additional workers
    mJobWorker.stop();
  }

  private void stopServing() {
    if (isServing()) {
      if (!mGrpcServer.shutdown()) {
        LOG.warn("RPC server shutdown timed out.");
      }
    }
    try {
      mWebServer.stop();
    } catch (Exception e) {
      LOG.error("Failed to stop web server", e);
    }
  }

  @Override
  public String toString() {
    return "Alluxio job worker";
  }
}
