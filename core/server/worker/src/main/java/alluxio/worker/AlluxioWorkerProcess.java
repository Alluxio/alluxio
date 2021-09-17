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

import alluxio.Constants;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.ChannelType;
import alluxio.underfs.UfsManager;
import alluxio.underfs.WorkerUfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.JvmPauseMonitor;
import alluxio.util.WaitForOptions;
import alluxio.util.io.FileUtils;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NettyUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.WebServer;
import alluxio.web.WorkerWebServer;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.grpc.GrpcDataServer;

import io.netty.channel.unix.DomainSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class encapsulates the different worker services that are configured to run.
 */
@NotThreadSafe
public final class AlluxioWorkerProcess implements WorkerProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioWorkerProcess.class);

  private final TieredIdentity mTieredIdentitiy;

  /** Server for data requests and responses. */
  private DataServer mDataServer;

  /** If started (i.e. not null), this server is used to serve local data transfer. */
  private DataServer mDomainSocketDataServer;

  /** The worker registry. */
  private WorkerRegistry mRegistry;

  /** Worker Web UI server. */
  private WebServer mWebServer;

  /** Used for auto binding. **/
  private ServerSocket mBindSocket;

  /** The bind address for the rpc server. */
  private InetSocketAddress mRpcBindAddress;

  /** The connect address for the rpc server. */
  private InetSocketAddress mRpcConnectAddress;

  /** Worker start time in milliseconds. */
  private final long mStartTimeMs;

  /** The manager for all ufs. */
  private UfsManager mUfsManager;

  /** The jvm monitor.*/
  private JvmPauseMonitor mJvmPauseMonitor;

  /**
   * Creates a new instance of {@link AlluxioWorkerProcess}.
   */
  AlluxioWorkerProcess(TieredIdentity tieredIdentity) {
    mTieredIdentitiy = tieredIdentity;
    try {
      mStartTimeMs = System.currentTimeMillis();
      mUfsManager = new WorkerUfsManager();
      mRegistry = new WorkerRegistry();
      List<Callable<Void>> callables = new ArrayList<>();
      for (final WorkerFactory factory : ServiceLoader.load(WorkerFactory.class,
          WorkerFactory.class.getClassLoader())) {
        callables.add(() -> {
          if (factory.isEnabled()) {
            factory.create(mRegistry, mUfsManager);
          }
          return null;
        });
      }
      // In the worst case, each worker factory is blocked waiting for the dependent servers to be
      // registered at worker registry, so the maximum timeout here is set to the multiply of
      // the number of factories by the default timeout of getting a worker from the registry.
      CommonUtils.invokeAll(callables,
          (long) callables.size() * 10 * Constants.DEFAULT_REGISTRY_GET_TIMEOUT_MS);

      // Setup web server
      mWebServer =
          new WorkerWebServer(NetworkAddressUtils.getBindAddress(ServiceType.WORKER_WEB,
              ServerConfiguration.global()), this,
              mRegistry.get(BlockWorker.class),
              NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC,
                  ServerConfiguration.global()),
              mStartTimeMs);

      // Random port binding.
      int bindPort;
      InetSocketAddress configuredBindAddress =
              NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC,
                  ServerConfiguration.global());
      if (configuredBindAddress.getPort() == 0) {
        mBindSocket = new ServerSocket(0);
        bindPort = mBindSocket.getLocalPort();
      } else {
        bindPort = configuredBindAddress.getPort();
      }
      mRpcBindAddress = new InetSocketAddress(configuredBindAddress.getHostName(), bindPort);
      mRpcConnectAddress = NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_RPC,
          ServerConfiguration.global());
      if (mBindSocket != null) {
        // Socket opened for auto bind.
        // Close it.
        mBindSocket.close();
      }
      // Setup Data server
      mDataServer = new GrpcDataServer(mRpcConnectAddress.getHostName(), mRpcBindAddress, this);

      // Setup domain socket data server
      if (isDomainSocketEnabled()) {
        String domainSocketPath =
            ServerConfiguration.get(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS);
        if (ServerConfiguration.getBoolean(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID)) {
          domainSocketPath =
              PathUtils.concatPath(domainSocketPath, UUID.randomUUID().toString());
        }
        LOG.info("Domain socket data server is enabled at {}.", domainSocketPath);
        mDomainSocketDataServer = new GrpcDataServer(mRpcConnectAddress.getHostName(),
            new DomainSocketAddress(domainSocketPath), this);
        // Share domain socket so that clients can access it.
        FileUtils.changeLocalFileToFullPermission(domainSocketPath);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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
  public String getDataBindHost() {
    return ((InetSocketAddress) mDataServer.getBindAddress()).getHostString();
  }

  @Override
  public int getDataLocalPort() {
    return ((InetSocketAddress) mDataServer.getBindAddress()).getPort();
  }

  @Override
  public String getDataDomainSocketPath() {
    if (mDomainSocketDataServer != null) {
      return ((DomainSocketAddress) mDomainSocketDataServer.getBindAddress()).path();
    }
    return "";
  }

  @Override
  public String getWebBindHost() {
    return mWebServer.getBindHost();
  }

  @Override
  public int getWebLocalPort() {
    return mWebServer.getLocalPort();
  }

  @Override
  public <T extends Worker> T getWorker(Class<T> clazz) {
    return mRegistry.get(clazz);
  }

  @Override
  public UfsManager getUfsManager() {
    return mUfsManager;
  }

  @Override
  public InetSocketAddress getRpcAddress() {
    return mRpcBindAddress;
  }

  @Override
  public void start() throws Exception {
    // NOTE: the order to start different services is sensitive. If you change it, do it cautiously.

    // Start serving metrics system, this will not block
    MetricsSystem.startSinks(ServerConfiguration.get(PropertyKey.METRICS_CONF_FILE));

    // Start each worker. This must be done before starting the web or RPC servers.
    // Requirement: NetAddress set in WorkerContext, so block worker can initialize BlockMasterSync
    // Consequence: worker id is granted
    startWorkers();

    // Start serving the web server, this will not block.
    mWebServer.start();

    // Start monitor jvm
    if (ServerConfiguration.getBoolean(PropertyKey.WORKER_JVM_MONITOR_ENABLED)) {
      mJvmPauseMonitor =
          new JvmPauseMonitor(
              ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS),
              ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS),
              ServerConfiguration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS));
      mJvmPauseMonitor.start();
      MetricsSystem.registerGaugeIfAbsent(
              MetricsSystem.getMetricName(MetricKey.TOTAL_EXTRA_TIME.getName()),
              mJvmPauseMonitor::getTotalExtraTime);
      MetricsSystem.registerGaugeIfAbsent(
              MetricsSystem.getMetricName(MetricKey.INFO_TIME_EXCEEDED.getName()),
              mJvmPauseMonitor::getInfoTimeExceeded);
      MetricsSystem.registerGaugeIfAbsent(
              MetricsSystem.getMetricName(MetricKey.WARN_TIME_EXCEEDED.getName()),
              mJvmPauseMonitor::getWarnTimeExceeded);
    }

    // Start serving RPC, this will block
    LOG.info("Alluxio worker started. id={}, bindHost={}, connectHost={}, rpcPort={}, webPort={}",
        mRegistry.get(BlockWorker.class).getWorkerId(),
        NetworkAddressUtils.getBindHost(ServiceType.WORKER_RPC, ServerConfiguration.global()),
        NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC, ServerConfiguration.global()),
        NetworkAddressUtils.getPort(ServiceType.WORKER_RPC, ServerConfiguration.global()),
        NetworkAddressUtils.getPort(ServiceType.WORKER_WEB, ServerConfiguration.global()));

    mDataServer.awaitTermination();

    LOG.info("Alluxio worker ended");
  }

  @Override
  public void stop() throws Exception {
    if (isServing()) {
      stopServing();
      if (mJvmPauseMonitor != null) {
        mJvmPauseMonitor.stop();
      }
    }
    stopWorkers();
  }

  private boolean isServing() {
    return mDataServer != null && !mDataServer.isClosed();
  }

  private void startWorkers() throws Exception {
    mRegistry.start(getAddress());
  }

  private void stopWorkers() throws Exception {
    mRegistry.stop();
  }

  private void stopServing() throws Exception {
    mDataServer.close();
    if (mDomainSocketDataServer != null) {
      mDomainSocketDataServer.close();
      mDomainSocketDataServer = null;
    }
    mUfsManager.close();
    try {
      mWebServer.stop();
    } catch (Exception e) {
      LOG.error("Failed to stop {} web server", this, e);
    }
    MetricsSystem.stopSinks();
  }

  /**
   * @return true if domain socket is enabled
   */
  private boolean isDomainSocketEnabled() {
    return NettyUtils.getWorkerChannel(ServerConfiguration.global()) == ChannelType.EPOLL
        && ServerConfiguration.isSet(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS);
  }

  @Override
  public boolean waitForReady(int timeoutMs) {
    try {
      CommonUtils.waitFor(this + " to start",
          () -> isServing() && mRegistry.get(BlockWorker.class).getWorkerId() != null
              && mWebServer != null && mWebServer.getServer().isRunning(),
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
  public WorkerNetAddress getAddress() {
    return new WorkerNetAddress()
        .setHost(NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC,
            ServerConfiguration.global()))
        .setContainerHost(ServerConfiguration.global()
            .getOrDefault(PropertyKey.WORKER_CONTAINER_HOSTNAME, ""))
        .setRpcPort(mRpcBindAddress.getPort())
        .setDataPort(getDataLocalPort())
        .setDomainSocketPath(getDataDomainSocketPath())
        .setWebPort(mWebServer.getLocalPort())
        .setTieredIdentity(mTieredIdentitiy);
  }

  @Override
  public String toString() {
    return "Alluxio worker @" + mRpcConnectAddress;
  }
}
