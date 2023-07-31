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

import static java.util.Objects.requireNonNull;

import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.network.ChannelType;
import alluxio.underfs.UfsManager;
import alluxio.util.CommonUtils;
import alluxio.util.JvmPauseMonitor;
import alluxio.util.WaitForOptions;
import alluxio.util.network.NettyUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.WebServer;
import alluxio.web.WorkerWebServer;
import alluxio.wire.TieredIdentity;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.dora.DoraWorker;
import alluxio.worker.http.HttpServer;
import alluxio.worker.netty.NettyDataServer;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.netty.channel.unix.DomainSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class encapsulates the different worker services that are configured to run.
 */
@NotThreadSafe
public class AlluxioWorkerProcess implements WorkerProcess {
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioWorkerProcess.class);

  private final TieredIdentity mTieredIdentitiy;

  /**
   * Server for data requests and responses.
   */
  private DataServer mDataServer;

  private final DataServer mNettyDataServer;

  /**
   * If started (i.e. not null), this server is used to serve local data transfer.
   */
  private DataServer mDomainSocketDataServer;

  /**
   * HTTP Server provides RESTful API to get/put/append/delete page.
   */
  private HttpServer mHttpServer;

  /**
   * The worker registry.
   */
  private final WorkerRegistry mRegistry;

  /**
   * Worker Web UI server.
   */
  private WebServer mWebServer;

  /**
   * The bind address for the rpc server.
   */
  private final InetSocketAddress mRpcBindAddress;

  /**
   * The connect address for the rpc server.
   */
  private final InetSocketAddress mRpcConnectAddress;

  /**
   * Worker start time in milliseconds.
   */
  private final long mStartTimeMs;

  /**
   * The manager for all ufs.
   */
  private final UfsManager mUfsManager;

  /**
   * The jvm monitor.
   */
  private JvmPauseMonitor mJvmPauseMonitor;

  private boolean mNettyDataTransmissionEnable;

  /**
   * Creates a new instance of {@link AlluxioWorkerProcess}.
   */
  @Inject
  AlluxioWorkerProcess(
      TieredIdentity tieredIdentity,
      WorkerRegistry workerRegistry,
      UfsManager ufsManager,
      Worker worker,
      DataServerFactory dataServerFactory,
      @Nullable NettyDataServer nettyDataServer,
      @Nullable HttpServer httpServer) {
    this(tieredIdentity, workerRegistry, ufsManager, worker,
        dataServerFactory, nettyDataServer, httpServer, false);
  }

  /**
   * Creates a new instance of {@link AlluxioWorkerProcess}.
   */
  protected AlluxioWorkerProcess(
      TieredIdentity tieredIdentity,
      WorkerRegistry workerRegistry,
      UfsManager ufsManager,
      Worker worker,
      DataServerFactory dataServerFactory,
      @Nullable NettyDataServer nettyDataServer,
      @Nullable HttpServer httpServer,
      boolean delayWebServer) {
    try {
      mTieredIdentitiy = requireNonNull(tieredIdentity);
      mUfsManager = requireNonNull(ufsManager);
      mRegistry = requireNonNull(workerRegistry);
      mRpcBindAddress = requireNonNull(dataServerFactory.getGRpcBindAddress());
      mRpcConnectAddress = requireNonNull(dataServerFactory.getConnectAddress());
      mStartTimeMs = System.currentTimeMillis();
      List<Callable<Void>> callables = ImmutableList.of(() -> {
        if (worker instanceof DoraWorker) {
          mRegistry.add(DoraWorker.class, worker);
          mRegistry.addAlias(DataWorker.class, worker);
          return null;
        } else {
          throw new UnsupportedOperationException(worker.getClass().getCanonicalName()
              + " is no longer supported in Alluxio 3.x");
        }
      });
      CommonUtils.invokeAll(callables,
          Configuration.getMs(PropertyKey.WORKER_STARTUP_TIMEOUT));

      // Setup web server
      if (!delayWebServer) {
        mWebServer =
            new WorkerWebServer(NetworkAddressUtils.getBindAddress(ServiceType.WORKER_WEB,
                Configuration.global()), this,
                mRegistry.get(DataWorker.class));
      }

      // Setup GRPC server
      mDataServer = dataServerFactory.createRemoteGrpcDataServer(
          mRegistry.get(DataWorker.class));

      // Setup domain socket data server
      if (isDomainSocketEnabled()) {
        mDomainSocketDataServer = dataServerFactory.createDomainSocketDataServer(
            mRegistry.get(DataWorker.class));
      }

      // Setup Netty Data Server
      mNettyDataTransmissionEnable =
          Configuration.global().getBoolean(PropertyKey.USER_NETTY_DATA_TRANSMISSION_ENABLED);
      if (mNettyDataTransmissionEnable) {
        mNettyDataServer = nettyDataServer;
      } else {
        mNettyDataServer = null;
      }

      mHttpServer = httpServer;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void setWebServer(WebServer webServer) {
    mWebServer = webServer;
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
    if (mNettyDataTransmissionEnable) {
      return ((InetSocketAddress) mNettyDataServer.getBindAddress()).getPort();
    }
    return ((InetSocketAddress) mDataServer.getBindAddress()).getPort();
  }

  @Override
  public int getNettyDataLocalPort() {
    return ((InetSocketAddress) mNettyDataServer.getBindAddress()).getPort();
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
    MetricsSystem.startSinks(Configuration.getString(PropertyKey.METRICS_CONF_FILE));

    // Start each worker. This must be done before starting the web or RPC servers.
    // Requirement: NetAddress set in WorkerContext, so block worker can initialize BlockMasterSync
    // Consequence: worker id is granted
    startWorkers();

    // Start serving the web server, this will not block.
    mWebServer.start();

    // Start HTTP Server
    if (mHttpServer != null && Configuration.getBoolean(PropertyKey.WORKER_HTTP_SERVER_ENABLED)) {
      mHttpServer.start();
    }

    // Start monitor jvm
    if (Configuration.getBoolean(PropertyKey.WORKER_JVM_MONITOR_ENABLED)) {
      mJvmPauseMonitor =
          new JvmPauseMonitor(
              Configuration.getMs(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS),
              Configuration.getMs(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS),
              Configuration.getMs(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS));
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
    AtomicReference<Long> workerId = mRegistry.get(DataWorker.class).getWorkerId();
    LOG.info("Alluxio worker started. id={}, bindHost={}, connectHost={}, rpcPort={}, webPort={}",
        workerId,
        NetworkAddressUtils.getBindHost(ServiceType.WORKER_RPC, Configuration.global()),
        NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC, Configuration.global()),
        NetworkAddressUtils.getPort(ServiceType.WORKER_RPC, Configuration.global()),
        NetworkAddressUtils.getPort(ServiceType.WORKER_WEB, Configuration.global()));

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
    boolean dataServerStart = mDataServer != null && !mDataServer.isClosed();
    if (mNettyDataTransmissionEnable) {
      boolean nettyDataServerStart = mNettyDataServer != null && !mNettyDataServer.isClosed();
      return dataServerStart && nettyDataServerStart;
    }
    return dataServerStart;
  }

  private void startWorkers() throws Exception {
    mRegistry.start(getAddress());
  }

  private void stopWorkers() throws Exception {
    mRegistry.stop();
  }

  private void stopServing() throws Exception {
    mDataServer.close();
    if (mNettyDataServer != null) {
      mNettyDataServer.close();
    }
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
    return NettyUtils.getWorkerChannel(Configuration.global()) == ChannelType.EPOLL
        && Configuration.isSet(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS);
  }

  @Override
  public boolean waitForReady(int timeoutMs) {
    try {
      CommonUtils.waitFor(this + " to start",
          () -> isServing() && mRegistry.get(DataWorker.class).getWorkerId() != null
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
    WorkerNetAddress workerNetAddress = new WorkerNetAddress()
        .setHost(NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC,
            Configuration.global()))
        .setContainerHost(Configuration.global()
            .getOrDefault(PropertyKey.WORKER_CONTAINER_HOSTNAME, ""))
        .setRpcPort(mRpcBindAddress.getPort())
        .setDataPort(getDataLocalPort())
        .setDomainSocketPath(getDataDomainSocketPath())
        .setWebPort(mWebServer.getLocalPort())
        .setTieredIdentity(mTieredIdentitiy);
    if (mNettyDataTransmissionEnable) {
      workerNetAddress.setNettyDataPort(getNettyDataLocalPort());
    }
    return workerNetAddress;
  }

  @Override
  public String toString() {
    return "Alluxio worker @" + mRpcConnectAddress;
  }
}
