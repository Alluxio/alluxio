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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.RuntimeConstants;
import alluxio.ServiceUtils;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.sink.MetricsServlet;
import alluxio.metrics.sink.PrometheusMetricsServlet;
import alluxio.network.ChannelType;
import alluxio.network.thrift.BootstrapServerTransport;
import alluxio.network.thrift.ThriftUtils;
import alluxio.security.authentication.TransportProvider;
import alluxio.underfs.DefaultUfsClientCache;
import alluxio.underfs.UfsCache;
import alluxio.underfs.WorkerUfsClientFetcher;
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

import com.google.common.base.Throwables;
import io.netty.channel.unix.DomainSocketAddress;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
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

  /** Whether the worker is serving the RPC server. */
  private boolean mIsServingRPC = false;

  private final MetricsServlet mMetricsServlet = new MetricsServlet(MetricsSystem.METRIC_REGISTRY);
  private final PrometheusMetricsServlet mPMetricsServlet = new PrometheusMetricsServlet(
      MetricsSystem.METRIC_REGISTRY);

  /** The worker registry. */
  private WorkerRegistry mRegistry;

  /** Worker Web UI server. */
  private WebServer mWebServer;

  /** The transport provider to create thrift server transport. */
  private TransportProvider mTransportProvider;

  /** Thread pool for thrift. */
  private TThreadPoolServer mThriftServer;

  /** Server socket for thrift. */
  private TServerSocket mThriftServerSocket;

  /** The address for the rpc server. */
  private InetSocketAddress mRpcAddress;

  /** Worker start time in milliseconds. */
  private long mStartTimeMs;

  /** The manager for all ufs. */
  private UfsClientCache mUfsClientCache;

  /** The jvm monitor.*/
  private JvmPauseMonitor mJvmPauseMonitor;

  /**
   * Creates a new instance of {@link AlluxioWorkerProcess}.
   */
  AlluxioWorkerProcess(TieredIdentity tieredIdentity) {
    mTieredIdentitiy = tieredIdentity;
    try {
      mStartTimeMs = System.currentTimeMillis();
      UfsCache ufsCache = new UfsCache();
      mUfsClientCache = new DefaultUfsClientCache(ufsCache, new WorkerUfsClientFetcher(ufsCache));
      WorkerContext workerContext = new WorkerContext(mUfsClientCache);
      mRegistry = new WorkerRegistry();
      List<Callable<Void>> callables = new ArrayList<>();
      for (final WorkerFactory factory : ServiceUtils.getWorkerServiceLoader()) {
        callables.add(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            if (factory.isEnabled()) {
              factory.create(mRegistry, workerContext);
            }
            return null;
          }
        });
      }
      // In the worst case, each worker factory is blocked waiting for the dependent servers to be
      // registered at worker registry, so the maximum timeout here is set to the multiply of
      // the number of factories by the default timeout of getting a worker from the registry.
      CommonUtils.invokeAll(callables,
          (long) callables.size() * Constants.DEFAULT_REGISTRY_GET_TIMEOUT_MS,
          TimeUnit.MILLISECONDS);

      // Setup web server
      mWebServer =
          new WorkerWebServer(NetworkAddressUtils.getBindAddress(ServiceType.WORKER_WEB), this,
              mRegistry.get(BlockWorker.class),
              NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC), mStartTimeMs);

      // Setup Thrift server
      mTransportProvider = TransportProvider.Factory.create();
      mThriftServerSocket = createThriftServerSocket();
      int rpcPort = ThriftUtils.getThriftPort(mThriftServerSocket);
      String rpcHost = ThriftUtils.getThriftSocket(mThriftServerSocket).getInetAddress()
          .getHostAddress();
      mRpcAddress = new InetSocketAddress(rpcHost, rpcPort);
      mThriftServer = createThriftServer();

      // Setup Data server
      mDataServer = DataServer.Factory
          .create(NetworkAddressUtils.getBindAddress(ServiceType.WORKER_DATA), this);

      // Setup domain socket data server
      if (isDomainSocketEnabled()) {
        String domainSocketPath =
            Configuration.get(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS);
        if (Configuration.getBoolean(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID)) {
          domainSocketPath =
              PathUtils.concatPath(domainSocketPath, UUID.randomUUID().toString());
        }
        LOG.info("Domain socket data server is enabled at {}.", domainSocketPath);
        mDomainSocketDataServer =
            DataServer.Factory.create(new DomainSocketAddress(domainSocketPath), this);
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
  public UfsClientCache getUfsClientCache() {
    return mUfsClientCache;
  }

  @Override
  public InetSocketAddress getRpcAddress() {
    return mRpcAddress;
  }

  @Override
  public void start() throws Exception {
    // NOTE: the order to start different services is sensitive. If you change it, do it cautiously.

    // Start serving metrics system, this will not block
    MetricsSystem.startSinks();

    // Start each worker. This must be done before starting the web or RPC servers.
    // Requirement: NetAddress set in WorkerContext, so block worker can initialize BlockMasterSync
    // Consequence: worker id is granted
    startWorkers();
    LOG.info("Started {} with id {}", this, mRegistry.get(BlockWorker.class).getWorkerId());

    // Start serving the web server, this will not block.
    mWebServer.addHandler(mMetricsServlet.getHandler());
    mWebServer.addHandler(mPMetricsServlet.getHandler());
    mWebServer.start();

    // Start monitor jvm
    if (Configuration.getBoolean(PropertyKey.WORKER_JVM_MONITOR_ENABLED)) {
      mJvmPauseMonitor = new JvmPauseMonitor();
      mJvmPauseMonitor.start();
    }

    mIsServingRPC = true;

    // Start serving RPC, this will block
    LOG.info("Alluxio worker version {} started. "
            + "bindHost={}, connectHost={}, rpcPort={}, dataPort={}, webPort={}",
        RuntimeConstants.VERSION,
        NetworkAddressUtils.getBindHost(ServiceType.WORKER_RPC),
        NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC),
        NetworkAddressUtils.getPort(ServiceType.WORKER_RPC),
        NetworkAddressUtils.getPort(ServiceType.WORKER_DATA),
        NetworkAddressUtils.getPort(ServiceType.WORKER_WEB));
    mThriftServer.serve();
    LOG.info("Alluxio worker ended");
  }

  @Override
  public void stop() throws Exception {
    if (mIsServingRPC) {
      stopServing();
      if (mJvmPauseMonitor != null) {
        mJvmPauseMonitor.stop();
      }
      stopWorkers();
      mIsServingRPC = false;
    }
  }

  private void startWorkers() throws Exception {
    mRegistry.start(getAddress());
  }

  private void stopWorkers() throws Exception {
    mRegistry.stop();
  }

  private void stopServing() throws IOException {
    mDataServer.close();
    if (mDomainSocketDataServer != null) {
      mDomainSocketDataServer.close();
      mDomainSocketDataServer = null;
    }
    mThriftServer.stop();
    mThriftServerSocket.close();
    mUfsClientCache.close();
    try {
      mWebServer.stop();
    } catch (Exception e) {
      LOG.error("Failed to stop {} web server", this, e);
    }
    MetricsSystem.stopSinks();
  }

  private void registerServices(TMultiplexedProcessor processor, Map<String, TProcessor> services) {
    for (Map.Entry<String, TProcessor> service : services.entrySet()) {
      processor.registerProcessor(service.getKey(), service.getValue());
    }
  }

  /**
   * Helper method to create a {@link org.apache.thrift.server.TThreadPoolServer} for handling
   * incoming RPC requests.
   *
   * @return a thrift server
   */
  private TThreadPoolServer createThriftServer() {
    int minWorkerThreads = Configuration.getInt(PropertyKey.WORKER_BLOCK_THREADS_MIN);
    int maxWorkerThreads = Configuration.getInt(PropertyKey.WORKER_BLOCK_THREADS_MAX);
    TMultiplexedProcessor processor = new TMultiplexedProcessor();

    for (Worker worker : mRegistry.getServers()) {
      registerServices(processor, worker.getServices());
    }

    // Return a TTransportFactory based on the authentication type
    TTransportFactory transportFactory;
    try {
      String serverName = NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC);
      transportFactory = new BootstrapServerTransport.Factory(mTransportProvider
          .getServerTransportFactory(serverName));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    TThreadPoolServer.Args args = new TThreadPoolServer.Args(mThriftServerSocket)
        .minWorkerThreads(minWorkerThreads).maxWorkerThreads(maxWorkerThreads).processor(processor)
        .transportFactory(transportFactory)
        .protocolFactory(new TBinaryProtocol.Factory(true, true));
    if (Configuration.getBoolean(PropertyKey.TEST_MODE)) {
      args.stopTimeoutVal = 0;
    } else {
      args.stopTimeoutVal = Constants.THRIFT_STOP_TIMEOUT_SECONDS;
    }
    return new TThreadPoolServer(args);
  }

  /**
   * Helper method to create a {@link org.apache.thrift.transport.TServerSocket} for the RPC server.
   *
   * @return a thrift server socket
   */
  private TServerSocket createThriftServerSocket() {
    try {
      return new TServerSocket(NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC));
    } catch (TTransportException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * @return true if domain socket is enabled
   */
  private boolean isDomainSocketEnabled() {
    return NettyUtils.WORKER_CHANNEL_TYPE == ChannelType.EPOLL
        && !Configuration.get(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_ADDRESS).isEmpty();
  }

  @Override
  public boolean waitForReady(int timeoutMs) {
    try {
      CommonUtils.waitFor(this + " to start",
          () -> mThriftServer.isServing() && mRegistry.get(BlockWorker.class).getWorkerId() != null
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
  public WorkerNetAddress getAddress() {
    return new WorkerNetAddress()
        .setHost(NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC))
        .setRpcPort(mRpcAddress.getPort())
        .setDataPort(getDataLocalPort())
        .setDomainSocketPath(getDataDomainSocketPath())
        .setWebPort(mWebServer.getLocalPort())
        .setTieredIdentity(mTieredIdentitiy);
  }

  @Override
  public String toString() {
    return "Alluxio worker";
  }
}
