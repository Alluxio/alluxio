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
import alluxio.ServerUtils;
import alluxio.metrics.MetricsSystem;
import alluxio.metrics.sink.MetricsServlet;
import alluxio.security.authentication.TransportProvider;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.WebServer;
import alluxio.web.WorkerWebServer;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.block.DefaultBlockWorker;
import alluxio.worker.file.DefaultFileSystemWorker;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
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
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class encapsulates the different worker services that are configured to run.
 */
@NotThreadSafe
public final class DefaultAlluxioWorker implements AlluxioWorkerService {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultAlluxioWorker.class);

  /** The worker serving blocks. */
  private BlockWorker mBlockWorker;

  /** The worker serving file system operations. */
  private DefaultFileSystemWorker mFileSystemWorker;

  /** Server for data requests and responses. */
  private DataServer mDataServer;

  /** A list of extra workers to launch based on service loader. */
  private List<Worker> mAdditionalWorkers;

  /** Whether the worker is serving the RPC server. */
  private boolean mIsServingRPC = false;

  private final MetricsServlet mMetricsServlet = new MetricsServlet(MetricsSystem.METRIC_REGISTRY);

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

  /**
   * The worker ID for this worker. This is set when the block worker is initialized and may be
   * updated by the block sync thread if the master requests re-registration.
   */
  private AtomicReference<Long> mWorkerId;

  /**
   * Creates a new instance of {@link DefaultAlluxioWorker}.
   */
  public DefaultAlluxioWorker() {
    try {
      mWorkerId = new AtomicReference<>();
      mStartTimeMs = System.currentTimeMillis();
      mBlockWorker = new DefaultBlockWorker(mWorkerId);
      mFileSystemWorker = new DefaultFileSystemWorker(mBlockWorker, mWorkerId);

      mAdditionalWorkers = new ArrayList<>();
      List<? extends Worker> workers = Lists.newArrayList(mBlockWorker, mFileSystemWorker);
      for (WorkerFactory factory : ServerUtils.getWorkerServiceLoader()) {
        Worker worker = factory.create(workers);
        if (worker != null) {
          mAdditionalWorkers.add(worker);
        }
      }

      // Setup web server
      mWebServer =
          new WorkerWebServer(NetworkAddressUtils.getBindAddress(ServiceType.WORKER_WEB), this,
              mBlockWorker, NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC),
              mStartTimeMs);

      // Setup Thrift server
      mTransportProvider = TransportProvider.Factory.create();
      mThriftServerSocket = createThriftServerSocket();
      int rpcPort = NetworkAddressUtils.getThriftPort(mThriftServerSocket);
      String rpcHost = NetworkAddressUtils.getThriftSocket(mThriftServerSocket).getInetAddress()
          .getHostAddress();
      mRpcAddress = new InetSocketAddress(rpcHost, rpcPort);
      mThriftServer = createThriftServer();

      // Setup Data server
      mDataServer = DataServer.Factory
          .create(NetworkAddressUtils.getBindAddress(ServiceType.WORKER_DATA), this);
    } catch (Exception e) {
      Throwables.propagate(e);
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
    return mDataServer.getBindHost();
  }

  @Override
  public int getDataLocalPort() {
    return mDataServer.getPort();
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
  public BlockWorker getBlockWorker() {
    return mBlockWorker;
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

    // Start serving the web server, this will not block.
    mWebServer.addHandler(mMetricsServlet.getHandler());
    mWebServer.start();

    // Start each worker
    // Requirement: NetAddress set in WorkerContext, so block worker can initialize BlockMasterSync
    // Consequence: worker id is granted
    startWorkers();
    LOG.info("Started Alluxio worker with id {}", mWorkerId.get());

    mIsServingRPC = true;

    // Start serving RPC, this will block
    LOG.info("Alluxio worker version {} started @ {}", RuntimeConstants.VERSION, mRpcAddress);
    mThriftServer.serve();
    LOG.info("Alluxio worker version {} ended @ {}", RuntimeConstants.VERSION, mRpcAddress);
  }

  @Override
  public void stop() throws Exception {
    LOG.info("Stopping Alluxio worker @ {}", mRpcAddress);
    if (mIsServingRPC) {
      stopServing();
      stopWorkers();
      mIsServingRPC = false;
    }
  }

  private void startWorkers() throws Exception {
    mBlockWorker.init(getAddress());
    mBlockWorker.start();
    mFileSystemWorker.start();
    // start additional workers
    for (Worker worker : mAdditionalWorkers) {
      worker.start();
    }
  }

  private void stopWorkers() throws Exception {
    // stop additional workers
    for (Worker worker : mAdditionalWorkers) {
      worker.stop();
    }
    mFileSystemWorker.stop();
    mBlockWorker.stop();
  }

  private void stopServing() throws IOException {
    mDataServer.close();
    mThriftServer.stop();
    mThriftServerSocket.close();
    try {
      mWebServer.stop();
    } catch (Exception e) {
      LOG.error("Failed to stop web server", e);
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

    registerServices(processor, mBlockWorker.getServices());
    registerServices(processor, mFileSystemWorker.getServices());

    // register additional workers for RPC service
    for (Worker worker: mAdditionalWorkers) {
      registerServices(processor, worker.getServices());
    }

    // Return a TTransportFactory based on the authentication type
    TTransportFactory tTransportFactory;
    try {
      tTransportFactory = mTransportProvider.getServerTransportFactory();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    TThreadPoolServer.Args args = new TThreadPoolServer.Args(mThriftServerSocket)
        .minWorkerThreads(minWorkerThreads).maxWorkerThreads(maxWorkerThreads).processor(processor)
        .transportFactory(tTransportFactory)
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

  @Override
  public void waitForReady() {
    CommonUtils.waitFor("Alluxio worker to start", new Function<Void, Boolean>() {
      @Override
      public Boolean apply(Void input) {
        return mThriftServer.isServing() && mWorkerId.get() != null && mWebServer.getServer()
            .isRunning();
      }
    });
  }

  @Override
  public WorkerNetAddress getAddress() {
    return new WorkerNetAddress()
        .setHost(NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC))
        .setRpcPort(mRpcAddress.getPort())
        .setDataPort(mDataServer.getPort())
        .setWebPort(mWebServer.getLocalPort());
  }
}
