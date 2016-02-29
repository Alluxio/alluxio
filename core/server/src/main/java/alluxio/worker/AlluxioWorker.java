/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
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
import alluxio.ValidateConf;
import alluxio.Version;
import alluxio.metrics.MetricsSystem;
import alluxio.security.authentication.AuthenticationUtils;
import alluxio.util.CommonUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.util.network.NetworkAddressUtils.ServiceType;
import alluxio.web.UIWebServer;
import alluxio.web.WorkerUIWebServer;
import alluxio.wire.WorkerNetAddress;
import alluxio.worker.block.BlockWorker;
import alluxio.worker.file.FileSystemWorker;

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
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Entry point for the Alluxio worker program. This class is responsible for initializing the
 * different workers that are configured to run. It also creates the data server which listens
 * for all remote I/O requests for the workers.
 */
@NotThreadSafe
public final class AlluxioWorker {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static AlluxioWorker sAlluxioWorker = null;

  /**
   * Main method for Alluxio Worker. A Block Worker will be started and the Alluxio Worker will
   * continue to run until the Block Worker thread exits.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    checkArgs(args);
    // validate the conf
    if (!ValidateConf.validate()) {
      System.exit(-1);
    }
    AlluxioWorker worker = get();
    try {
      worker.start();
    } catch (Exception e) {
      LOG.error("Uncaught exception while running worker, stopping it and exiting.", e);
      try {
        worker.stop();
      } catch (Exception ex) {
        // continue to exit
        LOG.error("Uncaught exception while stopping worker, simply exiting.", ex);
      }
      System.exit(-1);
    }
  }

  /**
   * Returns a handle to the Alluxio worker instance.
   *
   * @return Alluxio master handle
   */
  public static synchronized AlluxioWorker get() {
    if (sAlluxioWorker == null) {
      sAlluxioWorker = new AlluxioWorker();
    }
    return sAlluxioWorker;
  }

  private Configuration mConfiguration;

  /** The worker serving blocks. */
  private BlockWorker mBlockWorker;

  /** The worker serving file system operations. */
  private FileSystemWorker mFileSystemWorker;

  /** Server for data requests and responses. */
  private DataServer mDataServer;

  /** A list of extra workers to launch based on service loader. */
  private List<Worker> mAdditionalWorkers;

  /** Whether the worker is serving the RPC server. */
  private boolean mIsServingRPC = false;

  /** Worker metrics system. */
  private MetricsSystem mWorkerMetricsSystem;

  /** Worker Web UI server. */
  private UIWebServer mWebServer;

  /** Thread pool for thrift. */
  private TThreadPoolServer mThriftServer;

  /** Server socket for thrift. */
  private TServerSocket mThriftServerSocket;

  /** RPC local port for thrift. */
  private int mRPCPort;

  /** The address for the rpc server. */
  private InetSocketAddress mWorkerAddress;

  /** Net address of this worker. */
  private WorkerNetAddress mNetAddress;

  /** Worker start time in milliseconds. */
  private long mStartTimeMs;

  /**
   * Constructor of {@link AlluxioWorker}.
   */
  public AlluxioWorker() {
    try {
      mStartTimeMs = System.currentTimeMillis();
      mConfiguration = WorkerContext.getConf();

      mBlockWorker = new BlockWorker();
      mFileSystemWorker = new FileSystemWorker(mBlockWorker);

      mAdditionalWorkers = Lists.newArrayList();
      List<? extends Worker> workers = Lists.newArrayList(mBlockWorker, mFileSystemWorker);
      // Discover and register the available factories
      // NOTE: ClassLoader is explicitly specified so we don't need to set ContextClassLoader
      ServiceLoader<WorkerFactory> discoveredMasterFactories =
          ServiceLoader.load(WorkerFactory.class, WorkerFactory.class.getClassLoader());
      for (WorkerFactory factory : discoveredMasterFactories) {
        Worker worker = factory.create(workers);
        if (worker != null) {
          mAdditionalWorkers.add(worker);
        }
      }

      // Setup metrics collection system
      mWorkerMetricsSystem = new MetricsSystem("worker", mConfiguration);
      WorkerSource workerSource = WorkerContext.getWorkerSource();
      workerSource.registerGauges(mBlockWorker);
      mWorkerMetricsSystem.registerSource(workerSource);

      // Setup web server
      mWebServer =
          new WorkerUIWebServer(ServiceType.WORKER_WEB, NetworkAddressUtils.getBindAddress(
              ServiceType.WORKER_WEB, mConfiguration), this, mBlockWorker,
              NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_RPC, mConfiguration),
              mStartTimeMs, mConfiguration);

      // Setup Thrift server
      mThriftServerSocket = createThriftServerSocket();
      mRPCPort = NetworkAddressUtils.getThriftPort(mThriftServerSocket);
      // Reset worker RPC port based on assigned port number
      mConfiguration.set(Constants.WORKER_RPC_PORT, Integer.toString(mRPCPort));
      mThriftServer = createThriftServer();

      // Setup Data server
      mDataServer = DataServer.Factory.create(
          NetworkAddressUtils.getBindAddress(ServiceType.WORKER_DATA, mConfiguration),
          mBlockWorker, mConfiguration);
      // Reset data server port
      mConfiguration.set(Constants.WORKER_DATA_PORT, Integer.toString(mDataServer.getPort()));

      mWorkerAddress =
          NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.WORKER_RPC,
              mConfiguration);

    } catch (Exception e) {
      LOG.error("Failed to initialize {}", this.getClass().getName(), e);
      System.exit(-1);
    }
  }

  /**
   * @return the worker RPC service bind host
   */
  public String getRPCBindHost() {
    return NetworkAddressUtils.getThriftSocket(mThriftServerSocket).getInetAddress()
        .getHostAddress();
  }

  /**
   * @return the worker RPC service port
   */
  public int getRPCLocalPort() {
    return mRPCPort;
  }

  /**
   * @return the worker data service bind host (used by unit test only)
   */
  public String getDataBindHost() {
    return mDataServer.getBindHost();
  }

  /**
   * @return the worker data service port (used by unit test only)
   */
  public int getDataLocalPort() {
    return mDataServer.getPort();
  }

  /**
   * @return the worker web service bind host (used by unit test only)
   */
  public String getWebBindHost() {
    return mWebServer.getBindHost();
  }

  /**
   * @return the worker web service port (used by unit test only)
   */
  public int getWebLocalPort() {
    return mWebServer.getLocalPort();
  }

  /**
   * @return the block worker
   */
  public BlockWorker getBlockWorker() {
    return mBlockWorker;
  }

  /**
   * Gets this worker's {@link WorkerNetAddress}, which is the worker's hostname, rpc
   * server port, data server port, and web server port.
   *
   * @return the worker's net address
   */
  public WorkerNetAddress getNetAddress() {
    return mNetAddress;
  }

  /**
   * Starts the Alluxio worker server.
   *
   * @throws Exception if the workers fail to start
   */
  public void start() throws Exception {
    // NOTE: the order to start different services is sensitive. If you change it, do it cautiously.

    // Start serving metrics system, this will not block
    mWorkerMetricsSystem.start();

    // Start serving the web server, this will not block
    // Requirement: metrics system started so we could add the metrics servlet to the web server
    // Consequence: when starting webserver, the webport will be updated.
    mWebServer.addHandler(mWorkerMetricsSystem.getServletHandler());
    mWebServer.startWebServer();

    // Set updated net address for this worker in context
    // Requirement: RPC, web, and dataserver ports are updated
    // Consequence: create a NetAddress object and set it into WorkerContext
    mNetAddress =
        new WorkerNetAddress()
            .setHost(NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC, mConfiguration))
            .setRpcPort(mConfiguration.getInt(Constants.WORKER_RPC_PORT))
            .setDataPort(getDataLocalPort())
            .setWebPort(mConfiguration.getInt(Constants.WORKER_WEB_PORT));
    WorkerContext.setWorkerNetAddress(mNetAddress);

    // Start each worker
    // Requirement: NetAddress set in WorkerContext, so block worker can initialize BlockMasterSync
    // Consequence: worker id is granted
    startWorkers();
    LOG.info("Started worker with id {}", WorkerIdRegistry.getWorkerId());

    mIsServingRPC = true;

    // Start serving RPC, this will block
    LOG.info("Alluxio Worker version {} started @ {}", Version.VERSION, mWorkerAddress);
    mThriftServer.serve();
    LOG.info("Alluxio Worker version {} ended @ {}", Version.VERSION, mWorkerAddress);
  }

  /**
   * Stops the Alluxio worker server.
   *
   * @throws Exception if the workers fail to stop
   */
  public void stop() throws Exception {
    if (mIsServingRPC) {
      LOG.info("Stopping RPC server on Alluxio Worker @ {}", mWorkerAddress);
      stopServing();
      stopWorkers();
      mIsServingRPC = false;
    } else {
      LOG.info("Stopping Alluxio Worker @ {}", mWorkerAddress);
    }
  }

  private void startWorkers() throws Exception {
    mBlockWorker.start();
    mFileSystemWorker.start();
    // start additional workers
    for (Worker master : mAdditionalWorkers) {
      master.start();
    }
  }

  private void stopWorkers() throws Exception {
    // stop additional workers
    for (Worker master : mAdditionalWorkers) {
      master.stop();
    }
    mFileSystemWorker.stop();
    mBlockWorker.stop();
  }

  private void stopServing() throws IOException {
    mDataServer.close();
    mThriftServer.stop();
    mThriftServerSocket.close();
    mWorkerMetricsSystem.stop();
    try {
      mWebServer.shutdownWebServer();
    } catch (Exception e) {
      LOG.error("Failed to stop web server", e);
    }
    mWorkerMetricsSystem.stop();

    // TODO(binfan): investigate why we need to close dataserver again. There used to be a comment
    // saying the reason to stop and close again is due to some issues in Thrift.
    while (!mDataServer.isClosed()) {
      mDataServer.close();
      CommonUtils.sleepMs(100);
    }
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
    int minWorkerThreads = mConfiguration.getInt(Constants.WORKER_WORKER_BLOCK_THREADS_MIN);
    int maxWorkerThreads = mConfiguration.getInt(Constants.WORKER_WORKER_BLOCK_THREADS_MAX);
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
      tTransportFactory = AuthenticationUtils.getServerTransportFactory(mConfiguration);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    TThreadPoolServer.Args args = new TThreadPoolServer.Args(mThriftServerSocket)
        .minWorkerThreads(minWorkerThreads).maxWorkerThreads(maxWorkerThreads).processor(processor)
        .transportFactory(tTransportFactory)
        .protocolFactory(new TBinaryProtocol.Factory(true, true));
    if (WorkerContext.getConf().getBoolean(Constants.IN_TEST_MODE)) {
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
      return new TServerSocket(
          NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC, mConfiguration));
    } catch (TTransportException e) {
      LOG.error(e.getMessage(), e);
      throw Throwables.propagate(e);
    }
  }

  /**
   * Verifies that no parameters are passed in.
   *
   * @param args command line arguments
   */
  private static void checkArgs(String[] args) {
    if (args.length != 0) {
      LOG.info("Usage: java AlluxioWorker");
      System.exit(-1);
    }
  }

  /**
   * @return the master metric system reference
   */
  public MetricsSystem getWorkerMetricsSystem() {
    return mWorkerMetricsSystem;
  }
}
