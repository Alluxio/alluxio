/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.worker;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import tachyon.Constants;
import tachyon.Version;
import tachyon.conf.TachyonConf;
import tachyon.metrics.MetricsSystem;
import tachyon.security.authentication.AuthenticationUtils;
import tachyon.thrift.BlockWorkerClientService;
import tachyon.thrift.WorkerNetAddress;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;
import tachyon.web.UIWebServer;
import tachyon.web.WorkerUIWebServer;
import tachyon.worker.block.BlockWorker;
import tachyon.worker.block.BlockWorkerClientServiceHandler;
import tachyon.worker.file.FileSystemWorker;

/**
 * Entry point for the Tachyon Worker. This class is responsible for initializing the different
 * workers that are configured to run.
 * <p>
 * This class is not thread-safe.
 */
public final class TachyonWorker {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private TachyonConf mTachyonConf;
  // The workers
  /** The worker serving blocks */
  private BlockWorker mBlockWorker;
  /** The worker serving file system operations */
  private FileSystemWorker mFileSystemWorker;

  /** is true if the worker is serving the RPC server */
  private boolean mIsServingRPC = false;
  /** Worker metrics system */
  private MetricsSystem mWorkerMetricsSystem;
  /** Worker Web UI server */
  private UIWebServer mWebServer;
  /** Thread pool for thrift */
  private TThreadPoolServer mThriftServer;
  /** Server socket for thrift */
  private TServerSocket mThriftServerSocket;
  /** RPC local port for thrift */
  private int mRPCPort;
  /** The address for the rpc server */
  private InetSocketAddress mWorkerAddress;
  /** Net address of this worker */
  private WorkerNetAddress mWorkerNetAddress;
  /** Worker start time in milliseconds */
  private long mStartTimeMs;

  /**
   * Constructor of {@link TachyonWorker}.
   */
  public TachyonWorker() {
    try {
      mStartTimeMs = System.currentTimeMillis();
      mTachyonConf = WorkerContext.getConf();

      mBlockWorker = new BlockWorker();
      mFileSystemWorker = new FileSystemWorker(mBlockWorker.getBlockDataManager());

      // Setup metrics collection system
      mWorkerMetricsSystem = new MetricsSystem("worker", mTachyonConf);
      WorkerSource workerSource = WorkerContext.getWorkerSource();
      workerSource.registerGauges(mBlockWorker.getBlockDataManager());
      mWorkerMetricsSystem.registerSource(workerSource);

      // Setup web server
      mWebServer =
          new WorkerUIWebServer(ServiceType.WORKER_WEB, NetworkAddressUtils.getBindAddress(
              ServiceType.WORKER_WEB, mTachyonConf), mBlockWorker.getBlockDataManager(),
              NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_RPC, mTachyonConf),
              mStartTimeMs, mTachyonConf);

      // Setup Thrift server
      mThriftServerSocket = createThriftServerSocket();
      mRPCPort = NetworkAddressUtils.getThriftPort(mThriftServerSocket);
      // Reset worker RPC port based on assigned port number
      mTachyonConf.set(Constants.WORKER_RPC_PORT, Integer.toString(mRPCPort));
      mThriftServer = createThriftServer();

      mWorkerAddress =
          NetworkAddressUtils.getConnectAddress(NetworkAddressUtils.ServiceType.WORKER_RPC,
              mTachyonConf);

    } catch (Exception e) {
      LOG.error("Failed to initialize {}", this.getClass().getName(), e);
      System.exit(-1);
    }
  }

  /**
   * Main method for Tachyon Worker. A Block Worker will be started and the Tachyon Worker will
   * continue to run until the Block Worker thread exits.
   *
   * @param args command line arguments, should be empty
   */
  public static void main(String[] args) {
    checkArgs(args);
    TachyonWorker worker = new TachyonWorker();
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
    return mBlockWorker.getDataBindHost();
  }

  /**
   * @return the worker data service port (used by unit test only)
   */
  public int getDataLocalPort() {
    return mBlockWorker.getDataLocalPort();
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
   * @return the worker service handler (used by unit test only)
   */
  public BlockWorkerClientServiceHandler getBlockWorkerServiceHandler() {
    return mBlockWorker.getWorkerServiceHandler();
  }

  /**
   * Gets this worker's {@link tachyon.thrift.WorkerNetAddress}, which is the worker's hostname, rpc
   * server port, data server port, and web server port.
   *
   * @return the worker's net address
   */
  public WorkerNetAddress getWorkerNetAddress() {
    return mWorkerNetAddress;
  }

  /**
   * Starts the Tachyon worker server.
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
    mWorkerNetAddress = new WorkerNetAddress(
        NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC, mTachyonConf),
        mTachyonConf.getInt(Constants.WORKER_RPC_PORT), getDataLocalPort(),
        mTachyonConf.getInt(Constants.WORKER_WEB_PORT));
    WorkerContext.setWorkerNetAddress(mWorkerNetAddress);

    // Start each worker
    // Requirement: NetAddress set in WorkerContext, so block worker can initialize BlockMasterSync
    // Consequence: worker id is granted
    startWorkers();
    LOG.info("Started worker with id {}", WorkerIdRegistry.getWorkerId());

    mIsServingRPC = true;

    // Start serving RPC, this will block
    LOG.info("Tachyon Worker version {} started @ {}", Version.VERSION, mWorkerAddress);
    mThriftServer.serve();
    LOG.info("Tachyon Worker version {} ended @ {}", Version.VERSION, mWorkerAddress);
  }

  /**
   * Stops the Tachyon worker server.
   */
  public void stop() throws Exception {
    if (mIsServingRPC) {
      LOG.info("Stopping RPC server on Tachyon Worker @ {}", mWorkerAddress);
      stopServing();
      stopWorkers();
      mIsServingRPC = false;
    } else {
      LOG.info("Stopping Tachyon Worker @ {}", mWorkerAddress);
    }
  }

  private void startWorkers() throws Exception {
    mBlockWorker.start();
    mFileSystemWorker.start();
  }

  private void stopWorkers() throws Exception {
    mFileSystemWorker.stop();
    mBlockWorker.stop();
  }

  private void stopServing() {
    mThriftServer.stop();
    mThriftServerSocket.close();
    mWorkerMetricsSystem.stop();
    try {
      mWebServer.shutdownWebServer();
    } catch (Exception e) {
      LOG.error("Failed to stop web server", e);
    }
    mWorkerMetricsSystem.stop();
  }

  /**
   * Helper method to create a {@link org.apache.thrift.server.TThreadPoolServer} for handling
   * incoming RPC requests.
   *
   * @return a thrift server
   */
  private TThreadPoolServer createThriftServer() {
    int minWorkerThreads = mTachyonConf.getInt(Constants.WORKER_WORKER_BLOCK_THREADS_MIN);
    int maxWorkerThreads = mTachyonConf.getInt(Constants.WORKER_WORKER_BLOCK_THREADS_MAX);
    BlockWorkerClientService.Processor<BlockWorkerClientServiceHandler> processor =
        new BlockWorkerClientService.Processor<BlockWorkerClientServiceHandler>(
            mBlockWorker.getWorkerServiceHandler());
    TTransportFactory tTransportFactory;
    try {
      tTransportFactory = AuthenticationUtils.getServerTransportFactory(mTachyonConf);
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
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
   * Helper method to create a {@link org.apache.thrift.transport.TServerSocket} for the RPC server
   *
   * @return a thrift server socket
   */
  private TServerSocket createThriftServerSocket() {
    try {
      return new TServerSocket(
          NetworkAddressUtils.getBindAddress(ServiceType.WORKER_RPC, mTachyonConf));
    } catch (TTransportException tte) {
      LOG.error(tte.getMessage(), tte);
      throw Throwables.propagate(tte);
    }
  }

  /**
   * Verifies that no parameters are passed in.
   *
   * @param args command line arguments
   */
  private static void checkArgs(String[] args) {
    if (args.length != 0) {
      LOG.info("Usage: java TachyonWorker");
      System.exit(-1);
    }
  }
}
