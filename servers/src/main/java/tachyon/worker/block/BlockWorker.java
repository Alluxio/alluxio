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

package tachyon.worker.block;

import java.io.IOException;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.exception.ConnectionFailedException;
import tachyon.heartbeat.HeartbeatContext;
import tachyon.heartbeat.HeartbeatThread;
import tachyon.metrics.MetricsSystem;
import tachyon.thrift.NetAddress;
import tachyon.util.CommonUtils;
import tachyon.util.ThreadFactoryUtils;
import tachyon.util.network.NetworkAddressUtils;
import tachyon.util.network.NetworkAddressUtils.ServiceType;
import tachyon.web.UIWebServer;
import tachyon.web.WorkerUIWebServer;
import tachyon.worker.DataServer;
import tachyon.worker.WorkerBase;
import tachyon.worker.WorkerContext;
import tachyon.worker.WorkerIdRegistry;
import tachyon.worker.WorkerSource;
import tachyon.worker.file.FileSystemMasterClient;

/**
 * The class is responsible for managing all top level components of the Block Worker, including:
 *
 * Servers: {@link BlockWorkerClientServiceHandler} (RPC Server), BlockDataServer (Data Server)
 *
 * Periodic Threads: {@link BlockMasterSync} (Worker to Master continuous communication)
 *
 * Logic: {@link BlockDataManager} (Logic for all block related storage operations)
 */
public final class BlockWorker extends WorkerBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /** Runnable responsible for heartbeating and registration with master. */
  private final BlockMasterSync mBlockMasterSync;
  /** Runnable responsible for fetching pinlist from master. */
  private final PinListSync mPinListSync;
  /** Runnable responsible for clean up potential zombie sessions. */
  private final SessionCleaner mSessionCleanerThread;
  /** Logic for handling RPC requests. */
  private final BlockWorkerClientServiceHandler mServiceHandler;
  /** Logic for managing block store and under file system store. */
  private final BlockDataManager mBlockDataManager;
  /** Server for data requests and responses. */
  private final DataServer mDataServer;
  /** Client for all block master communication */
  private final BlockMasterClient mBlockMasterClient;
  /** Client for all file system master communication */
  private final FileSystemMasterClient mFileSystemMasterClient;
  /** Net address of this worker */
  private final NetAddress mWorkerNetAddress;
  /** Configuration object */
  private final TachyonConf mTachyonConf;
  /** RPC local port for thrift */
  private final int mPort;
  /** Worker start time in milliseconds */
  private final long mStartTimeMs;
  // TODO(binfan): move to TachyonWorker
  /** Worker Web UI server */
  private final UIWebServer mWebServer;
  // TODO(binfan): move to TachyonWorker
  /** Worker metrics system */
  private MetricsSystem mWorkerMetricsSystem;
  /** Space reserver for the block data manager */
  private SpaceReserver mSpaceReserver = null;

  /**
   * @return the block data manager for other worker services
   */
  public BlockDataManager getBlockDataManager() {
    return mBlockDataManager;
  }

  /**
   * @return the worker service handler
   */
  public BlockWorkerClientServiceHandler getWorkerServiceHandler() {
    return mServiceHandler;
  }

  /**
   * @return the worker data service bind host
   */
  public String getDataBindHost() {
    return mDataServer.getBindHost();
  }

  /**
   * @return the worker data service port
   */
  public int getDataLocalPort() {
    return mDataServer.getPort();
  }

  /**
   * @return the worker web service bind host
   */
  public String getWebBindHost() {
    return mWebServer.getBindHost();
  }

  /**
   * @return the worker web service port
   */
  public int getWebLocalPort() {
    return mWebServer.getLocalPort();
  }

  /**
   * Creates a new instance of {@link BlockWorker}.
   *
   * @throws ConnectionFailedException if network connection failed
   * @throws IOException for other exceptions
   */
  public BlockWorker() throws IOException, ConnectionFailedException {
    super(Executors.newFixedThreadPool(4,
        ThreadFactoryUtils.build("block-worker-heartbeat-%d", true)));
    mTachyonConf = WorkerContext.getConf();
    mStartTimeMs = System.currentTimeMillis();

    // Setup MasterClientBase
    mBlockMasterClient = new BlockMasterClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, mTachyonConf), mTachyonConf);

    mFileSystemMasterClient = new FileSystemMasterClient(
        NetworkAddressUtils.getConnectAddress(ServiceType.MASTER_RPC, mTachyonConf), mTachyonConf);

    // Set up BlockDataManager
    WorkerSource workerSource = new WorkerSource();
    mBlockDataManager =
        new BlockDataManager(workerSource, mBlockMasterClient, mFileSystemMasterClient,
            new TieredBlockStore());

    // Setup metrics collection
    mWorkerMetricsSystem = new MetricsSystem("worker", mTachyonConf);
    workerSource.registerGauges(mBlockDataManager);
    mWorkerMetricsSystem.registerSource(workerSource);

    // Setup DataServer
    mDataServer =
        DataServer.Factory.create(
            NetworkAddressUtils.getBindAddress(ServiceType.WORKER_DATA, mTachyonConf),
            mBlockDataManager, mTachyonConf);
    // Reset data server port
    mTachyonConf.set(Constants.WORKER_DATA_PORT, Integer.toString(mDataServer.getPort()));

    // Setup RPC Server
    mServiceHandler = new BlockWorkerClientServiceHandler(mBlockDataManager);

    // Setup web server
    mWebServer =
        new WorkerUIWebServer(ServiceType.WORKER_WEB, NetworkAddressUtils.getBindAddress(
            ServiceType.WORKER_WEB, mTachyonConf), mBlockDataManager,
            NetworkAddressUtils.getConnectAddress(ServiceType.WORKER_RPC, mTachyonConf),
            mStartTimeMs, mTachyonConf);
    mWorkerMetricsSystem.start();
    // Add the metrics servlet to the web server, this must be done after the metrics system starts
    mWebServer.addHandler(mWorkerMetricsSystem.getServletHandler());
    mWebServer.startWebServer();
    int webPort = mWebServer.getLocalPort();

    // Get the worker id
    mPort = mTachyonConf.getInt(Constants.WORKER_RPC_PORT);
    mWorkerNetAddress =
        new NetAddress(NetworkAddressUtils.getConnectHost(ServiceType.WORKER_RPC, mTachyonConf),
            mPort, mDataServer.getPort(), webPort);
    WorkerIdRegistry.registerWithBlockMaster(mBlockMasterClient, mWorkerNetAddress);

    mBlockMasterSync = new BlockMasterSync(mBlockDataManager, mWorkerNetAddress,
        mBlockMasterClient);

    // Setup PinListSyncer
    mPinListSync = new PinListSync(mBlockDataManager, mFileSystemMasterClient);

    // Setup session cleaner
    mSessionCleanerThread = new SessionCleaner(mBlockDataManager);

    // Setup space reserver
    if (mTachyonConf.getBoolean(Constants.WORKER_TIERED_STORE_RESERVER_ENABLED)) {
      mSpaceReserver = new SpaceReserver(mBlockDataManager);
    }
  }

  /**
   * Gets this worker's {@link tachyon.thrift.NetAddress}, which is the worker's hostname, rpc
   * server port, data server port, and web server port.
   *
   * @return the worker's net address
   */
  public NetAddress getWorkerNetAddress() {
    return mWorkerNetAddress;
  }

  /**
   * Runs the block worker. The thread calling this will be blocked until the thrift server shuts
   * down.
   */
  @Override
  public void start() {
    getExecutorService().submit(
        new HeartbeatThread(HeartbeatContext.WORKER_BLOCK_SYNC, mBlockMasterSync,
            WorkerContext.getConf().getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)));

    // Start the pinlist syncer to perform the periodical fetching
    getExecutorService().submit(
        new HeartbeatThread(HeartbeatContext.WORKER_PIN_LIST_SYNC, mPinListSync,
            WorkerContext.getConf().getInt(Constants.WORKER_BLOCK_HEARTBEAT_INTERVAL_MS)));

    // Start the session cleanup checker to perform the periodical checking
    getExecutorService().submit(mSessionCleanerThread);

    // Start the space reserver
    if (mSpaceReserver != null) {
      getExecutorService().submit(mSpaceReserver);
    }

  }

  /**
   * Stops the block worker. This method should only be called to terminate the worker.
   *
   * @throws IOException if the data server fails to close
   */
  @Override
  public void stop() throws IOException {
    mDataServer.close();

    mSessionCleanerThread.stop();
    mBlockMasterClient.close();
    if (mSpaceReserver != null) {
      mSpaceReserver.stop();
    }
    mFileSystemMasterClient.close();
    // Use shutdownNow because HeartbeatThreads never finish until they are interrupted
    getExecutorService().shutdownNow();
    mWorkerMetricsSystem.stop();
    try {
      mWebServer.shutdownWebServer();
    } catch (Exception e) {
      LOG.error("Failed to stop web server", e);
    }
    mBlockDataManager.stop();
    while (!mDataServer.isClosed()) {
      mDataServer.close();
      CommonUtils.sleepMs(100);
    }
  }


}
